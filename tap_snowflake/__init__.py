#!/usr/bin/env python3
# pylint: disable=missing-docstring,not-an-iterable,too-many-locals,too-many-arguments,too-many-branches,invalid-name,duplicate-code,too-many-statements

import collections
import copy
import itertools
import re
import sys
import logging
import traceback
import json
from uuid import uuid4
import time
import pickle

import singer
import singer.metrics as metrics
import singer.schema
import snowflake.connector
from singer import metadata
from singer import utils
from singer.catalog import Catalog, CatalogEntry
from singer.schema import Schema

import tap_snowflake.sync_strategies.common as common
import tap_snowflake.sync_strategies.full_table as full_table
import tap_snowflake.sync_strategies.incremental as incremental
from tap_snowflake.connection import SnowflakeConnection
from tap_snowflake.symon_exception import SymonException

import boto3

LOGGER = singer.get_logger('tap_snowflake')

# Max number of rows that a SHOW SCHEMAS|TABLES|COLUMNS can return.
# If more than this number of rows returned then tap-snowflake will raise TooManyRecordsException
SHOW_COMMAND_MAX_ROWS = 9999
MAX_COLS = 150

# Tone down snowflake connector logs noise
logging.getLogger('snowflake.connector').setLevel(logging.WARNING)

Column = collections.namedtuple('Column', [
    'table_catalog',
    'table_schema',
    'table_name',
    'column_name',
    'data_type',
    'character_maximum_length',
    'numeric_precision',
    'numeric_scale'])

REQUIRED_CONFIG_KEYS = [
    'account',
    'dbname',
    'user',
    'password',
    'warehouse',
    'tables'
]

# Snowflake data types
STRING_TYPES = set(['varchar', 'char', 'character', 'string', 'text'])
NUMBER_TYPES = set(['number', 'decimal', 'numeric'])
INTEGER_TYPES = set(['int', 'integer', 'bigint', 'smallint'])
FLOAT_TYPES = set(['float', 'float4', 'float8', 'real',
                  'double', 'double precision'])
DATETIME_TYPES = set(['datetime', 'timestamp', 'date',
                     'timestamp_ltz', 'timestamp_ntz', 'timestamp_tz'])
BINARY_TYPE = set(['binary', 'varbinary'])
SEMI_STRUCTURED_TYPES = set(['variant', 'object', 'array'])
GEOGRAPHY_TYPE = set(['geography'])

# for symon error logging
ERROR_START_MARKER = '[tap_error_start]'
ERROR_END_MARKER = '[tap_error_end]'

def schema_for_column(c):
    '''Returns the Schema object for the given Column.'''
    data_type = c.data_type.lower()

    inclusion = 'available'
    result = Schema(inclusion=inclusion)

    if data_type == 'boolean': # done
        result.type = ['null', 'boolean']

    elif data_type in INTEGER_TYPES: # done
        result.type = ['null', 'number']

    elif data_type in FLOAT_TYPES: # done
        result.type = ['null', 'number']

    elif data_type in NUMBER_TYPES: # done
        result.type = ['null', 'number']

    elif data_type in STRING_TYPES: # done
        result.type = ['null', 'string']
        result.maxLength = c.character_maximum_length

    elif data_type in DATETIME_TYPES: # done
        result.type = ['null', 'string']
        result.format = 'date-time'

    elif data_type == 'time': # done
        result.type = ['null', 'string']
        result.format = 'time'

    elif data_type in BINARY_TYPE: # done - fail
        result.type = ['null', 'string']
        result.format = 'binary'

    elif data_type in SEMI_STRUCTURED_TYPES:
        result.type = ['null', 'string']
        result.format = 'semi_structured'

    elif data_type in GEOGRAPHY_TYPE: # done - fail
        result.type = ['null', 'string']
        result.format = 'geography'

    else:
        result = Schema(None,
                        inclusion='unsupported',
                        description='Unsupported data type {}'.format(data_type))
    return result


def create_column_metadata(cols):
    mdata = {}
    mdata = metadata.write(mdata, (), 'selected-by-default', False)
    for c in cols:
        schema = schema_for_column(c)
        mdata = metadata.write(mdata,
                               ('properties', c.column_name),
                               'selected-by-default',
                               schema.inclusion != 'unsupported')
        mdata = metadata.write(mdata,
                               ('properties', c.column_name),
                               'sql-datatype',
                               c.data_type.lower())

    return metadata.to_list(mdata)


def get_table_columns(snowflake_conn, tables):
    """Get column definitions of a list of tables

       It's using SHOW commands instead of INFORMATION_SCHEMA views because information_schemas views are slow
       and can cause unexpected exception of:
            Information schema query returned too much data. Please repeat query with more selective predicates.
    """
    table_columns = []
    for table in tables:
        queries = []

        LOGGER.info('Getting column information for %s...', table)

        # Get column data types by SHOW commands
        show_columns = f'SHOW COLUMNS IN TABLE {table}'

        # Convert output of SHOW commands to tables and use SQL joins to get every required information
        select = """
            WITH
              show_columns  AS (SELECT * FROM TABLE(RESULT_SCAN(%(LAST_QID)s)))
            SELECT show_columns."database_name"     AS table_catalog
                  ,show_columns."schema_name"       AS table_schema
                  ,show_columns."table_name"        AS table_name
                  ,show_columns."column_name"       AS column_name
                  -- ----------------------------------------------------------------------------------------
                  -- Character and numeric columns display their generic data type rather than their defined
                  -- data type (i.e. TEXT for all character types, FIXED for all fixed-point numeric types,
                  -- and REAL for all floating-point numeric types).
                  --
                  -- Further info at https://docs.snowflake.net/manuals/sql-reference/sql/show-columns.html
                  -- ----------------------------------------------------------------------------------------
                  ,CASE PARSE_JSON(show_columns."data_type"):type::varchar
                     WHEN 'FIXED' THEN 'NUMBER'
                     WHEN 'REAL'  THEN 'FLOAT'
                     ELSE PARSE_JSON("data_type"):type::varchar
                   END data_type
                  ,PARSE_JSON(show_columns."data_type"):length::number      AS character_maximum_length
                  ,PARSE_JSON(show_columns."data_type"):precision::number   AS numeric_precision
                  ,PARSE_JSON(show_columns."data_type"):scale::number       AS numeric_scale
              FROM show_columns
        """
        queries.extend([show_columns, select])

        # Run everything in one transaction
        columns = snowflake_conn.query(
            queries, max_records=SHOW_COMMAND_MAX_ROWS)
        table_columns.extend(columns)

    return table_columns


def discover_catalog(snowflake_conn, config):
    """Returns a Catalog describing the structure of the database."""
    tables = config.get('tables').split(',')
    sql_columns = get_table_columns(snowflake_conn, tables)

    table_info = {}
    columns = []
    for sql_col in sql_columns:
        catalog = sql_col['TABLE_CATALOG']
        schema = sql_col['TABLE_SCHEMA']
        table_name = sql_col['TABLE_NAME']

        if catalog not in table_info:
            table_info[catalog] = {}

        if schema not in table_info[catalog]:
            table_info[catalog][schema] = {}

        table_info[catalog][schema][table_name] = {
            'row_count': sql_col.get('ROW_COUNT'),
            'is_view': sql_col.get('TABLE_TYPE') == 'VIEW'
        }

        columns.append(Column(
            table_catalog=catalog,
            table_schema=schema,
            table_name=table_name,
            column_name=sql_col['COLUMN_NAME'],
            data_type=sql_col['DATA_TYPE'],
            character_maximum_length=sql_col['CHARACTER_MAXIMUM_LENGTH'],
            numeric_precision=sql_col['NUMERIC_PRECISION'],
            numeric_scale=sql_col['NUMERIC_SCALE']
        ))

    entries = []
    for (k, cols) in itertools.groupby(columns, lambda c: (c.table_catalog, c.table_schema, c.table_name)):
        cols = list(cols)
        (table_catalog, table_schema, table_name) = k
        schema = Schema(type='object',
                        properties={c.column_name: schema_for_column(c) for c in cols})
        md = create_column_metadata(cols)
        md_map = metadata.to_map(md)

        md_map = metadata.write(md_map, (), 'database-name', table_catalog)
        md_map = metadata.write(md_map, (), 'schema-name', table_schema)

        if (
                table_catalog in table_info and
                table_schema in table_info[table_catalog] and
                table_name in table_info[table_catalog][table_schema]
        ):
            # Row Count of views returns NULL - Transform it to not null integer by defaults to 0
            row_count = table_info[table_catalog][table_schema][table_name].get(
                'row_count', 0) or 0
            is_view = table_info[table_catalog][table_schema][table_name]['is_view']

            md_map = metadata.write(md_map, (), 'row-count', row_count)
            md_map = metadata.write(md_map, (), 'is-view', is_view)

            entry = CatalogEntry(
                table=table_name,
                stream=table_name,
                metadata=metadata.to_list(md_map),
                tap_stream_id=common.generate_tap_stream_id(
                    table_catalog, table_schema, table_name),
                schema=schema)

            entries.append(entry)

    return Catalog(entries)


def do_discover(snowflake_conn, config):
    discover_catalog(snowflake_conn, config).dump()


# pylint: disable=fixme
# TODO: Maybe put in a singer-db-utils library.
def desired_columns(selected, table_schema):
    """Return the set of column names we need to include in the SELECT.

    selected - set of column names marked as selected in the input catalog
    table_schema - the most recently discovered Schema for the table
    """
    all_columns = set()
    available = set()
    automatic = set()
    unsupported = set()

    for column, column_schema in table_schema.properties.items():
        all_columns.add(column)
        inclusion = column_schema.inclusion
        if inclusion == 'automatic':
            automatic.add(column)
        elif inclusion == 'available':
            available.add(column)
        elif inclusion == 'unsupported':
            unsupported.add(column)
        else:
            raise Exception('Unknown inclusion ' + inclusion)

    selected_but_unsupported = selected.intersection(unsupported)
    if selected_but_unsupported:
        LOGGER.warning(
            'Columns %s were selected but are not supported. Skipping them.',
            selected_but_unsupported)

    selected_but_nonexistent = selected.difference(all_columns)
    if selected_but_nonexistent:
        LOGGER.warning(
            'Columns %s were selected but do not exist.',
            selected_but_nonexistent)

    not_selected_but_automatic = automatic.difference(selected)
    if not_selected_but_automatic:
        LOGGER.warning(
            'Columns %s are primary keys but were not selected. Adding them.',
            not_selected_but_automatic)

    return sorted(selected.intersection(available).union(automatic), key=list(table_schema.properties.keys()).index)


def resolve_catalog(discovered_catalog, streams_to_sync):
    result = Catalog(streams=[])

    # Iterate over the streams in the input catalog and match each one up
    # with the same stream in the discovered catalog.
    for catalog_entry in streams_to_sync:
        catalog_metadata = metadata.to_map(catalog_entry.metadata)
        replication_key = catalog_metadata.get((), {}).get('replication-key')

        discovered_table = discovered_catalog.get_stream(
            catalog_entry.tap_stream_id)
        database_name = common.get_database_name(catalog_entry)

        if not discovered_table:
            LOGGER.warning('Database %s table %s was selected but does not exist',
                           database_name, catalog_entry.table)
            continue

        selected = {k for k, v in catalog_entry.schema.properties.items()
                    if common.property_is_selected(catalog_entry, k) or k == replication_key}

        # These are the columns we need to select
        columns = desired_columns(selected, discovered_table.schema)

        if (len(columns) > MAX_COLS):
            raise SymonException(f'Number of columns could not exceed {MAX_COLS}', 'snowflake.SnowflakeClientError')
        else:
            LOGGER.info(f'length of cols: {len(columns)}')

        result.streams.append(CatalogEntry(
            tap_stream_id=catalog_entry.tap_stream_id,
            metadata=catalog_entry.metadata,
            stream=catalog_entry.tap_stream_id,
            table=catalog_entry.table,
            schema=Schema(
                type='object',
                properties={col: discovered_table.schema.properties[col]
                            for col in columns}
            )
        ))

    return result


def get_streams(snowflake_conn, catalog, config, state):
    """Returns the Catalog of data we're going to sync for all SELECT-based
    streams (i.e. INCREMENTAL and FULL_TABLE that require a historical
    sync).

    Using the Catalog provided from the input file, this function will return a
    Catalog representing exactly which tables and columns that will be emitted
    by SELECT-based syncs. This is achieved by comparing the input Catalog to a
    freshly discovered Catalog to determine the resulting Catalog.

    The resulting Catalog will include the following any streams marked as
    "selected" that currently exist in the database. Columns marked as "selected"
    and those labled "automatic" (e.g. primary keys and replication keys) will be
    included. Streams will be prioritized in the following order:
      1. currently_syncing if it is SELECT-based
      2. any streams that do not have state
      3. any streams that do not have a replication method of LOG_BASED
    """
    discovered = discover_catalog(snowflake_conn, config)

    # Filter catalog to include only selected streams
    # pylint: disable=unnecessary-lambda
    selected_streams = list(
        filter(lambda s: common.stream_is_selected(s), catalog.streams))
    streams_with_state = []
    streams_without_state = []

    for stream in selected_streams:
        stream_state = state.get('bookmarks', {}).get(stream.tap_stream_id)

        if not stream_state:
            streams_without_state.append(stream)
        else:
            streams_with_state.append(stream)

    # If the state says we were in the middle of processing a stream, skip
    # to that stream. Then process streams without prior state and finally
    # move onto streams with state (i.e. have been synced in the past)
    currently_syncing = singer.get_currently_syncing(state)

    # prioritize streams that have not been processed
    ordered_streams = streams_without_state + streams_with_state

    if currently_syncing:
        currently_syncing_stream = list(filter(
            lambda s: s.tap_stream_id == currently_syncing, streams_with_state))

        non_currently_syncing_streams = list(
            filter(lambda s: s.tap_stream_id != currently_syncing, ordered_streams))

        streams_to_sync = currently_syncing_stream + non_currently_syncing_streams
    else:
        # prioritize streams that have not been processed
        streams_to_sync = ordered_streams

    return resolve_catalog(discovered, streams_to_sync)


def write_schema_message(catalog_entry, bookmark_properties=None):
    key_properties = common.get_key_properties(catalog_entry)

    singer.write_message(singer.SchemaMessage(
        stream=catalog_entry.stream,
        schema=catalog_entry.schema.to_dict(),
        key_properties=key_properties,
        bookmark_properties=bookmark_properties
    ))


def do_sync_internal_unload(snowflake_conn, catalog_entry, columns, temp_s3_upload_folder):
    LOGGER.info('Stream %s is using internal unload', catalog_entry.stream)
    with snowflake_conn.connect_with_backoff() as open_conn:
        with open_conn.cursor() as cur:
            # cur.execute('alter session set ENABLE_UNLOAD_PHYSICAL_TYPE_OPTIMIZATION = true')
            # result = cur.fetchall()
            # LOGGER.info(f'ALTER SESSION result: {result}')
            
            # prefix (subdirectory) to use for exporting table to snowflake user stage + downloading to local
            prefix = uuid4()
            
            try:
                # 1. export table to snowflake user stage under folder <prefix> as parquet files. 
                select_sql = common.generate_select_sql(catalog_entry, columns)
                copy_sql = common.generate_copy_sql(select_sql, prefix)
                LOGGER.info(f'Running COPY query to export table {catalog_entry.stream} to {prefix} folder in snowflake user stage')
                cur.execute(copy_sql)
                copy_results = cur.fetchall()

                # 2. list all files written to user stage under folder <prefix>
                list_sql = f"LIST @~/{prefix}/"
                LOGGER.info(f'Running LIST query to list files in {prefix} folder in user stage')
                cur.execute(list_sql)
                files_in_user_stage = cur.fetchall()
                
                # 3. download files to local working directory (./<prefix>/) and move files to s3. use the same prefix for local working directory
                s3_client = boto3.client('s3')                
                local_working_dir = common.make_directory(prefix)
                
                LOGGER.info(f'Moving files to s3')
                for file_info in files_in_user_stage:
                    # filename is the filename in snowflake user stage, which includes folder path in the filename. since we created 
                    # local working directory using the same prefix used in snowflake's user stage, we can reuse the filename to clean up 
                    # local working directory
                    filename = file_info[0]
                    get_sql = f"GET @~/{filename} file://{local_working_dir}/"
                    cur.execute(get_sql)

                    common.upload_file_to_s3(s3_client, filename, temp_s3_upload_folder)
                    common.remove_file(filename)
                
                # log rows processed for Symon import progress bar update. For unloading, we do not use target so using print should be fine. 
                # logger logs prefix info, which fails to parsed as Symon progress message
                total_rows_processed = sum([copy_result[2] for copy_result in copy_results])
                print(json.dumps({'newRowsProcessed': total_rows_processed}))
            finally:
                try:
                    # remove files from snowflake user stage
                    remove_sql = f"REMOVE @~/{prefix}/"
                    LOGGER.info(f'Running REMOVE query to clean up {prefix} folder in user stage')
                    cur.execute(remove_sql)
                    LOGGER.info(f'Cleaning up local working directory {local_working_dir}')
                    common.remove_directory(local_working_dir)
                except:
                    pass


def do_sync_external_unload(snowflake_conn, catalog_entry, columns, temp_s3_creds, temp_s3_upload_folder):
    LOGGER.info('Stream %s is using external unload', catalog_entry.stream)
    with snowflake_conn.connect_with_backoff() as open_conn:
        with open_conn.cursor() as cur:
            select_sql = common.generate_select_sql(catalog_entry, columns)
            # random filename to use for parquet files exported to s3
            prefix = uuid4()
            copy_sql = common.generate_copy_sql(select_sql, prefix, temp_s3_upload_folder, temp_s3_creds)

            LOGGER.info(f'Running COPY query to export table {catalog_entry.stream} to S3')
            cur.execute(copy_sql)
            copy_results = cur.fetchall()
            # log rows processed for Symon import progress bar update. For unloading, we do not use target so using print should be fine. 
            # logger logs prefix info, which fails to parsed as Symon progress message
            total_rows_processed = sum([copy_result[2] for copy_result in copy_results])
            print(json.dumps({'newRowsProcessed': total_rows_processed}))


def do_sync_incremental(snowflake_conn, catalog_entry, state, columns):
    LOGGER.info('Stream %s is using incremental replication', catalog_entry.stream)

    md_map = metadata.to_map(catalog_entry.metadata)
    replication_key = md_map.get((), {}).get('replication-key')

    if not replication_key:
        raise Exception(f'Cannot use INCREMENTAL replication for table ({catalog_entry.stream}) without a replication '
                        f'key.')

    write_schema_message(catalog_entry=catalog_entry,
                         bookmark_properties=[replication_key])

    incremental.sync_table(snowflake_conn, catalog_entry, state, columns)

    singer.write_message(singer.StateMessage(value=copy.deepcopy(state)))


def do_sync_full_table(snowflake_conn, catalog_entry, state, columns, config):
    LOGGER.info('Stream %s is using full table replication', catalog_entry.stream)

    write_schema_message(catalog_entry)

    stream_version = common.get_stream_version(
        catalog_entry.tap_stream_id, state)

    full_table.sync_table(snowflake_conn, catalog_entry,
                          state, columns, stream_version, config)

    # Prefer initial_full_table_complete going forward
    singer.clear_bookmark(state, catalog_entry.tap_stream_id, 'version')

    state = singer.write_bookmark(state,
                                  catalog_entry.tap_stream_id,
                                  'initial_full_table_complete',
                                  True)

    singer.write_message(singer.StateMessage(value=copy.deepcopy(state)))

def do_sync_full_table_parallel(snowflake_conn, catalog_entry, state, columns, config):
    LOGGER.info('Stream %s is using full table replication in parallel',
                catalog_entry.stream)
    
    result_batch_location = config.get('result_batch_location', None)
    start_index = config.get('start_index', None)
    jump = config.get('jump', None)

    if result_batch_location is None:
        raise Exception('Missing config: result_batch_location')

    # For parallel import, we call do_sync_full_table_parallel twice:
    # 1. first to submit sql query and write ResultBatch objects to s3 that could be used to fetch rows from
    # 2. second to read ResultBatch objects from s3 and fetch rows in parallel
    # only one of the two should be executed at a time
    
    # first call, query hasn't been executed. submit query and write ResultBatch objects to s3
    if start_index is None and jump is None:
        LOGGER.info('Submitting query to snowflake and writing ResultBatch objects to s3')
        with snowflake_conn.connect_with_backoff() as open_conn:
            with open_conn.cursor() as cur:
                select_sql = common.generate_select_sql(catalog_entry, columns)
                LOGGER.info(f'Running {select_sql}')
                cur.execute(select_sql)

                batches = cur.get_result_batches()
                batches.sort(key=lambda x: x.uncompressed_size if x.uncompressed_size is not None else 0)
                # metadata to be used when determining number of parallel splits for Symon import
                batch_metadata = {
                    'totalBatchCount': len(batches),
                    'totalUncompressedSize': sum([x.uncompressed_size for x in batches if x.uncompressed_size is not None]),
                    'totalCompressedSize': sum([x.compressed_size for x in batches if x.compressed_size is not None]),
                    'totalRowCount': sum([x.rowcount for x in batches if x.rowcount is not None]),
                    'totalColumnCount': len(columns)
                }
                
                try:
                    local_working_dir = common.make_directory(uuid4())
                    
                    # file_path = os.path.join(local_working_dir, common.RESULT_BATCH_FILENAME)
                    result_batch_file_path = f'{local_working_dir}/{common.RESULT_BATCH_FILENAME}'
                    pickle.dump(batches, open(result_batch_file_path, 'wb'))

                    result_batch_metadata_file_path = f'{local_working_dir}/{common.RESULT_BATCH_METADATA_FILENAME}'
                    json.dump(batch_metadata, open(result_batch_metadata_file_path, 'w'))

                    s3_client = boto3.client('s3')
                    common.upload_file_to_s3(s3_client, result_batch_file_path, result_batch_location)
                    common.upload_file_to_s3(s3_client, result_batch_metadata_file_path, result_batch_location)
                finally:
                    try:
                        common.remove_directory(local_working_dir)
                    except:
                        pass
        return
    
    LOGGER.info('Syncing in parallel using ResultBatch')
    # second call, query has been executed in prev call, read ResultBatch objects from s3 and sync in parallel
    write_schema_message(catalog_entry)

    stream_version = common.get_stream_version(
        catalog_entry.tap_stream_id, state)

    full_table.sync_table(snowflake_conn, catalog_entry,
                          state, columns, stream_version, True, config)

    # Prefer initial_full_table_complete going forward
    singer.clear_bookmark(state, catalog_entry.tap_stream_id, 'version')

    state = singer.write_bookmark(state,
                                  catalog_entry.tap_stream_id,
                                  'initial_full_table_complete',
                                  True)

    singer.write_message(singer.StateMessage(value=copy.deepcopy(state)))


def sync_streams(snowflake_conn, catalog, state, config):
    for catalog_entry in catalog.streams:
        columns = list(catalog_entry.schema.properties.keys())

        if not columns:
            LOGGER.warning(
                'There are no columns selected for stream %s, skipping it.', catalog_entry.stream)
            continue

        state = singer.set_currently_syncing(
            state, catalog_entry.tap_stream_id)

        # Emit a state message to indicate that we've started this stream
        singer.write_message(singer.StateMessage(value=copy.deepcopy(state)))

        md_map = metadata.to_map(catalog_entry.metadata)

        replication_method = md_map.get((), {}).get('replication-method')

        database_name = common.get_database_name(catalog_entry)
        schema_name = common.get_schema_name(catalog_entry)

        with metrics.job_timer('sync_table') as timer:
            timer.tags['database'] = database_name
            timer.tags['table'] = catalog_entry.table

            LOGGER.info('Beginning to sync %s.%s.%s', database_name,
                        schema_name, catalog_entry.table)

            if replication_method == 'INCREMENTAL':
                do_sync_incremental(
                    snowflake_conn, catalog_entry, state, columns)
            elif replication_method == 'FULL_TABLE':
                # 4 syncing methods for Symon 
                temp_s3_creds = config.get('temp_s3_creds', None)
                temp_s3_upload_folder = config.get('temp_s3_upload_folder', None)
                # 1) unload table as parquet files to s3
                if temp_s3_creds is not None and temp_s3_upload_folder is not None:
                    do_sync_external_unload(snowflake_conn, catalog_entry, columns, temp_s3_creds, temp_s3_upload_folder)
                # 2) unload table as parquet files to snowflake internal stage, move to s3
                elif temp_s3_upload_folder is not None:
                    do_sync_internal_unload(snowflake_conn, catalog_entry, columns, temp_s3_upload_folder)
                # 3) parallel import - multiple workers running tap-snowflake
                elif config.get('result_batch_location', None) is not None:
                    do_sync_full_table_parallel(snowflake_conn, catalog_entry, state, columns, config)
                # 4) normal import - single worker running tap-snowflake
                else:
                    do_sync_full_table(snowflake_conn, catalog_entry, state, columns, config)
            else:
                raise Exception(
                    'Only INCREMENTAL and FULL TABLE replication methods are supported')

    state = singer.set_currently_syncing(state, None)
    singer.write_message(singer.StateMessage(value=copy.deepcopy(state)))


def do_sync(snowflake_conn, config, catalog, state):
    catalog = get_streams(snowflake_conn, catalog, config, state)
    sync_streams(snowflake_conn, catalog, state, config)


def main_impl():
    try:
        start = time.time()
        # used for storing error info to write if error occurs
        error_info = None
        args = utils.parse_args(REQUIRED_CONFIG_KEYS)

        snowflake_conn = SnowflakeConnection(args.config)

        if args.discover:
            do_discover(snowflake_conn, args.config)
        elif args.catalog:
            state = args.state or {}
            do_sync(snowflake_conn, args.config, args.catalog, state)
        elif args.properties:
            catalog = Catalog.from_dict(args.properties)
            state = args.state or {}
            if args.config.get('profile_execution', False):
                LOGGER.info(
                    f'Executing with profiling enabled based on *profile_execution* config parameter')
                import cProfile
                pr = cProfile.Profile()
                pr.enable()
                do_sync(snowflake_conn, args.config, catalog, state)
                pr.disable()
                pr.dump_stats('output.dat')

                import pstats
                from pstats import SortKey

                time_str = time.strftime("%Y%m%d-%H%M%S")

                with open(f'output_time_{time_str}.txt', 'w') as f:
                    p = pstats.Stats("output.dat", stream=f)
                    p.sort_stats("time").print_stats()
                    LOGGER.info(f'Execution profile written to {f.name}')

                with open(f'output_calls_{time_str}.txt', 'w') as f:
                    p = pstats.Stats("output.dat", stream=f)
                    p.sort_stats("calls").print_stats()
                    LOGGER.info(f'Execution profile written to {f.name}')

            else:
                do_sync(snowflake_conn, args.config, catalog, state)

        else:
            LOGGER.info('No properties were selected')
        end = time.time()
        LOGGER.info(f"tap start: {start} --- end: {end} --- diff: {end - start}")
    except SymonException as e:
        exc_type, exc_value, exc_traceback = sys.exc_info()
        error_info = {
            'message': traceback.format_exception_only(exc_type, exc_value)[-1],
            'code': e.code,
            'traceback': "".join(traceback.format_tb(exc_traceback))
        }

        if e.details is not None:
            error_info['details'] = e.details
        raise
    except BaseException as e:
        exc_type, exc_value, exc_traceback = sys.exc_info()
        error_info = {
            'message': traceback.format_exception_only(exc_type, exc_value)[-1],
            'traceback': "".join(traceback.format_tb(exc_traceback))
        }
        raise
    finally:
        if error_info is not None:
            try:
                error_file_path = args.config.get('error_file_path', None)
                if error_file_path is not None:
                    try:
                        with open(error_file_path, 'w', encoding='utf-8') as fp:
                            json.dump(error_info, fp)
                    except:
                        pass
                # log error info as well in case file is corrupted
                error_info_json = json.dumps(error_info)
                error_start_marker = args.config.get('error_start_marker', ERROR_START_MARKER)
                error_end_marker = args.config.get('error_end_marker', ERROR_END_MARKER)
                LOGGER.info(f'{error_start_marker}{error_info_json}{error_end_marker}')
            except:
                # error occurred before args was parsed correctly, log the error
                error_info_json = json.dumps(error_info)
                LOGGER.info(f'{ERROR_START_MARKER}{error_info_json}{ERROR_END_MARKER}')


def main():
    try:
        main_impl()
    except Exception as exc:
        LOGGER.critical(exc)
        raise exc


if __name__ == '__main__':
    main()
