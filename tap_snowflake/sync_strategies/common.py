#!/usr/bin/env python3
# pylint: disable=too-many-arguments,duplicate-code,too-many-locals

import copy
import datetime
import time
from decimal import Decimal

import singer
import singer.metrics as metrics
from singer import metadata
from singer import utils
import logging
import os
import pickle
import boto3
from uuid import uuid4

LOGGER = singer.get_logger('tap_snowflake')

def escape(string):
    """Escape strings to be SQL safe"""
    if '"' in string:
        raise Exception("Can't escape identifier {} because it contains a backtick"
                        .format(string))
    return '"{}"'.format(string)


def generate_tap_stream_id(catalog_name, schema_name, table_name):
    """Generate tap stream id as appears in properties.json"""
    return catalog_name + '-' + schema_name + '-' + table_name


def get_stream_version(tap_stream_id, state):
    """Get stream version from bookmark"""
    stream_version = singer.get_bookmark(state, tap_stream_id, 'version')

    if stream_version is None:
        stream_version = int(time.time() * 1000)

    return stream_version


def stream_is_selected(stream):
    """Detect if stream is selected to sync"""
    md_map = metadata.to_map(stream.metadata)
    selected_md = metadata.get(md_map, (), 'selected')

    return selected_md


def property_is_selected(stream, property_name):
    """Detect if field is selected to sync"""
    md_map = metadata.to_map(stream.metadata)
    return singer.should_sync_field(
        metadata.get(md_map, ('properties', property_name), 'inclusion'),
        metadata.get(md_map, ('properties', property_name), 'selected'),
        True)


def get_is_view(catalog_entry):
    """Detect if stream is a view"""
    md_map = metadata.to_map(catalog_entry.metadata)

    return md_map.get((), {}).get('is-view')


def get_database_name(catalog_entry):
    """Get database name from catalog"""
    md_map = metadata.to_map(catalog_entry.metadata)

    return md_map.get((), {}).get('database-name')


def get_schema_name(catalog_entry):
    """Get schema name from catalog"""
    md_map = metadata.to_map(catalog_entry.metadata)

    return md_map.get((), {}).get('schema-name')


def get_key_properties(catalog_entry):
    """Get key properties from catalog"""
    catalog_metadata = metadata.to_map(catalog_entry.metadata)
    stream_metadata = catalog_metadata.get((), {})

    is_view = get_is_view(catalog_entry)

    if is_view:
        key_properties = stream_metadata.get('view-key-properties', [])
    else:
        key_properties = stream_metadata.get('table-key-properties', [])

    return key_properties


def generate_select_sql(catalog_entry, columns):
    """Generate SQL to extract data froom snowflake"""
    database_name = get_database_name(catalog_entry)
    schema_name = get_schema_name(catalog_entry)
    escaped_db = escape(database_name)
    escaped_schema = escape(schema_name)
    escaped_table = escape(catalog_entry.table)
    escaped_columns = []

    for col_name in columns:
        escaped_col = escape(col_name)
        
        # fetch the column type format from the json schema alreay built
        property_format = catalog_entry.schema.properties[col_name].format

        # if the column format is binary, fetch the hexified value
        if property_format == 'binary':
            escaped_columns.append(f'hex_encode({escaped_col}) as {escaped_col}')
        elif property_format == 'semi_structured':
            escaped_columns.append(f'TO_VARCHAR({escaped_col}) as {escaped_col}')
        elif property_format == 'geography':
            escaped_columns.append(f'ST_ASTEXT({escaped_col}) as {escaped_col}')
        else:
            escaped_columns.append(escaped_col)

    select_sql = f'SELECT {",".join(escaped_columns)} FROM {escaped_db}.{escaped_schema}.{escaped_table}'

    # escape percent signs
    select_sql = select_sql.replace('%', '%%')
    return select_sql


def generate_copy_to_s3_sql(s3_loc, catalog_entry, aws_temp_creds):
    database_name = get_database_name(catalog_entry)
    schema_name = get_schema_name(catalog_entry)
    escaped_db = escape(database_name)
    escaped_schema = escape(schema_name)
    escaped_table = escape(catalog_entry.table)


    s3_url = f"s3://{s3_loc['bucket']}/{s3_loc['key']}/{catalog_entry.table}"
    credentials_line = f"CREDENTIALS = (AWS_KEY_ID = '{aws_temp_creds['accessKeyID']}', AWS_SECRET_KEY = '{aws_temp_creds['secretKey']}', AWS_TOKEN = '{aws_temp_creds['sessionToken']}')"
    file_format_line = f"FILE_FORMAT = (TYPE = 'PARQUET')"
    copy_option_line = f"HEADER = TRUE DETAILED_OUTPUT = TRUE"
    
    copy_sql = f"COPY INTO '{s3_url}' FROM {escaped_db}.{escaped_schema}.{escaped_table} {credentials_line} {file_format_line} {copy_option_line}"

    # escape percent signs
    copy_sql = copy_sql.replace('%', '%%')
    return copy_sql


def generate_copy_to_user_stage_sql(catalog_entry):
    database_name = get_database_name(catalog_entry)
    schema_name = get_schema_name(catalog_entry)
    escaped_db = escape(database_name)
    escaped_schema = escape(schema_name)
    escaped_table = escape(catalog_entry.table)

    file_format_line = f"FILE_FORMAT = (TYPE = 'PARQUET')"
    copy_option_line = f"HEADER = TRUE DETAILED_OUTPUT = TRUE"

    copy_sql = f"COPY INTO @~/{escaped_table}/ FROM {escaped_db}.{escaped_schema}.{escaped_table} {file_format_line} {copy_option_line}"

    # escape percent signs
    copy_sql = copy_sql.replace('%', '%%')
    return copy_sql


def generate_list_sql(catalog_entry):
    escaped_table = escape(catalog_entry.table)
    list_sql = f"LIST @~/{escaped_table}/"

    # escape percent signs
    list_sql = list_sql.replace('%', '%%')
    return list_sql

def generate_get_sql(file_name, working_dir):
    get_sql = f"GET @~/{file_name} file://{working_dir}/"
    
    # escape percent signs
    get_sql = get_sql.replace('%', '%%')
    return get_sql

def generate_remove_sql(catalog_entry):
    escaped_table = escape(catalog_entry.table)
    remove_sql = f"REMOVE @~/{escaped_table}/"

    # escape percent signs
    remove_sql = remove_sql.replace('%', '%%')
    return remove_sql

# pylint: disable=too-many-branches
def row_to_singer_record(catalog_entry, version, row, columns, time_extracted):
    """Transform SQL row to singer compatible record message"""
    row_to_persist = ()
    for idx, elem in enumerate(row):
        property_type = catalog_entry.schema.properties[columns[idx]].type
        if isinstance(elem, datetime.datetime):
            row_to_persist += (elem.isoformat() + '+00:00',)

        elif isinstance(elem, datetime.date):
            row_to_persist += (elem.isoformat() + 'T00:00:00+00:00',)

        elif isinstance(elem, datetime.timedelta):
            epoch = datetime.datetime.utcfromtimestamp(0)
            timedelta_from_epoch = epoch + elem
            row_to_persist += (timedelta_from_epoch.isoformat() + '+00:00',)

        elif isinstance(elem, datetime.time):
            row_to_persist += (str(elem),)

        elif isinstance(elem, bytes):
            # for BIT value, treat 0 as False and anything else as True
            if 'boolean' in property_type:
                boolean_representation = elem != b'\x00'
                row_to_persist += (boolean_representation,)
            else:
                row_to_persist += (elem.hex(),)
        
        # orjson is used when writing singer record and it does not recognize Decimal type
        elif isinstance(elem, Decimal):
            row_to_persist += (float(elem),)

        elif 'boolean' in property_type or property_type == 'boolean':
            if elem is None:
                boolean_representation = None
            elif elem == 0:
                boolean_representation = False
            else:
                boolean_representation = True
            row_to_persist += (boolean_representation,)

        else:
            row_to_persist += (elem,)
    rec = dict(zip(columns, row_to_persist))

    return singer.RecordMessage(
        stream=catalog_entry.stream,
        record=rec,
        version=version,
        time_extracted=time_extracted)


def row_to_singer_record2(catalog_entry, version, row, columns, time_extracted):
    """Transform SQL row to singer compatible record message"""
    rec = dict.fromkeys(columns)
    for idx, elem in enumerate(row):
        property_type = catalog_entry.schema.properties[columns[idx]].type
        column = columns[idx]

        if isinstance(elem, str):
            rec[column] = elem

        elif isinstance(elem, datetime.datetime):
            rec[column] = elem.isoformat() + '+00:00'

        elif isinstance(elem, datetime.date):
            rec[column] = elem.isoformat() + 'T00:00:00+00:00'

        elif isinstance(elem, datetime.timedelta):
            epoch = datetime.datetime.utcfromtimestamp(0)
            timedelta_from_epoch = epoch + elem
            rec[column] = timedelta_from_epoch.isoformat() + '+00:00'

        elif isinstance(elem, datetime.time):
            rec[column] = str(elem)

        elif isinstance(elem, bytes):
            # for BIT value, treat 0 as False and anything else as True
            if 'boolean' in property_type:
                boolean_representation = elem != b'\x00'
                rec[column] = boolean_representation
            else:
                rec[column] = elem.hex()
        
        # orjson is used when writing singer record and it does not recognize Decimal type
        elif isinstance(elem, Decimal):
            rec[column] = float(elem)

        elif 'boolean' in property_type or property_type == 'boolean':
            if elem is None:
                boolean_representation = None
            elif elem == 0:
                boolean_representation = False
            else:
                boolean_representation = True
            rec[column] = boolean_representation

        else:
            rec[column] = elem

    return singer.RecordMessage(
        stream=catalog_entry.stream,
        record=rec,
        version=version,
        time_extracted=time_extracted)

def whitelist_bookmark_keys(bookmark_key_set, tap_stream_id, state):
    """..."""
    for bookmark_key in [non_whitelisted_bookmark_key
                         for non_whitelisted_bookmark_key
                         in state.get('bookmarks', {}).get(tap_stream_id, {}).keys()
                         if non_whitelisted_bookmark_key not in bookmark_key_set]:
        singer.clear_bookmark(state, tap_stream_id, bookmark_key)

# def set_log_level():
#     for logger_name in ['snowflake.sqlalchemy', 'snowflake.connector', 'botocore']:
#         logger = logging.getLogger(logger_name)
#         logger.setLevel(logging.DEBUG)
#         ch = logging.StreamHandler()
#         ch.setLevel(logging.DEBUG)
#         ch.setFormatter(logging.Formatter('%(asctime)s - %(threadName)s - %(filename)s:%(lineno)d - %(funcName)s - %(levelname)s - %(message)s'))

def sync_query(cursor, catalog_entry, state, select_sql, columns, stream_version, params, config=None):
    """..."""
    replication_key = singer.get_bookmark(state,
                                          catalog_entry.tap_stream_id,
                                          'replication_key')

    time_extracted = utils.now()

    if config is not None:
        result_batch_location = config.get('result_batch_location')
        index = config.get('index')
        total_workers = config.get('total_workers')
    
    if result_batch_location is not None:
        s3 = boto3.client('s3')
        download_filename = f'{uuid4()}'
        s3.download_file(result_batch_location['bucket'], result_batch_location['key'] + '/importFileCopy/sf_batches', download_filename)
        batches = pickle.load(open(download_filename, 'rb'))

        rows_saved = 0
        database_name = get_database_name(catalog_entry)
        with metrics.record_counter(None) as counter:
            counter.tags['database'] = database_name
            counter.tags['table'] = catalog_entry.table

            md_map = metadata.to_map(catalog_entry.metadata)
            stream_metadata = md_map.get((), {})
            replication_method = stream_metadata.get('replication-method')

            key_properties = get_key_properties(catalog_entry)

            while index < len(batches):
                batch = batches[index]
                LOGGER.info(f'Processing batch {index}: {batch}')

                for row in batch:
                    counter.increment()
                    rows_saved += 1
                    record_message = row_to_singer_record2(catalog_entry,
                                                        stream_version,
                                                        row,
                                                        columns,
                                                        time_extracted)
                    singer.write_message(record_message)
                
                index += total_workers

            os.remove(download_filename)
            singer.write_message(singer.StateMessage(value=copy.deepcopy(state)))
            return

    LOGGER.info('Running %s', select_sql)
    cursor.execute(select_sql, params)

    row = cursor.fetchone()
    rows_saved = 0

    database_name = get_database_name(catalog_entry)

    with metrics.record_counter(None) as counter:
        counter.tags['database'] = database_name
        counter.tags['table'] = catalog_entry.table

        md_map = metadata.to_map(catalog_entry.metadata)
        stream_metadata = md_map.get((), {})
        replication_method = stream_metadata.get('replication-method')

        key_properties = get_key_properties(catalog_entry)

        while row:
            counter.increment()
            rows_saved += 1
            record_message = row_to_singer_record2(catalog_entry,
                                                   stream_version,
                                                   row,
                                                   columns,
                                                   time_extracted)
            singer.write_message(record_message)

            # Jeff McMahon - Aug, 2022
            # Symon does not support incremental updates - therefore we do not need to emit STATE
            # When we revisit incremental updates in Symon we should revisit the following code.
            # if replication_method == 'FULL_TABLE':
            #
            #     max_pk_values = singer.get_bookmark(state,
            #                                         catalog_entry.tap_stream_id,
            #                                         'max_pk_values')
            #
            #     if max_pk_values:
            #         last_pk_fetched = {k: v for k, v in record_message.record.items()
            #                            if k in key_properties}
            #
            #         state = singer.write_bookmark(state,
            #                                       catalog_entry.tap_stream_id,
            #                                       'last_pk_fetched',
            #                                       last_pk_fetched)
            #
            # elif replication_method == 'INCREMENTAL':
            #     if replication_key is not None:
            #         state = singer.write_bookmark(state,
            #                                       catalog_entry.tap_stream_id,
            #                                       'replication_key',
            #                                       replication_key)
            #
            #         state = singer.write_bookmark(state,
            #                                       catalog_entry.tap_stream_id,
            #                                       'replication_key_value',
            #                                       record_message.record[replication_key])
            # if rows_saved % 1000 == 0:
            #     singer.write_message(singer.StateMessage(value=copy.deepcopy(state)))

            row = cursor.fetchone()

    singer.write_message(singer.StateMessage(value=copy.deepcopy(state)))
