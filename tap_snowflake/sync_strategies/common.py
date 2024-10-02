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
import os
import shutil
import pickle
import boto3
from uuid import uuid4
from tap_snowflake.symon_exception import SymonException

LOGGER = singer.get_logger('tap_snowflake')

# TODO: Commenting out for now as they are not for coming release 3.49.0/3.50.0
# RESULT_BATCH_FILENAME = 'snowflake_result_batches'
# RESULT_BATCH_METADATA_FILENAME = 'snowflake_result_batches_metadata.json'

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
        data_type = catalog_entry.schema.properties[col_name].type[1]

        # if the column format is binary, fetch the hexified value
        if property_format == 'binary':
            escaped_columns.append(f'hex_encode({escaped_col}) as {escaped_col}')
        elif property_format == 'semi_structured':
            escaped_columns.append(f'TO_VARCHAR({escaped_col}) as {escaped_col}')
        elif property_format == 'geography':
            escaped_columns.append(f'ST_ASTEXT({escaped_col}) as {escaped_col}')
        # Castings below were added for WP-21311 to make sure that Snowflake import using s3 unload sync vs regular sync 
        # uses the same select query.
        elif property_format == 'time':
            escaped_columns.append(f"TO_VARCHAR({escaped_col}, 'HH24:MI:SS.FF6') as {escaped_col}")
        # Symon simply drops timezone info instead of casting to UTC. Snowflake's TO_TIMESTAMP_NTZ also has the same behavior.
        elif property_format == 'date-time':
            escaped_columns.append(f'TO_TIMESTAMP_NTZ({escaped_col}) as {escaped_col}')
        # Symon treats all number type as double.
        elif data_type == 'number':
            escaped_columns.append(f'TO_DOUBLE({escaped_col}) as {escaped_col}')
        else:
            escaped_columns.append(escaped_col)

    select_sql = f'SELECT {",".join(escaped_columns)} FROM {escaped_db}.{escaped_schema}.{escaped_table}'

    # escape percent signs
    select_sql = select_sql.replace('%', '%%')
    return select_sql


def generate_copy_sql(select_sql, prefix, temp_s3_upload_folder=None, temp_s3_creds=None, storage_integration=None):
    file_format_line = f"FILE_FORMAT = (TYPE = 'PARQUET')"
    copy_option_line = f"HEADER = TRUE MAX_FILE_SIZE = {128 * 1024 * 1024} DETAILED_OUTPUT = TRUE"

    # export table to external stage s3
    if temp_s3_upload_folder is not None and (temp_s3_creds is not None or storage_integration is not None):
        # snowflake addes suffix _x_y_z to the prefix to generate filenames (to ensure distinct filenames across files generated from parallel threads)
        # e.g. for s3 location below, filename would be <prefix>_0_1_0.snappy.parquet
        s3_url = f"s3://{temp_s3_upload_folder['bucket']}/{temp_s3_upload_folder['key']}/{prefix}"
        if temp_s3_creds is not None:
            credentials_line = f"CREDENTIALS = (AWS_KEY_ID = '{temp_s3_creds['accessKeyID']}', AWS_SECRET_KEY = '{temp_s3_creds['secretKey']}', AWS_TOKEN = '{temp_s3_creds['sessionToken']}')"
        else:
            credentials_line = f"STORAGE_INTEGRATION = {storage_integration}"
        return f"COPY INTO '{s3_url}' FROM ({select_sql}) {credentials_line} {file_format_line} {copy_option_line}"
    
    # export table to snowflake internal stage. exports files into <user stage>/<prefix> folder with auto-generated filename data_x_y_z.parquet
    return f"COPY INTO @~/{prefix}/ FROM ({select_sql}) {file_format_line} {copy_option_line}"


# TODO: Commenting out for now as they are not for coming release 3.49.0/3.50.0
#  def upload_file_to_s3(s3_client, file_to_copy, s3_loc):
#     upload_key = f'{s3_loc["key"]}/{os.path.basename(file_to_copy)}'
#     LOGGER.info(f'Uploading file {file_to_copy} to s3://{s3_loc["bucket"]}/{upload_key}')
#     s3_client.upload_file(file_to_copy, s3_loc['bucket'], upload_key)


# def make_directory(directory_name):
#     path = f'{os.getcwd()}/{directory_name}'
#     if not os.path.exists(path):
#         os.mkdir(path)
#     return path


# def remove_file(file_path):
#     os.remove(file_path)


# def remove_directory(directory):
#     shutil.rmtree(directory)


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


def sync_query(cursor, catalog_entry, state, select_sql, columns, stream_version, params, is_full_table=False):
    """..."""
    replication_key = singer.get_bookmark(state,
                                          catalog_entry.tap_stream_id,
                                          'replication_key')

    time_extracted = utils.now()

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

    # do_sync_external_unload, do_sync_internal_unload makes snowflake export table as parquet file.
    # if table is empty, snowflake exports no file and we raise an error. raise same error for normal 
    # sync for consistency.
    if rows_saved == 0 and is_full_table:
        raise SymonException('No data available.', 'snowflake.SnowflakeClientError')

    singer.write_message(singer.StateMessage(value=copy.deepcopy(state)))


# TODO: Commenting out for now as they are not for coming release 3.49.0/3.50.0
# def sync_query_parallel(catalog_entry, state, columns, stream_version, config):
#     time_extracted = utils.now()

#     # cursor.execute('alter session set ENABLE_UNLOAD_PHYSICAL_TYPE_OPTIMIZATION = true')
#     result_batch_location = config.get('result_batch_location', None)
#     start_index = config.get('start_index', None)
#     jump = config.get('jump', None)

#     if result_batch_location is None or start_index is None or jump is None:
#         raise Exception('Missing configs for parallel import: result_batch_location, start_index, jump required.')

#     s3 = boto3.client('s3')
#     download_filename = f'{uuid4()}'
#     s3.download_file(result_batch_location['bucket'], f"{result_batch_location['key']}/{RESULT_BATCH_FILENAME}", download_filename)
#     batches = pickle.load(open(download_filename, 'rb'))

#     rows_saved = 0
#     database_name = get_database_name(catalog_entry)
#     with metrics.record_counter(None) as counter:
#         counter.tags['database'] = database_name
#         counter.tags['table'] = catalog_entry.table

#         while start_index < len(batches):
#             batch = batches[start_index]
#             LOGGER.info(f'Processing batch {start_index}: {batch}')

#             for row in batch:
#                 counter.increment()
#                 rows_saved += 1
#                 record_message = row_to_singer_record2(catalog_entry,
#                                                     stream_version,
#                                                     row,
#                                                     columns,
#                                                     time_extracted)
#                 singer.write_message(record_message)
            
#             start_index += jump

#     remove_file(download_filename)
#     singer.write_message(singer.StateMessage(value=copy.deepcopy(state)))
