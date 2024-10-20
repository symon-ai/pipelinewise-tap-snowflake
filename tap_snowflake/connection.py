#!/usr/bin/env python3
from typing import Union, List, Dict

import backoff
import singer
import sys
import snowflake.connector
from tap_snowflake.symon_exception import SymonException

LOGGER = singer.get_logger('tap_snowflake')


class TooManyRecordsException(Exception):
    """Exception to raise when query returns more records than max_records"""


def retry_pattern():
    """Retry pattern decorator used when connecting to snowflake
    """
    return backoff.on_exception(backoff.expo,
                                snowflake.connector.errors.OperationalError,
                                max_tries=5,
                                on_backoff=log_backoff_attempt,
                                factor=2)


def log_backoff_attempt(details):
    """Log backoff attempts used by retry_pattern
    """
    LOGGER.info(
        'Error detected communicating with Snowflake, triggering backoff: %d try', details.get('tries'))


def validate_config(config):
    """Validate configuration dictionary"""
    errors = []
    required_config_keys = [
        'account',
        'dbname',
        'warehouse',
        'tables',
        'auth_method'
    ]

    # Check if mandatory keys exist
    for k in required_config_keys:
        if not config.get(k, None):
            errors.append(f'Required key is missing from config: [{k}]')
    
    if config.get('auth_method', None) == 'basic':
        if not (config.get('user', None) and config.get('password', None)):
            errors.append('user/password must be provided in the config for basic authentication.')
    elif config.get('auth_method', None) == 'oauth':
        if not (config.get('access_token', None)):
            errors.append('access_token must be provided in the config for oauth authentication.')
    else:
        errors.append('auth_method must be either "basic" or "oauth".')

    return errors


class SnowflakeConnection:
    """Class to manage connection to snowflake data warehouse"""

    def __init__(self, connection_config):
        """
        connection_config:      Snowflake connection details
        """
        self.connection_config = connection_config
        config_errors = validate_config(connection_config)
        if len(config_errors) == 0:
            self.connection_config = connection_config
        else:
            LOGGER.error('Invalid configuration:\n   * %s',
                         '\n   * '.join(config_errors))
            sys.exit(1)

    def open_connection(self):
        """Connect to snowflake database"""
        try:
            config = {
                    'account': self.connection_config['account'],
                    'role': self.connection_config.get('role'),  # optional parameter
                    'database': self.connection_config['dbname'],
                    'warehouse': self.connection_config['warehouse'],
                    'client_prefetch_threads': self.connection_config.get(
                        'client_prefetch_threads', 4),
                    'insecure_mode': self.connection_config.get('insecure_mode', False),
                    'network_timeout': 1800
                    # Use insecure mode to avoid "Failed to get OCSP response" warnings
                    # insecure_mode=True
                }
            if self.connection_config.get('auth_method') == 'basic':
                config['user'] = self.connection_config['user']
                config['password'] = self.connection_config['password']
            else: # oauth - connection_config['auth_method'] is validated already
                config['authenticator'] = 'oauth'
                config['token'] = self.connection_config['access_token']
            
            return snowflake.connector.connect(**config)
        except snowflake.connector.errors.DatabaseError as e:
            if 'Incorrect username or password was specified' in str(e):
                raise SymonException('The username or password provided is incorrect. Please check and try again.', 'snowflake.SnowflakeClientError')
            raise
        except snowflake.connector.errors.ForbiddenError as e:
            if 'Failed to connect to DB. Verify the account name is correct' in str(e):
                raise SymonException("Sorry, we couldn't connect to the database. Please check the Snowflake URL and try again.", "snowflake.SnowflakeClientError")
            raise

    @retry_pattern()
    def connect_with_backoff(self):
        """Connect to snowflake database and retry automatically a few times if fails"""
        return self.open_connection()

    def query(self, query: Union[List[str], str], params: Dict = None, max_records=0):
        """Run a query in snowflake"""
        result = []

        if params is None:
            params = {}
        else:
            if 'LAST_QID' in params:
                LOGGER.warning('LAST_QID is a reserved prepared statement parameter name, '
                               'it will be overridden with each executed query!')

        with self.connect_with_backoff() as connection:
            with connection.cursor(snowflake.connector.DictCursor) as cur:

                # Run every query in one transaction if query is a list of SQL
                if isinstance(query, list):
                    cur.execute('START TRANSACTION')
                    queries = query
                else:
                    queries = [query]

                qid = None

                for sql in queries:
                    LOGGER.debug('Running query: %s', sql)

                    # update the LAST_QID
                    params['LAST_QID'] = qid

                    try:
                        cur.execute(sql, params)
                    except snowflake.connector.errors.ProgrammingError as e:
                        message = str(e)
                        if "does not exist or not authorized" in message:
                            if "Table" in message:
                                table_name = message[message.find("Table"):message.find(" does not exist")].replace("'", '"')
                                raise SymonException(f'{table_name} was not found, or you are not authorized to access it.', 'snowflake.SnowflakeClientError')
                            if "Schema" in message:
                                schema_name = message[message.find("Schema"):message.find(" does not exist")].replace("'", '"')
                                raise SymonException(f'{schema_name} was not found, or you are not authorized to access it.', 'snowflake.SnowflakeClientError')
                            if "Database" in message:
                                database_name = message[message.find("Database"):message.find(" does not exist")].replace("'", '"')
                                raise SymonException(f'{database_name} was not found, or you are not authorized to access it.', 'snowflake.SnowflakeClientError')
                            raise
                        if 'No active warehouse selected in the current session.' in message:
                            raise SymonException(f'The warehouse provided is incorrect. Please ensure it is correct and you are authorized to access it', 'snowflake.SnowflakeClientError')
                        if 'This session does not have a current database' in message:
                            raise SymonException(f'The database provided is incorrect. Please ensure it is correct and you are authorized to access it.', 'snowflake.SnowflakeClientError')
                        raise

                    qid = cur.sfqid

                    # Raise exception if returned rows greater than max allowed records
                    if 0 < max_records < cur.rowcount:
                        raise TooManyRecordsException(
                            f'Query returned too many records. This query can return max {max_records} records')

                    if cur.rowcount > 0:
                        result = cur.fetchall()

        return result
