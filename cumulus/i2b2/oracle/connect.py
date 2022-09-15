import getpass
import logging
import os
import cx_Oracle

def get_data_source_name() -> str:
    return os.environ.get('I2B2_SQL_DSN', None)

def get_user() -> str:
    """
    :return: user for I2b2 database
    """
    return os.environ.get('I2B2_SQL_USER', None)

def get_password() -> str:
    """
    :return: password for the DSN
    """
    pwd = os.environ.get('I2B2_SQL_PASS', None)
    if not pwd or len(pwd) < 4:
        return getpass.getpass("Enter password for I2B2_SQL_USER %s: " % get_user())

def get_library_path() -> str:
    """
    :return: SQL connection requires Linked Library
    """
    ldpath = os.getenv('LD_LIBRARY_PATH', None)

    if ldpath is None or not 'instantclient' in ldpath:
        raise Exception('LD_LIBRARY_PATH does not exist OR does not contain a path to instantclient. '
                        'This environment variable must be set before calling scripts.')
    else:
        return ldpath

def connect() -> cx_Oracle.Connection:
    """
    :return: connection to oracle database
    """
    logging.info(f"Attempting to connect to {get_data_source_name()}")
    cx_Oracle.init_oracle_client(lib_dir=get_library_path())
    return cx_Oracle.connect(get_user(), get_password(), get_data_source_name())
