"""Connects to oracle database"""

import getpass
import logging
import os

import oracledb


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
        user = get_user()
        return getpass.getpass(f'Enter password for I2B2_SQL_USER {user}: ')


def connect() -> oracledb.Connection:
    """
    :return: connection to oracle database
    """
    logging.info('Attempting to connect to %s', get_data_source_name())
    return oracledb.connect(user=get_user(), password=get_password(), dsn=get_data_source_name())
