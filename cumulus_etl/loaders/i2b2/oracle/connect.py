"""Connects to oracle database"""

import logging
import os

import oracledb

from cumulus_etl import errors


def _get_user() -> str:
    """
    :return: user for I2b2 database
    """
    user = os.environ.get("CUMULUS_SQL_USER")
    if not user:
        errors.fatal(
            "To connect to an Oracle SQL server, "
            "please set the environment variable CUMULUS_SQL_USER",
            errors.SQL_USER_MISSING,
        )
    return user


def _get_password() -> str:
    """
    :return: password for the DSN
    """
    pwd = os.environ.get("CUMULUS_SQL_PASSWORD")
    if not pwd:
        errors.fatal(
            "To connect to an Oracle SQL server, "
            "please set the environment variable CUMULUS_SQL_PASSWORD",
            errors.SQL_PASSWORD_MISSING,
        )
    return pwd


def connect(dsn: str) -> oracledb.Connection:
    """
    :return: connection to oracle database
    """
    logging.info("Attempting to connect to %s", dsn)
    return oracledb.connect(user=_get_user(), password=_get_password(), dsn=dsn)
