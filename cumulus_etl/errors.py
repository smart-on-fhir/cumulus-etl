"""Exception classes and error handling"""

import sys
from typing import NoReturn


# Error return codes, mostly just distinguished for the benefit of tests.
# These start at 10 just to leave some room for future use.
SQL_USER_MISSING = 10
SQL_PASSWORD_MISSING = 11
MSTOOL_FAILED = 12
MSTOOL_MISSING = 13
CTAKES_MISSING = 14
SMART_CREDENTIALS_MISSING = 15
BULK_EXPORT_FAILED = 16
TASK_UNKNOWN = 17
CNLPT_MISSING = 18
TASK_FAILED = 19
TASK_FILTERED_OUT = 20
TASK_SET_EMPTY = 21
ARGS_CONFLICT = 22
ARGS_INVALID = 23
FHIR_URL_MISSING = 24
BASIC_CREDENTIALS_MISSING = 25
FOLDER_NOT_EMPTY = 26
BULK_EXPORT_FOLDER_NOT_LOCAL = 27
CTAKES_RESTART_FAILED = 28
CTAKES_OVERRIDES_INVALID = 29
LABEL_STUDIO_CONFIG_INVALID = 30
LABEL_STUDIO_MISSING = 31
FHIR_AUTH_FAILED = 32
SERVICE_MISSING = 33  # generic init-check service is missing


class FatalError(Exception):
    """An unrecoverable error"""


def fatal(message: str, status: int) -> NoReturn:
    """Convenience method to exit the program with a user-friendly error message a test-friendly status code"""
    print(message, file=sys.stderr)
    sys.exit(status)  # raises a SystemExit exception
