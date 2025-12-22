"""Exception classes and error handling"""

import sys
from typing import NoReturn

import rich.console

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
# TASK_FILTERED_OUT = 20 # Obsolete, we don't have --task-filter anymore
# TASK_SET_EMPTY = 21 # Obsolete, we don't have --task-filter anymore
ARGS_CONFLICT = 22
ARGS_INVALID = 23
# FHIR_URL_MISSING = 24 # Obsolete, it's no longer fatal
BASIC_CREDENTIALS_MISSING = 25
FOLDER_NOT_EMPTY = 26
BULK_EXPORT_FOLDER_NOT_LOCAL = 27
CTAKES_RESTART_FAILED = 28
CTAKES_OVERRIDES_INVALID = 29
LABEL_STUDIO_CONFIG_INVALID = 30
LABEL_STUDIO_MISSING = 31
FHIR_AUTH_FAILED = 32
SERVICE_MISSING = 33  # generic init-check service is missing
COMPLETION_ARG_MISSING = 34
TASK_HELP = 35
MISSING_REQUESTED_RESOURCES = 36
TOO_MANY_SMART_CREDENTIALS = 37
BAD_SMART_CREDENTIAL = 38
INLINE_TASK_FAILED = 39
INLINE_WITHOUT_FOLDER = 40
WRONG_PHI_FOLDER = 41
# TASK_NOT_PROVIDED = 42  # checked now by argparse
TASK_MISMATCH = 43
ATHENA_TABLE_TOO_BIG = 44
ATHENA_TABLE_NAME_INVALID = 45
ATHENA_DATABASE_MISSING = 46
MULTIPLE_COHORT_ARGS = 47
COHORT_NOT_FOUND = 48
MULTIPLE_LABELING_ARGS = 49
LABEL_UNKNOWN = 50
LABEL_CONFIG_TYPE_UNKNOWN = 51
TASK_TOO_MANY = 52
NLP_BATCHING_UNSUPPORTED = 53
FOLDER_DOES_NOT_EXIST = 54


class FatalError(Exception):
    """An unrecoverable error"""


class FhirConnectionConfigError(FatalError):
    """We needed to connect to a FHIR server but are not configured correctly"""


def fatal(message: str, status: int, extra: str = "") -> NoReturn:
    """Convenience method to exit the program with a user-friendly error message a test-friendly status code"""
    stderr = rich.console.Console(stderr=True)
    stderr.print(message, style="bold red", highlight=False)
    if extra:
        stderr.print(rich.padding.Padding.indent(extra, 2), highlight=False)
    sys.exit(status)  # raises a SystemExit exception
