"""Exception classes and error handling"""

import sys
from typing import NoReturn

import httpx
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
TASK_FILTERED_OUT = 20
TASK_SET_EMPTY = 21
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


class FatalError(Exception):
    """An unrecoverable error"""


class NetworkError(FatalError):
    """
    A network error

    The response field may be None in cases where we failed before we could get a response.
    Like DNS errors or other transport errors.
    """

    def __init__(self, msg: str, response: httpx.Response | None):
        super().__init__(msg)
        self.response = response


class FatalNetworkError(NetworkError):
    """An unrecoverable network error that should not be retried"""


class TemporaryNetworkError(NetworkError):
    """An recoverable network error that could be retried"""


class FhirConnectionConfigError(FatalError):
    """We needed to connect to a FHIR server but are not configured correctly"""


class FhirUrlMissing(FhirConnectionConfigError):
    """We needed to connect to a FHIR server but no URL was provided"""

    def __init__(self):
        super().__init__("Could not download some files without a FHIR server URL (use --fhir-url)")


class FhirAuthMissing(FhirConnectionConfigError):
    """We needed to connect to a FHIR server but no authentication config was provided"""

    def __init__(self):
        super().__init__(
            "Could not download some files without authentication parameters (see --help)"
        )


def fatal(message: str, status: int, extra: str = "") -> NoReturn:
    """Convenience method to exit the program with a user-friendly error message a test-friendly status code"""
    stderr = rich.console.Console(stderr=True)
    stderr.print(message, style="bold red", highlight=False)
    if extra:
        stderr.print(rich.padding.Padding.indent(extra, 2), highlight=False)
    sys.exit(status)  # raises a SystemExit exception
