"""Exception classes and error handling"""

from typing import NoReturn

# Error return codes, mostly just distinguished for the benefit of tests.
# These start at 10 just to leave some room for future use.
SQL_USER_MISSING = 10
SQL_PASSWORD_MISSING = 11
# MSTOOL_FAILED = 12 # Obsolete, feature removed
# MSTOOL_MISSING = 13 # Obsolete, feature removed
CTAKES_MISSING = 14
SMART_CREDENTIALS_MISSING = 15
# BULK_EXPORT_FAILED = 16 # Obsolete, feature removed
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
# CTAKES_OVERRIDES_INVALID = 29  # Obsolete, feature removed
LABEL_STUDIO_CONFIG_INVALID = 30
LABEL_STUDIO_MISSING = 31
FHIR_AUTH_FAILED = 32
SERVICE_MISSING = 33  # generic init-check service is missing
COMPLETION_ARG_MISSING = 34
TASK_HELP = 35
MISSING_REQUESTED_RESOURCES = 36
TOO_MANY_SMART_CREDENTIALS = 37
BAD_SMART_CREDENTIAL = 38
# INLINE_TASK_FAILED = 39  # Obsolete, feature removed
# INLINE_WITHOUT_FOLDER = 40  # Obsolete, feature removed
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
NOTES_NOT_FOUND = 55
NOTES_NOT_FOUND_WITH_TEXT = 56
NOTES_NOT_FOUND_WITH_FILTER = 57
NOTES_TOO_FEW = 58
FEATURE_REMOVED = 59


class FatalError(SystemExit):
    """
    An unrecoverable error.

    Raising an error this way instead of calling printing and/or calling sys.exit() ourselves is
    nice for two reasons:
    - Easier to catch/handle/inspect in tests
    - Lets rich live regions (like progress bars) finish before printing our message, which might
      otherwise get eaten up by rich's rendering.
    """

    def __init__(self, message: str, status: int, details: str = ""):
        super().__init__(status)
        self.message = message
        self.status = status
        self.details = details


class FhirConnectionConfigError(FatalError):
    """We needed to connect to a FHIR server but are not configured correctly"""


def fatal(message: str, status: int, details: str = "") -> NoReturn:
    """Exits the program with a user-friendly error message and a test-friendly status code"""
    raise FatalError(message, status, details=details)
