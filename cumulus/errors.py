"""Exception classes and error handling"""

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
