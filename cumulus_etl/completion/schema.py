"""Schemas and Format helpers for writing completion tables."""

import pyarrow

COMPLETION_TABLE = "etl__completion"
COMPLETION_ENCOUNTERS_TABLE = "etl__completion_encounters"


# FORMATTERS


def completion_format_args() -> dict:
    """Returns kwargs to pass to the Format class initializer of your choice"""
    return {
        "dbname": COMPLETION_TABLE,
        "uniqueness_fields": {"table_name", "group_name"},
    }


# OUTPUT TABLES


def completion_encounters_output_args() -> dict:
    """Returns output table kwargs for the etl__completion_encounters table"""
    return {
        "name": COMPLETION_ENCOUNTERS_TABLE,
        "uniqueness_fields": {"encounter_id", "group_name"},
        "update_existing": False,  # we want to keep the first export time we make for a group
        "resource_type": None,
        "visible": False,
    }


# SCHEMAS


def completion_schema() -> pyarrow.Schema:
    """Returns a schema for the etl__completion table"""
    return pyarrow.schema(
        [
            pyarrow.field("table_name", pyarrow.string()),
            pyarrow.field("group_name", pyarrow.string()),
            # You might think this is an opportunity to use pyarrow.timestamp(),
            # but because ndjson output formats (which can't natively represent a
            # datetime) would then require conversion to and fro, it's easier to
            # just mirror our FHIR tables and use strings for timestamps.
            pyarrow.field("export_time", pyarrow.string()),
        ]
    )


def completion_encounters_schema() -> pyarrow.Schema:
    """Returns a schema for the etl__completion_encounters table"""
    return pyarrow.schema(
        [
            pyarrow.field("encounter_id", pyarrow.string()),
            pyarrow.field("group_name", pyarrow.string()),
            # See note above for why this isn't a pyarrow.timestamp() field.
            pyarrow.field("export_time", pyarrow.string()),
        ]
    )
