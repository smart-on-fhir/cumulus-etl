"""Code to help managing a single batch of data"""

import pyarrow


class Batch:
    """
    A single chunk of FHIR data, winding its way through the ETL.

    A batch is:
    - Always the same FHIR resource
    - Kept to roughly --batch-size in size
      - It may be less if we have less data or duplicates were pruned
      - It may be more if a group_field wanted to make sure some rows were in the same batch (see OutputTable)
    - Written to the target location as one piece (e.g. one ndjson file or one Delta Lake update chunk)
    """

    def __init__(
        self, rows: list[dict], groups: set[str] | None = None, schema: pyarrow.Schema = None
    ):
        self.rows = rows
        # `groups` is the set of the values of the format's `group_field` represented by `rows`.
        # We can't just get this from rows directly because there might be groups that now have zero entries.
        # And those won't be in rows, but should still be removed from the database.
        self.groups = groups
        self.schema = schema
