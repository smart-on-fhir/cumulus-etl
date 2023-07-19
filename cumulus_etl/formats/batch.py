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

    def __init__(self, rows: list[dict], schema: pyarrow.Schema = None, index: int = 0):
        self.rows = rows
        self.schema = schema
        self.index = index
