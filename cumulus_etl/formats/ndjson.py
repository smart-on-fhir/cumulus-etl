"""An implementation of Format that writes to a few flat ndjson files"""

from cumulus_etl import common
from cumulus_etl.formats.batch import Batch
from cumulus_etl.formats.batched_files import BatchedFileFormat


class NdjsonFormat(BatchedFileFormat):
    """Stores output files in a few flat ndjson files"""

    @property
    def suffix(self) -> str:
        return "ndjson"

    def write_format(self, batch: Batch, path: str) -> None:
        # This is mostly used in tests and debugging, so we'll write out sparse files (no null columns)
        common.write_rows_to_ndjson(path, batch.rows, sparse=True)
