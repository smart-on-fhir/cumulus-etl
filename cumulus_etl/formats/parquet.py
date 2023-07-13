"""An implementation of Format that writes to a few flat parquet files"""

import pyarrow.parquet

from cumulus_etl.formats.batch import Batch
from cumulus_etl.formats.batched_files import BatchedFileFormat


class ParquetFormat(BatchedFileFormat):
    """Stores output files in a few flat parquet files"""

    @property
    def suffix(self) -> str:
        return "parquet"

    def write_format(self, batch: Batch, path: str) -> None:
        table = pyarrow.Table.from_pylist(batch.rows, schema=batch.schema)
        pyarrow.parquet.write_table(table, path)
