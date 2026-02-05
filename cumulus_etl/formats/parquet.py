"""An implementation of Format that writes to a few parquet files"""

import pyarrow

from cumulus_etl import store
from cumulus_etl.formats.batch import Batch
from cumulus_etl.formats.batched_files import BatchedFileFormat
from cumulus_etl.formats.nlp import AthenaMixin


class NlpParquetFormat(AthenaMixin, BatchedFileFormat):
    """Stores output files in a few parquet files"""

    @property
    def suffix(self) -> str:
        return "parquet"

    def write_format(self, batch: Batch, path: str) -> None:
        fs = store.Root(path).fs
        table = pyarrow.Table.from_pylist(batch.rows, schema=batch.schema)
        pyarrow.parquet.write_table(table, path, compression="snappy", filesystem=fs)

    def _athena_args(self) -> tuple[str, str]:
        return "STORED AS PARQUET", 'TBLPROPERTIES ("parquet.compression"="SNAPPY")'
