"""An implementation of Format that writes to a few parquet files"""

import cumulus_fhir_support as cfs
import pyarrow

from cumulus_etl.formats.batch import Batch
from cumulus_etl.formats.batched_files import BatchedFileFormat
from cumulus_etl.formats.nlp import AthenaMixin


class NlpParquetFormat(AthenaMixin, BatchedFileFormat):
    """Stores output files in a few parquet files"""

    @property
    def suffix(self) -> str:
        return "parquet"

    def write_format(self, batch: Batch, path: cfs.FsPath) -> None:
        table = pyarrow.Table.from_pylist(batch.rows, schema=batch.schema)
        pyarrow.parquet.write_table(table, str(path), compression="snappy", filesystem=path.fs)

        super().write_format(batch, path)

    def _athena_args(self) -> tuple[str, str]:
        return "STORED AS PARQUET", 'TBLPROPERTIES ("parquet.compression"="SNAPPY")'
