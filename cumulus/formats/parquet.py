"""An implementation of Format that writes to a few flat parquet files"""

import pandas

from .athena import AthenaBatchedFileFormat


class ParquetFormat(AthenaBatchedFileFormat):
    """Stores output files in a few flat parquet files"""

    @property
    def suffix(self) -> str:
        return 'parquet'

    def write_format(self, df: pandas.DataFrame, path: str) -> None:
        df.to_parquet(path, index=False, storage_options=self.root.fsspec_options())
