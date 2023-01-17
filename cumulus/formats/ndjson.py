"""An implementation of Format that writes to a few flat ndjson files"""

import pandas

from cumulus.formats.batched_files import BatchedFileFormat


class NdjsonFormat(BatchedFileFormat):
    """Stores output files in a few flat ndjson files"""

    @property
    def suffix(self) -> str:
        return "ndjson"

    def write_format(self, df: pandas.DataFrame, path: str) -> None:
        df.to_json(path, orient="records", lines=True, storage_options=self.root.fsspec_options())
