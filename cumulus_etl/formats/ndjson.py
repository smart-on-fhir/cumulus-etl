"""An implementation of Format that writes to a few flat ndjson files"""

from cumulus_etl import common
from cumulus_etl.formats.batch import Batch
from cumulus_etl.formats.batched_files import BatchedFileFormat


class NdjsonFormat(BatchedFileFormat):
    """Stores output files in a few flat ndjson files"""

    @property
    def suffix(self) -> str:
        return "ndjson"

    def make_metadata(self, batch: Batch) -> dict:
        """
        Craft a JSON dictionary for per-batch metadata.

        This is necessary to convey information that isn't necessary for the ndjson output format,
        but is useful for other formats like deltalake.
        This way, the `convert` command can losslessly pass that info along.
        """
        metadata = {}

        if self.group_field and batch.groups:
            metadata["groups"] = sorted(batch.groups)  # sort them just for ease of testing

        return metadata

    def write_format(self, batch: Batch, path: str) -> None:
        metadata = self.make_metadata(batch)
        if metadata:  # Don't bother writing the file for the common case of no metadata
            metadata_path = path.removesuffix(".ndjson") + ".meta"
            common.write_json(metadata_path, metadata, indent=2)

        # This is mostly used in tests and debugging, so we'll write out sparse files (no null columns)
        common.write_rows_to_ndjson(path, batch.rows, sparse=True)

    def table_metadata_path(self) -> str:
        return self.dbroot.joinpath(f"{self.dbname}.meta")  # no batch number

    def read_table_metadata(self) -> dict:
        try:
            return common.read_json(self.table_metadata_path())
        except (FileNotFoundError, PermissionError):
            return {}

    def write_table_metadata(self, metadata: dict) -> None:
        self.root.makedirs(self.dbroot.path)
        common.write_json(self.table_metadata_path(), metadata, indent=2)

    def delete_records(self, ids: set[str]) -> None:
        # Read and write back table metadata, with the addition of these new deleted IDs
        meta = self.read_table_metadata()
        meta.setdefault("deleted", []).extend(sorted(ids))
        self.write_table_metadata(meta)
