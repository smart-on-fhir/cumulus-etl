"""An implementation of Format that writes to a few flat ndjson files"""

import cumulus_fhir_support as cfs

from cumulus_etl import common
from cumulus_etl.formats.batch import Batch
from cumulus_etl.formats.batched_files import BatchedFileFormat
from cumulus_etl.formats.nlp import AthenaMixin


class NdjsonFormat(BatchedFileFormat):
    """Stores output files in a few flat ndjson files"""

    @property
    def suffix(self) -> str:
        return "ndjson"

    def write_format(self, batch: Batch, path: cfs.FsPath) -> None:
        # This is mostly used in tests and debugging, so we'll write out sparse files
        # (no null columns)
        common.write_rows_to_ndjson(path, batch.rows, sparse=True)

        super().write_format(batch, path)

    def table_metadata_path(self) -> cfs.FsPath:
        return self.dbroot.joinpath(f"{self.dbname}.meta")  # no batch number

    def read_table_metadata(self) -> dict:
        return self.table_metadata_path().read_json(default={})

    def write_table_metadata(self, metadata: dict) -> None:
        self.dbroot.makedirs()
        self.table_metadata_path().write_json(metadata, indent=2)

    def delete_records(self, ids: set[str]) -> None:
        # Read and write back table metadata, with the addition of these new deleted IDs
        meta = self.read_table_metadata()
        meta.setdefault("deleted", []).extend(sorted(ids))
        self.write_table_metadata(meta)


class NlpNdjsonFormat(AthenaMixin, NdjsonFormat):
    def _athena_args(self) -> tuple[str, str]:
        return "ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'", ""
