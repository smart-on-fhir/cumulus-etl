"""An implementation of Format that writes to a few flat ndjson files"""

import zoneinfo

import pyarrow

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

    @staticmethod
    def _convert_timestamps(batch: Batch) -> None:
        """
        Convert any timestamp fields from the batch to JSON strings.

        Since JSON can't represent a datetime natively, we need to serialize it to a string.
        """
        for field_name in batch.schema.names:
            # Only bother looking one level deep right now.
            # The only table for which it matters is one we control - etl__completion
            field = batch.schema.field(field_name)
            if pyarrow.types.is_timestamp(field.type):
                target_tz = zoneinfo.ZoneInfo(field.type.tz)
                for row in batch.rows:
                    # Convert to the schema target timezone because:
                    # (A) the schema asked for that timezone
                    # (B) if we were given a naive datetime, this will make it aware
                    target_datetime = row[field_name].astimezone(target_tz)
                    row[field_name] = target_datetime.isoformat()

    def write_format(self, batch: Batch, path: str) -> None:
        metadata = self.make_metadata(batch)
        if metadata:  # Don't bother writing the file for the common case of no metadata
            metadata_path = path.removesuffix(".ndjson") + ".meta"
            common.write_json(metadata_path, metadata, indent=2)

        # Replace actual datetime objects with string timestamps
        self._convert_timestamps(batch)

        # This is mostly used in tests and debugging, so we'll write out sparse files (no null columns)
        common.write_rows_to_ndjson(path, batch.rows, sparse=True)
