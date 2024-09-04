"""An implementation of Format designed to write in batches of files"""

import abc
import os
import re

from cumulus_etl import cli_utils, store
from cumulus_etl.formats.base import Format
from cumulus_etl.formats.batch import Batch


class BatchedFileFormat(Format):
    """
    Stores output files as batched individual files.

    i.e. a few ndjson files that hold all the rows
    """

    @property
    @abc.abstractmethod
    def suffix(self) -> str:
        """
        The suffix to use for any files written out

        Honestly, not super necessary, since S3 filetypes are independent of suffix. But useful if writing locally.
        """

    @abc.abstractmethod
    def write_format(self, batch: Batch, path: str) -> None:
        """
        Write the data in `batch` to the target path file
        """

    ##########################################################################################
    #
    # Implementation details below
    #
    ##########################################################################################

    @classmethod
    def initialize_class(cls, root: store.Root) -> None:
        # The ndjson formatter has a few main use cases:
        # - unit testing
        # - manual testing
        # - an initial ETL run, manual inspection, then converting that to deltalake
        #
        # In all those use cases, we don't really need to re-use the same directory.
        # And re-using the target directory can cause problems:
        # - accidentally overriding important data
        # - how should we handle the 2nd ETL run writing less / different batched files?
        #
        # So we just confirm that the output folder is empty - let's avoid the whole thing.
        # But we do it in class-init rather than object-init because other tasks will create
        # files here during the ETL run.
        cli_utils.confirm_dir_is_empty(root)

    def __init__(self, *args, **kwargs) -> None:
        """Performs any preparation before any batches have been written."""
        super().__init__(*args, **kwargs)

        self.dbroot = store.Root(self.root.joinpath(self.dbname))

        # Grab the next available batch index to write.
        # You might wonder why we do this, if we already checked that the output folder is empty
        # during class initialization.
        # But some output tables (like etl__completion) are written to in many small batches
        # spread over the whole ETL run - so we need to support that workflow.
        self._index = self._get_next_index()

    def _get_next_index(self) -> int:
        try:
            basenames = [os.path.basename(path) for path in self.dbroot.ls()]
        except FileNotFoundError:
            return 0
        pattern = re.compile(rf"{self.dbname}\.([0-9]+)\.{self.suffix}")
        matches = [pattern.match(basename) for basename in basenames]
        numbers = [int(match.group(1)) for match in matches if match]
        return max(numbers, default=-1) + 1

    def _write_one_batch(self, batch: Batch) -> None:
        """Writes the whole dataframe to a single file"""
        self.root.makedirs(self.dbroot.path)
        full_path = self.dbroot.joinpath(f"{self.dbname}.{self._index:03}.{self.suffix}")
        self.write_format(batch, full_path)
        self._index += 1

    def delete_records(self, ids: set[str]) -> None:
        """
        Deletes the given IDs.

        Though this is a no-op for batched file outputs, since:
        - we guarantee the output folder is empty at the start
        - the spec says deleted IDs won't overlap with output IDs

        But subclasses may still want to write these to disk to preserve the metadata.
        """
