"""An implementation of Format designed to write in batches of files"""

import abc
import re

from cumulus_etl import errors, store
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

    def __init__(self, *args, **kwargs) -> None:
        """Performs any preparation before any batches have been written."""
        super().__init__(*args, **kwargs)

        # Let's clear out any existing files before writing any new ones.
        # Note: There is a real issue here where Athena will see invalid results until we've written all
        #       our files out. Use the deltalake format to get atomic updates.
        parent_dir = self.root.joinpath(self.dbname)
        self._confirm_no_unknown_files_exist(parent_dir)
        try:
            self.root.rm(parent_dir, recursive=True)
        except FileNotFoundError:
            pass

    def _confirm_no_unknown_files_exist(self, folder: str) -> None:
        """
        Errors out if any unknown files exist in the target dir already.

        This is designed to prevent accidents.
        """
        try:
            filenames = [path.split("/")[-1] for path in store.Root(folder).ls()]
        except FileNotFoundError:
            return  # folder doesn't exist, we're good!

        allowed_pattern = re.compile(rf"{self.dbname}\.[0-9]+\.{self.suffix}")
        if not all(map(allowed_pattern.fullmatch, filenames)):
            errors.fatal(
                f"There are unexpected files in the output folder '{folder}'.\n"
                f"Please confirm you are using the right output format.\n"
                f"If so, delete the output folder and try again.",
                errors.FOLDER_NOT_EMPTY,
            )

    def _write_one_batch(self, batch: Batch) -> None:
        """Writes the whole dataframe to a single file"""
        self.root.makedirs(self.root.joinpath(self.dbname))
        full_path = self.root.joinpath(f"{self.dbname}/{self.dbname}.{batch.index:03}.{self.suffix}")
        self.write_format(batch, full_path)
