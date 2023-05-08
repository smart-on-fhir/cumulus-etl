"""An implementation of Format designed to write in batches of files"""

import abc

import pandas

from cumulus_etl.formats.base import Format


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
    def write_format(self, df: pandas.DataFrame, path: str) -> None:
        """
        Write the data in `df` to the target path file
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
        try:
            self.root.rm(parent_dir, recursive=True)
        except FileNotFoundError:
            pass
        self.root.makedirs(parent_dir)

    def _write_one_batch(self, dataframe: pandas.DataFrame, batch: int) -> None:
        """Writes the whole dataframe to a single file"""
        full_path = self.root.joinpath(f"{self.dbname}/{self.dbname}.{batch:03}.{self.suffix}")
        self.write_format(dataframe, full_path)
