"""An implementation of Format designed to write in batches of files"""

import abc
import logging
import os

import pandas

from cumulus.formats.base import Format


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

    def write_records(self, summary, dataframe: pandas.DataFrame, dbname: str, batch: int) -> None:
        """Writes the whole dataframe to a single file"""
        summary.attempt += len(dataframe)

        if batch == 0:
            # First batch, let's clear out any existing files before writing any new ones.
            # Note: There is a real issue here where Athena will see invalid results until we've written all
            #       our files out. Use the deltalake format to get atomic updates.
            parent_dir = self.root.joinpath(dbname)
            try:
                self.root.rm(parent_dir, recursive=True)
            except FileNotFoundError:
                pass

        try:
            full_path = self.root.joinpath(f"{dbname}/{dbname}.{batch:03}.{self.suffix}")
            self.root.makedirs(os.path.dirname(full_path))
            self.write_format(dataframe, full_path)

            summary.success += len(dataframe)
            summary.success_rate(1)
        except Exception:  # pylint: disable=broad-except
            logging.exception("Could not process data records")
