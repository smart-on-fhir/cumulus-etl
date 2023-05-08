"""Abstraction for where to write and read data"""

import abc
import logging

import pandas

from cumulus_etl import store


class Format(abc.ABC):
    """
    An abstraction for how to write cumulus output.

    Subclass this to provide a different output format (like ndjson or parquet).
    """

    @classmethod
    def initialize_class(cls, root: store.Root) -> None:
        """
        Performs any preparation before any batches have been written, for an entire process.

        (e.g. some expensive setup that can be shared across per-table format instances, or eventually across threads)
        """

    def __init__(self, root: store.Root, dbname: str, group_field: str = None):
        """
        Initialize a new Format class
        :param root: the base location to write data to
        :param dbname: the database name (folder name)
        :param group_field: a field name that if specified, indicates all previous records with a same value should be
         deleted -- for example "docref_id" will mean that any existing rows matching docref_id will be deleted before
         inserting any from this dataframe. Make sure that all records for a given group are in one single dataframe.
         See the comments for the EtlTask.group_field class attribute for more context.
        """
        self.root = root
        self.dbname = dbname
        self.group_field = group_field

    def write_records(self, dataframe: pandas.DataFrame, batch: int) -> bool:
        """
        Writes a single dataframe to the output root.

        The dataframe must contain a unique (no duplicates) "id" column.

        :param dataframe: the data records to write
        :param batch: the batch number, from zero up
        :returns: whether the batch was successfully written
        """

        try:
            self._write_one_batch(dataframe, batch)
            return True
        except Exception:  # pylint: disable=broad-except
            logging.exception("Could not process data records")
            return False

    @abc.abstractmethod
    def _write_one_batch(self, dataframe: pandas.DataFrame, batch: int) -> None:
        """
        Writes a single dataframe to the output root.

        :param dataframe: the data records to write
        :param batch: the batch number, from zero up
        """

    def finalize(self) -> None:
        """
        Performs any necessary cleanup after all batches have been written.

        Note that this is not guaranteed to be called, if the ETL process gets interrupted.
        """
