"""Abstraction for where to write and read data"""

import abc
import logging

import pandas

from cumulus import store


class Format(abc.ABC):
    """
    An abstraction for how to write cumulus output.

    Subclass this to provide a different output format (like ndjson or parquet).
    """

    @classmethod
    def initialize_class(cls, root: store.Root) -> None:
        """
        Performs any preparation before any batches have been written.

        (e.g. some cross-thread expensive setup)
        """

    def __init__(self, root: store.Root, summary, dbname: str, group_field: str):
        """
        Initialize a new Format class
        :param root: the base location to write data to
        :param summary: JobSummary to be filled as records are written
        :param dbname: the database name (folder name)
        :param group_field: a field name that if specified, indicates all previous records with a same value should be
         deleted -- for example "docref_id" will mean that any existing rows matching docref_id will be deleted before
         inserting any from this dataframe. Make sure that all records for a given group are in one single dataframe.
         See the comments for the EtlTask.group_field class attribute for more context.
        """
        self.root = root
        self.summary = summary
        self.dbname = dbname
        self.group_field = group_field

    def initialize(self) -> None:
        """
        Performs any preparation before any batches have been written.
        """

    def write_records(self, dataframe: pandas.DataFrame, batch: int) -> None:
        """
        Writes a single dataframe to the output root.

        :param dataframe: the data records to write
        :param batch: the batch number, from zero up
        """
        count = len(dataframe)
        self.summary.attempt += count

        try:
            self._write_one_batch(dataframe, batch)
            self.summary.success += count
        except Exception:  # pylint: disable=broad-except
            logging.exception("Could not process data records")

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
