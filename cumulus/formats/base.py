"""Abstraction for where to write and read data"""

import abc

import pandas

from cumulus import store


class Format(abc.ABC):
    """
    An abstraction for how to write cumulus output.

    Subclass this to provide a different output format (like ndjson or parquet).
    """

    def __init__(self, root: store.Root):
        """
        Initialize a new Format class
        :param root: the base location to write data to
        """
        self.root = root

    def initialize(self, summary, dbname: str) -> None:
        """
        Performs any preparation before any batches have been written.

        :param summary: JobSummary to be filled as records are written
        :param dbname: the database name (folder name)
        """

    @abc.abstractmethod
    def write_records(
        self, summary, dataframe: pandas.DataFrame, dbname: str, batch: int, group_field: str = None
    ) -> None:
        """
        Writes a single dataframe to the output root.

        :param summary: JobSummary to be filled as records are written
        :param dataframe: the data records to write
        :param dbname: the database name (folder name)
        :param batch: the batch number, from zero up
        :param group_field: a field name that if specified, indicates all previous records with a same value should be
         deleted -- for example "docref_id" will mean that any existing rows matching docref_id will be deleted before
         inserting any from this dataframe. Make sure that all records for a given group are in one single dataframe.
         See the comments for the EtlTask.group_field class attribute for more context.
        """

    def finalize(self, summary, dbname: str) -> None:
        """
        Performs any necessary cleanup after all batches have been written.

        Note that this is not guaranteed to be called, if the ETL process gets interrupted.

        :param summary: JobSummary to be filled as records are written
        :param dbname: the database name (folder name)
        """
