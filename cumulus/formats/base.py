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

    @abc.abstractmethod
    def write_records(self, summary, dataframe: pandas.DataFrame, dbname: str, batch: int) -> None:
        """Writes a single dataframe to the output root"""
