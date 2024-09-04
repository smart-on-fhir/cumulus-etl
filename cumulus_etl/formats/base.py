"""Abstraction for where to write and read data"""

import abc
import logging
from collections.abc import Collection

from cumulus_etl import store
from cumulus_etl.formats.batch import Batch


class Format(abc.ABC):
    """
    An abstraction for how to write cumulus output.

    Subclass this to provide a different output format (like ndjson or deltalake).
    """

    @classmethod
    def initialize_class(cls, root: store.Root) -> None:
        """
        Performs any preparation before any batches have been written, for an entire process.

        (e.g. some expensive setup that can be shared across per-table format instances, or eventually across threads)
        """

    def __init__(
        self,
        root: store.Root,
        dbname: str,
        group_field: str | None = None,
        uniqueness_fields: Collection[str] | None = None,
        update_existing: bool = True,
    ):
        """
        Initialize a new Format class
        :param root: the base location to write data to
        :param dbname: the database name (folder name)
        :param group_field: a field name that if specified, indicates all previous records with a same value should be
         deleted -- for example "docref_id" will mean that any existing rows matching docref_id will be deleted before
         inserting any from this dataframe. Make sure that all records for a given group are in one single dataframe.
         See the comments for the EtlTask.group_field class attribute for more context.
        :param uniqueness_fields: a set of fields that together identify a unique row (defaults to {"id"})
        :param update_existing: whether to update existing rows or (if False) to ignore them and leave them in place
        """
        self.root = root
        self.dbname = dbname
        self.group_field = group_field
        self.uniqueness_fields = uniqueness_fields or {"id"}
        self.update_existing = update_existing

    def write_records(self, batch: Batch) -> bool:
        """
        Writes a single batch of data to the output root.

        The batch must contain no duplicate rows
        (i.e. rows with the same values in all the `uniqueness_fields` columns).

        :param batch: the batch of data
        :returns: whether the batch was successfully written
        """

        try:
            self._write_one_batch(batch)
            return True
        except Exception:
            logging.exception("Could not process data records")
            return False

    @abc.abstractmethod
    def _write_one_batch(self, batch: Batch) -> None:
        """
        Writes a single batch to the output root.

        :param batch: the batch of data
        """

    @abc.abstractmethod
    def delete_records(self, ids: set[str]) -> None:
        """
        Deletes all mentioned IDs from the table.

        :param ids: all IDs to remove
        """

    def finalize(self) -> None:
        """
        Performs any necessary cleanup after all batches have been written.

        Note that this is not guaranteed to be called, if the ETL process gets interrupted.
        """
