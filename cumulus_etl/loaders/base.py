"""Base abstract loader"""

import abc
import dataclasses
import datetime

from cumulus_etl import common, store


@dataclasses.dataclass(kw_only=True)
class LoaderResults:
    """Bundles results of a load request"""

    # Where loaded files reside on disk (use .path for convenience)
    directory: common.Directory

    @property
    def path(self) -> str:
        return self.directory.name

    # Completion tracking values - noting an export group name for this bundle of data
    # and the time when it was exported ("transactionTime" in bulk-export terms).
    group_name: str | None = None
    export_datetime: datetime.datetime | None = None


class Loader(abc.ABC):
    """
    An abstraction for how to load FHIR input

    Subclass this to provide a different input format (like ndjson or i2b2).

    All methods return an iterator over FHIR resources.
    """

    def __init__(self, root: store.Root):
        """
        Initialize a new Loader class
        :param root: the base location to read data from
        """
        self.root = root

    @abc.abstractmethod
    async def load_all(self, resources: list[str]) -> LoaderResults:
        """
        Loads the listed remote resources and places them into a local folder as FHIR ndjson

        :param resources: a list of resources to ingest
        :returns: an object holding the name of a local ndjson folder path (e.g. a TemporaryDirectory)
        """
