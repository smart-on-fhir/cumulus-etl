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
    export_url: str | None = None

    # A list of resource IDs that should be deleted from the output tables.
    # This is a map of resource -> set of IDs like {"Patient": {"A", "B"}}
    deleted_ids: dict[str, set[str]] = dataclasses.field(default_factory=dict)


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
    async def detect_resources(self) -> set[str] | None:
        """
        Inspect which resources are available for use.

        :returns: the types of resources detected (or None if that can't be determined yet)
        """

    @abc.abstractmethod
    async def load_resources(self, resources: set[str]) -> LoaderResults:
        """
        Loads the listed remote resources and places them into a local folder as FHIR ndjson

        :param resources: a list of resources to ingest
        :returns: an object holding the name of a local ndjson folder path (e.g. a TemporaryDirectory)
        """
