"""Base abstract loader"""

import abc

from cumulus_etl import common, store


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
    async def load_all(self, resources: list[str]) -> common.Directory:
        """
        Loads the listed remote resources and places them into a local folder as FHIR ndjson

        :param resources: a list of resources to ingest
        :returns: an object holding the name of a local ndjson folder path (e.g. a TemporaryDirectory)
        """
