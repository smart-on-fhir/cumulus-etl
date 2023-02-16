"""Base abstract loader"""

import abc
from typing import List, Protocol

from cumulus.store import Root


# Loaders often want the ability to provide callers with a TemporaryDirectory -- a folder that will be deleted as soon
# as the caller is done using it. However, some code flows may instead require returning a folder that already exists.
# So for that use case, we define a Directory protocol for typing purposes, which looks like a TemporaryDirectory.
# And then a RealDirectory class that does not delete its folder.
class Directory(Protocol):
    name: str


class RealDirectory:
    def __init__(self, path: str):
        self.name = path


class Loader(abc.ABC):
    """
    An abstraction for how to load FHIR input

    Subclass this to provide a different input format (like ndjson or i2b2).

    All methods return an iterator over FHIR resources.
    """

    def __init__(self, root: Root):
        """
        Initialize a new Loader class
        :param root: the base location to read data from
        """
        self.root = root

    @abc.abstractmethod
    async def load_all(self, resources: List[str]) -> Directory:
        """
        Loads the listed remote resources and places them into a local folder as FHIR ndjson

        :param resources: a list of resources to ingest
        :returns: an object holding the name of a local ndjson folder path (e.g. a TemporaryDirectory)
        """
