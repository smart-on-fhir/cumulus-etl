"""Base abstract loader"""

import abc
import tempfile

from cumulus.store import Root


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
    def load_all(self) -> tempfile.TemporaryDirectory:
        """
        Loads all remote resources and places them into a local folder as FHIR ndjson

        :returns: an object holding the name of a local ndjson folder path (e.g. a TemporaryDirectory)
        """
