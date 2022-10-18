"""Base abstract loader"""

import abc
from typing import Iterator

from fhirclient.models.resource import Resource

from cumulus.store import Root

ResourceIterator = Iterator[Resource]


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
    def load_conditions(self) -> ResourceIterator:
        pass

    @abc.abstractmethod
    def load_docrefs(self) -> ResourceIterator:
        pass

    @abc.abstractmethod
    def load_encounters(self) -> ResourceIterator:
        pass

    @abc.abstractmethod
    def load_labs(self) -> ResourceIterator:
        pass

    @abc.abstractmethod
    def load_patients(self) -> ResourceIterator:
        pass
