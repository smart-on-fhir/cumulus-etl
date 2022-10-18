"""Ndjson FHIR loader"""

import json
import logging
from typing import Iterator, Type

from fhirclient.models.condition import Condition
from fhirclient.models.documentreference import DocumentReference
from fhirclient.models.encounter import Encounter
from fhirclient.models.observation import Observation
from fhirclient.models.patient import Patient
from fhirclient.models.resource import Resource

from cumulus import common
from cumulus.loaders.base import Loader, ResourceIterator


class FhirNdjsonLoader(Loader):
    """
    Loader for fhir ndjson data.

    Expected format is a folder with one ndjson files per category (i.e. Condition.ndjson)
    TODO: make this a little more flexible, once we know what we want here
    """

    def load_conditions(self) -> ResourceIterator:
        return self._loop(Condition)

    def load_docrefs(self) -> ResourceIterator:
        return self._loop(DocumentReference)

    def load_encounters(self) -> ResourceIterator:
        return self._loop(Encounter)

    def load_labs(self) -> ResourceIterator:
        return self._loop(Observation)

    def load_patients(self) -> ResourceIterator:
        return self._loop(Patient)

    def _loop(self, resource_type: Type[Resource]) -> Iterator:
        filename = f'{resource_type.__name__}.ndjson'
        try:
            with common.open_file(self.root.joinpath(filename), 'r') as f:
                for line in f:
                    yield resource_type(jsondict=json.loads(line), strict=False)
        except FileNotFoundError:
            logging.error('Could not find %s in the input folder. Did you want a different --input-format value?',
                          filename)
