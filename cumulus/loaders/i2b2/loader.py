"""I2B2 loader"""

from typing import Callable, Iterable, Iterator, TypeVar

from fhirclient.models.resource import Resource

from cumulus import common
from cumulus.loaders.base import Loader, ResourceIterator
from cumulus.loaders.i2b2 import extract, schema, transform

AnyDimension = TypeVar('AnyDimension', bound=schema.Dimension)
CsvToI2b2Callable = Callable[[str], Iterable[schema.Dimension]]
I2b2ToFhirCallable = Callable[[AnyDimension], Resource]


class I2b2Loader(Loader):
    """
    Loader for i2b2 csv data.

    Expected format is any number of csv files in the following subdirectories:
    - csv_diagnosis
    - csv_lab
    - csv_note
    - csv_patient
    - csv_visit
    """

    def load_conditions(self) -> ResourceIterator:
        return self._loop(
            'csv_diagnosis',
            extract.extract_csv_observation_facts,
            transform.to_fhir_condition,
        )

    def load_docrefs(self) -> ResourceIterator:
        return self._loop(
            'csv_note',
            extract.extract_csv_observation_facts,
            transform.to_fhir_documentreference,
        )

    def load_encounters(self) -> ResourceIterator:
        return self._loop(
            'csv_visit',
            extract.extract_csv_visits,
            transform.to_fhir_encounter,
        )

    def load_labs(self) -> ResourceIterator:
        return self._loop(
            'csv_lab',
            extract.extract_csv_observation_facts,
            transform.to_fhir_observation_lab,
        )

    def load_patients(self) -> ResourceIterator:
        return self._loop(
            'csv_patient',
            extract.extract_csv_patients,
            transform.to_fhir_patient,
        )

    def _loop(self, folder: str, extractor: CsvToI2b2Callable, to_fhir: I2b2ToFhirCallable) -> Iterator:
        csv_files = common.list_csv(self.root.joinpath(folder))
        entries = self._extract_from_files(extractor, csv_files)
        return (to_fhir(x) for x in entries)

    @staticmethod
    def _extract_from_files(extractor: CsvToI2b2Callable, csv_files: Iterable[str]) -> Iterator[schema.Dimension]:
        """Generator method that lazily loads input csv files"""
        for csv_file in csv_files:
            for entry in extractor(csv_file):
                yield entry
