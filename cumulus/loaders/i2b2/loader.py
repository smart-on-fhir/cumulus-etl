"""I2B2 loader"""

import json
import os
import tempfile
from typing import Callable, Iterable, Iterator, TypeVar

from fhirclient.models.resource import Resource

from cumulus import common
from cumulus.loaders.base import Loader
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

    def load_all(self) -> tempfile.TemporaryDirectory:
        tmpdir = tempfile.TemporaryDirectory()  # pylint: disable=consider-using-with

        self._loop(
            'csv_diagnosis',
            os.path.join(tmpdir.name, 'Condition.ndjson'),
            extract.extract_csv_observation_facts,
            transform.to_fhir_condition,
        )

        self._loop(
            'csv_lab',
            os.path.join(tmpdir.name, 'Observation.ndjson'),
            extract.extract_csv_observation_facts,
            transform.to_fhir_observation_lab,
        )

        self._loop(
            'csv_note',
            os.path.join(tmpdir.name, 'DocumentReference.ndjson'),
            extract.extract_csv_observation_facts,
            transform.to_fhir_documentreference,
        )

        self._loop(
            'csv_patient',
            os.path.join(tmpdir.name, 'Patient.ndjson'),
            extract.extract_csv_patients,
            transform.to_fhir_patient,
        )

        self._loop(
            'csv_visit',
            os.path.join(tmpdir.name, 'Encounter.ndjson'),
            extract.extract_csv_visits,
            transform.to_fhir_encounter,
        )

        return tmpdir

    def _loop(self, folder: str, output_path: str, extractor: CsvToI2b2Callable, to_fhir: I2b2ToFhirCallable) -> None:
        """Takes one kind of i2b2 resource, loads them all up, and writes out a FHIR ndjson file"""
        with open(output_path, 'w', encoding='utf8') as output_file:
            csv_files = common.list_csv(self.root.joinpath(folder))
            i2b2_entries = self._extract_from_files(extractor, csv_files)
            fhir_resources = (to_fhir(x) for x in i2b2_entries)

            # Now write each FHIR resource line by line to the output
            # (we do this all line by line via generators to avoid loading everything in memory at once)
            for resource in fhir_resources:
                json.dump(resource.as_json(), output_file)
                output_file.write('\n')

    @staticmethod
    def _extract_from_files(extractor: CsvToI2b2Callable, csv_files: Iterable[str]) -> Iterator[schema.Dimension]:
        """Generator method that lazily loads input csv files"""
        for csv_file in csv_files:
            for entry in extractor(csv_file):
                yield entry
