"""I2B2 loader"""

import json
import os
import tempfile
from functools import partial
from typing import Callable, Iterable, Iterator, List, TypeVar

from fhirclient.models.resource import Resource

from cumulus import common
from cumulus.loaders.base import Loader
from cumulus.loaders.i2b2 import extract, schema, transform
from cumulus.loaders.i2b2.oracle import extract as oracle_extract

AnyDimension = TypeVar('AnyDimension', bound=schema.Dimension)
I2b2ExtractorCallable = Callable[[], Iterable[schema.Dimension]]
CsvToI2b2Callable = Callable[[str], Iterable[schema.Dimension]]
I2b2ToFhirCallable = Callable[[AnyDimension], Resource]


class I2b2Loader(Loader):
    """
    Loader for i2b2 data.

    Expected format is either a tcp:// URL pointing at an Oracle server or a local folder,
    holding any number of csv files in the following subdirectories:
    - csv_diagnosis
    - csv_lab
    - csv_note
    - csv_patient
    - csv_visit
    """

    def load_all(self, resources: List[str]) -> tempfile.TemporaryDirectory:
        if self.root.protocol in ['tcp']:
            return self._load_all_from_oracle(resources)

        return self._load_all_from_csv(resources)

    def _load_all_with_extractors(
        self,
        resources: List[str],
        conditions: I2b2ExtractorCallable,
        observations: I2b2ExtractorCallable,
        documentreferences: I2b2ExtractorCallable,
        patients: I2b2ExtractorCallable,
        encounters: I2b2ExtractorCallable,
    ) -> tempfile.TemporaryDirectory:
        """
        Load i2b2 content into a local folder as ndjson

        Argument names are short to encourage treating them as kwargs for easier readability.
        """
        tmpdir = tempfile.TemporaryDirectory()  # pylint: disable=consider-using-with

        if 'Condition' in resources:
            self._loop(
                conditions(),
                transform.to_fhir_condition,
                os.path.join(tmpdir.name, 'Condition.ndjson'),
            )

        if 'Observation' in resources:
            self._loop(
                observations(),
                transform.to_fhir_observation_lab,
                os.path.join(tmpdir.name, 'Observation.ndjson'),
            )

        if 'DocumentReference' in resources:
            self._loop(
                documentreferences(),
                transform.to_fhir_documentreference,
                os.path.join(tmpdir.name, 'DocumentReference.ndjson'),
            )

        if 'Patient' in resources:
            self._loop(
                patients(),
                transform.to_fhir_patient,
                os.path.join(tmpdir.name, 'Patient.ndjson'),
            )

        if 'Encounter' in resources:
            self._loop(
                encounters(),
                transform.to_fhir_encounter,
                os.path.join(tmpdir.name, 'Encounter.ndjson'),
            )

        return tmpdir

    def _loop(self, i2b2_entries: Iterable[schema.Dimension], to_fhir: I2b2ToFhirCallable, output_path: str) -> None:
        """Takes one kind of i2b2 resource, loads them all up, and writes out a FHIR ndjson file"""
        fhir_resources = (to_fhir(x) for x in i2b2_entries)

        ids = set()  # keep track of every ID we've seen so far, because sometimes i2b2 can have duplicates

        with open(output_path, 'w', encoding='utf8') as output_file:
            # Now write each FHIR resource line by line to the output
            # (we do this all line by line via generators to avoid loading everything in memory at once)
            for resource in fhir_resources:
                if resource.id in ids:
                    continue
                ids.add(resource.id)
                json.dump(resource.as_json(), output_file)
                output_file.write('\n')

    ###################################################################################################################
    #
    # CSV code
    #
    ###################################################################################################################

    @staticmethod
    def _extract_csv_files(extractor: CsvToI2b2Callable, csv_files: Iterable[str]) -> Iterator[schema.Dimension]:
        """Generator method that lazily loads a list of input csv files"""
        for csv_file in csv_files:
            for entry in extractor(csv_file):
                yield entry

    def _extract_csv_dir(self, folder: str, extractor: CsvToI2b2Callable) -> Iterator[schema.Dimension]:
        """Generator method that lazily loads all input csv files in the given folder"""
        csv_files = common.list_csv(folder)
        return self._extract_csv_files(extractor, csv_files)

    def _load_all_from_csv(self, resources: List[str]) -> tempfile.TemporaryDirectory:
        path = self.root.path
        return self._load_all_with_extractors(
            resources,
            conditions=partial(self._extract_csv_dir, os.path.join(path, 'csv_diagnosis'),
                               extract.extract_csv_observation_facts),
            observations=partial(self._extract_csv_dir, os.path.join(path, 'csv_lab'),
                                 extract.extract_csv_observation_facts),
            documentreferences=partial(self._extract_csv_dir, os.path.join(path, 'csv_note'),
                                       extract.extract_csv_observation_facts),
            patients=partial(self._extract_csv_dir, os.path.join(path, 'csv_patient'), extract.extract_csv_patients),
            encounters=partial(self._extract_csv_dir, os.path.join(path, 'csv_visit'), extract.extract_csv_visits),
        )

    ###################################################################################################################
    #
    # Oracle SQL server code
    #
    ###################################################################################################################

    def _load_all_from_oracle(self, resources: List[str]) -> tempfile.TemporaryDirectory:
        path = self.root.path
        return self._load_all_with_extractors(
            resources,
            conditions=partial(oracle_extract.list_observation_fact, path, 'Diagnosis'),
            observations=partial(oracle_extract.list_observation_fact, path, 'Lab View'),
            documentreferences=partial(oracle_extract.list_observation_fact, path, 'Notes'),
            patients=partial(oracle_extract.list_patient, path),
            encounters=partial(oracle_extract.list_visit, path),
        )
