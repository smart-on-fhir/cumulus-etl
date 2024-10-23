"""I2B2 loader"""

import json
import os
from collections.abc import Callable, Iterable
from functools import partial
from pathlib import Path
from typing import TypeVar

from cumulus_etl import cli_utils, common, store
from cumulus_etl.loaders import base
from cumulus_etl.loaders.i2b2 import extract, schema, transform
from cumulus_etl.loaders.i2b2.oracle import extract as oracle_extract

AnyDimension = TypeVar("AnyDimension", bound=schema.Dimension)
I2b2ExtractorCallable = Callable[[], Iterable[schema.Dimension]]
CsvToI2b2Callable = Callable[[str], Iterable[schema.Dimension]]
I2b2ToFhirCallable = Callable[[AnyDimension], dict]


class I2b2Loader(base.Loader):
    """
    Loader for i2b2 data.

    Expected format is either a tcp:// URL pointing at an Oracle server or a local folder.
    """

    def __init__(self, root: store.Root, export_to: str | None = None):
        """
        Initialize a new I2b2Loader class
        :param root: the base location to read data from
        :param export_to: folder to save the ndjson results of converting i2b2
        """
        super().__init__(root)
        self.export_to = export_to

    async def detect_resources(self) -> set[str] | None:
        if self.root.protocol in {"tcp"}:
            # We haven't done the export yet, so there are no files to inspect yet.
            # Returning None means "dunno" (i.e. "just accept whatever you eventually get").
            return None

        filenames = {
            "observation_fact_diagnosis.csv": "Condition",
            "observation_fact_lab_views.csv": "Observation",
            "observation_fact_medications.csv": "MedicationRequest",
            "observation_fact_notes.csv": "DocumentReference",
            "observation_fact_vitals.csv": "Observation",
            "patient_dimension.csv": "Patient",
            "visit_dimension.csv": "Encounter",
        }

        return {
            resource
            for path, resource in filenames.items()
            if self.root.exists(self.root.joinpath(path))
        }

    async def load_resources(self, resources: set[str]) -> base.LoaderResults:
        if self.root.protocol in ["tcp"]:
            directory = self._load_all_from_oracle(resources)
        else:
            directory = self._load_all_from_csv(resources)
        return base.LoaderResults(directory=directory)

    def _load_all_with_extractors(
        self,
        resources: set[str],
        conditions: I2b2ExtractorCallable,
        lab_views: I2b2ExtractorCallable,
        medicationrequests: I2b2ExtractorCallable,
        vitals: I2b2ExtractorCallable,
        documentreferences: I2b2ExtractorCallable,
        patients: I2b2ExtractorCallable,
        encounters: I2b2ExtractorCallable,
    ) -> common.Directory:
        """
        Load i2b2 content into a local folder as ndjson

        Argument names are short to encourage treating them as kwargs for easier readability.
        """
        tmpdir = cli_utils.make_export_dir(self.export_to)

        if "Condition" in resources:
            with open(
                Path(Path(__file__).resolve().parent, "icd.json"), encoding="utf-8"
            ) as code_json:
                code_dict = json.load(code_json)
            self._loop(
                conditions(),
                partial(transform.to_fhir_condition, display_codes=code_dict),
                os.path.join(tmpdir.name, "Condition.ndjson"),
            )

        if "MedicationRequest" in resources:
            self._loop(
                medicationrequests(),
                transform.to_fhir_medicationrequest,
                os.path.join(tmpdir.name, "MedicationRequest.ndjson"),
            )

        if "Observation" in resources:
            self._loop(
                lab_views(),
                transform.to_fhir_observation_lab,
                os.path.join(tmpdir.name, "Observation.0.ndjson"),
            )
            self._loop(
                vitals(),
                transform.to_fhir_observation_vitals,
                os.path.join(tmpdir.name, "Observation.1.ndjson"),
            )

        if "DocumentReference" in resources:
            self._loop(
                documentreferences(),
                transform.to_fhir_documentreference,
                os.path.join(tmpdir.name, "DocumentReference.ndjson"),
            )

        if "Patient" in resources:
            self._loop(
                patients(),
                transform.to_fhir_patient,
                os.path.join(tmpdir.name, "Patient.ndjson"),
            )

        if "Encounter" in resources:
            self._loop(
                encounters(),
                transform.to_fhir_encounter,
                os.path.join(tmpdir.name, "Encounter.ndjson"),
            )

        return tmpdir

    def _loop(
        self,
        i2b2_entries: Iterable[schema.Dimension],
        to_fhir: I2b2ToFhirCallable,
        output_path: str,
    ) -> None:
        """Takes one kind of i2b2 resource, loads them all up, and writes out a FHIR ndjson file"""
        fhir_resources = (to_fhir(x) for x in i2b2_entries)

        # keep track of every ID we've seen so far, because sometimes i2b2 can have duplicates
        ids = set()

        with common.NdjsonWriter(output_path) as output_file:
            # Now write each FHIR resource line by line to the output
            # (we do this all line by line via generators to avoid loading everything in memory at once)
            for resource in fhir_resources:
                if resource["id"] in ids:
                    continue
                ids.add(resource["id"])
                output_file.write(resource)

    ###################################################################################################################
    #
    # CSV code
    #
    ###################################################################################################################

    def _load_all_from_csv(self, resources: set[str]) -> common.Directory:
        path = self.root.path
        return self._load_all_with_extractors(
            resources,
            conditions=partial(
                extract.extract_csv_observation_facts,
                os.path.join(path, "observation_fact_diagnosis.csv"),
            ),
            lab_views=partial(
                extract.extract_csv_observation_facts,
                os.path.join(path, "observation_fact_lab_views.csv"),
            ),
            medicationrequests=partial(
                extract.extract_csv_observation_facts,
                os.path.join(path, "observation_fact_medications.csv"),
            ),
            vitals=partial(
                extract.extract_csv_observation_facts,
                os.path.join(path, "observation_fact_vitals.csv"),
            ),
            documentreferences=partial(
                extract.extract_csv_observation_facts,
                os.path.join(path, "observation_fact_notes.csv"),
            ),
            patients=partial(
                extract.extract_csv_patients, os.path.join(path, "patient_dimension.csv")
            ),
            encounters=partial(
                extract.extract_csv_visits, os.path.join(path, "visit_dimension.csv")
            ),
        )

    ###################################################################################################################
    #
    # Oracle SQL server code
    #
    ###################################################################################################################

    def _load_all_from_oracle(self, resources: set[str]) -> common.Directory:
        path = self.root.path
        return self._load_all_with_extractors(
            resources,
            conditions=partial(oracle_extract.list_observation_fact, path, ["ICD9", "ICD10"]),
            lab_views=partial(oracle_extract.list_observation_fact, path, ["LAB"]),
            medicationrequests=partial(
                oracle_extract.list_observation_fact, path, ["ADMINMED", "HOMEMED"]
            ),
            vitals=partial(oracle_extract.list_observation_fact, path, ["VITAL"]),
            documentreferences=partial(oracle_extract.list_observation_fact, path, ["NOTE"]),
            patients=partial(oracle_extract.list_patient, path),
            encounters=partial(oracle_extract.list_visit, path),
        )
