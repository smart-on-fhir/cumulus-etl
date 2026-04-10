"""I2B2 loader"""

from collections.abc import Callable, Iterable
from functools import partial
from typing import TypeVar

import cumulus_fhir_support as cfs

from cumulus_etl import cli_utils, common, feedback
from cumulus_etl.loaders import base
from cumulus_etl.loaders.i2b2 import extract, schema, transform

AnyDimension = TypeVar("AnyDimension", bound=schema.Dimension)
I2b2ExtractorCallable = Callable[[], Iterable[schema.Dimension]]
CsvToI2b2Callable = Callable[[str], Iterable[schema.Dimension]]
I2b2ToFhirCallable = Callable[[AnyDimension], dict]


class I2b2Loader(base.Loader):
    """
    Loader for i2b2 data.
    """

    async def detect_resources(self, *, progress: feedback.Progress) -> set[str] | None:
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
            resource for path, resource in filenames.items() if self.root.joinpath(path).exists()
        }

    async def load_resources(
        self, resources: set[str], *, progress: feedback.Progress
    ) -> base.LoaderResults:
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
        tmpdir = cli_utils.make_export_dir()
        tmp_path = cfs.FsPath(tmpdir.name)

        if "Condition" in resources:
            code_dict = cfs.FsPath(__file__).parent.joinpath("icd.json").read_json()
            self._loop(
                conditions(),
                partial(transform.to_fhir_condition, display_codes=code_dict),
                tmp_path.joinpath("Condition.ndjson"),
            )

        if "MedicationRequest" in resources:
            self._loop(
                medicationrequests(),
                transform.to_fhir_medicationrequest,
                tmp_path.joinpath("MedicationRequest.ndjson"),
            )

        if "Observation" in resources:
            self._loop(
                lab_views(),
                transform.to_fhir_observation_lab,
                tmp_path.joinpath("Observation.0.ndjson"),
            )
            self._loop(
                vitals(),
                transform.to_fhir_observation_vitals,
                tmp_path.joinpath("Observation.1.ndjson"),
            )

        if "DocumentReference" in resources:
            self._loop(
                documentreferences(),
                transform.to_fhir_documentreference,
                tmp_path.joinpath("DocumentReference.ndjson"),
            )

        if "Patient" in resources:
            self._loop(
                patients(),
                transform.to_fhir_patient,
                tmp_path.joinpath("Patient.ndjson"),
            )

        if "Encounter" in resources:
            self._loop(
                encounters(),
                transform.to_fhir_encounter,
                tmp_path.joinpath("Encounter.ndjson"),
            )

        return tmpdir

    def _loop(
        self,
        i2b2_entries: Iterable[schema.Dimension],
        to_fhir: I2b2ToFhirCallable,
        output_path: cfs.FsPath,
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
        return self._load_all_with_extractors(
            resources,
            conditions=partial(
                extract.extract_csv_observation_facts,
                self.root.joinpath("observation_fact_diagnosis.csv"),
            ),
            lab_views=partial(
                extract.extract_csv_observation_facts,
                self.root.joinpath("observation_fact_lab_views.csv"),
            ),
            medicationrequests=partial(
                extract.extract_csv_observation_facts,
                self.root.joinpath("observation_fact_medications.csv"),
            ),
            vitals=partial(
                extract.extract_csv_observation_facts,
                self.root.joinpath("observation_fact_vitals.csv"),
            ),
            documentreferences=partial(
                extract.extract_csv_observation_facts,
                self.root.joinpath("observation_fact_notes.csv"),
            ),
            patients=partial(
                extract.extract_csv_patients, self.root.joinpath("patient_dimension.csv")
            ),
            encounters=partial(
                extract.extract_csv_visits, self.root.joinpath("visit_dimension.csv")
            ),
        )
