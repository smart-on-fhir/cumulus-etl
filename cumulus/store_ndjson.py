"""An implementation of Store that writes to a few flat ndjson files"""

import json
import logging
from typing import Any, Iterable

from fhirclient.models.condition import Condition
from fhirclient.models.documentreference import DocumentReference
from fhirclient.models.encounter import Encounter
from fhirclient.models.observation import Observation
from fhirclient.models.patient import Patient

from cumulus import store


class NdjsonStore(store.Store):
    """Stores output files in a few flat ndjson files"""

    def __init__(self, path: str):
        super().__init__()
        self.dir_output = path

    def _write_records(self, job, records: Iterable[Any], path: str) -> None:
        full_path = store.path_file(self.dir_output, path)
        with open(full_path, 'w', encoding='utf8') as f:
            for record in records:
                try:
                    job.attempt.append(record)

                    json.dump(record.as_json(), f)
                    f.write('\n')

                    job.success.append(record.id)
                    job.success_rate()
                except Exception:  # pylint: disable=broad-except
                    logging.exception('Could not process data record')
                    job.failed.append(record.as_json())

    def store_patients(self, job, patients: Iterable[Patient]) -> None:
        self._write_records(job, patients, 'fhir_patients.ndjson')

    def store_encounters(self, job, encounters: Iterable[Encounter]) -> None:
        self._write_records(job, encounters, 'fhir_encounters.ndjson')

    def store_labs(self, job, labs: Iterable[Observation]) -> None:
        self._write_records(job, labs, 'fhir_labs.ndjson')

    def store_conditions(self, job, conditions: Iterable[Condition]) -> None:
        self._write_records(job, conditions, 'fhir_conditions.ndjson')

    def store_docrefs(self, job, docrefs: Iterable[DocumentReference]) -> None:
        self._write_records(job, docrefs, 'fhir_documentreferences.ndjson')

    def store_notes(self, job, docrefs: Iterable[DocumentReference]) -> None:
        self._write_records(job, docrefs, 'fhir_notes.ndjson')
