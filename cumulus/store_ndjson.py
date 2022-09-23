"""An implementation of Store that writes to a few flat ndjson files"""

import logging

import pandas

from cumulus import store


class NdjsonStore(store.Store):
    """Stores output files in a few flat ndjson files"""

    def __init__(self, path: str):
        super().__init__()
        self.dir_output = path

    def _write_records(self, job, df: pandas.DataFrame, path: str) -> None:
        job.attempt += len(df)

        try:
            full_path = store.path_file(self.dir_output, path)
            df.to_json(full_path, orient='records', lines=True)

            job.success += len(df)
            job.success_rate(1)
        except Exception:  # pylint: disable=broad-except
            logging.exception('Could not process data records')

    def store_patients(self, job, patients: pandas.DataFrame) -> None:
        self._write_records(job, patients, 'fhir_patients.ndjson')

    def store_encounters(self, job, encounters: pandas.DataFrame) -> None:
        self._write_records(job, encounters, 'fhir_encounters.ndjson')

    def store_labs(self, job, labs: pandas.DataFrame) -> None:
        self._write_records(job, labs, 'fhir_labs.ndjson')

    def store_conditions(self, job, conditions: pandas.DataFrame) -> None:
        self._write_records(job, conditions, 'fhir_conditions.ndjson')

    def store_docrefs(self, job, docrefs: pandas.DataFrame) -> None:
        self._write_records(job, docrefs, 'fhir_documentreferences.ndjson')

    def store_notes(self, job, docrefs: pandas.DataFrame) -> None:
        self._write_records(job, docrefs, 'fhir_notes.ndjson')
