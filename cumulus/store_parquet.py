"""An implementation of Store that writes to a few flat parquet files"""

import logging

import pandas

from cumulus import store


class ParquetStore(store.Store):
    """Stores output files in a few flat parquet files"""

    def __init__(self, path: str):
        super().__init__()
        self.dir_output = path

    def _write_records(self, job, df: pandas.DataFrame, path: str) -> None:
        job.attempt += len(df)

        try:
            full_path = store.path_file(self.dir_output, path)
            df.to_parquet(full_path)

            job.success += len(df)
            job.success_rate(1)
        except Exception:  # pylint: disable=broad-except
            logging.exception('Could not process data records')

    def store_patients(self, job, patients: pandas.DataFrame) -> None:
        self._write_records(job, patients, 'fhir_patients.parquet')

    def store_encounters(self, job, encounters: pandas.DataFrame) -> None:
        self._write_records(job, encounters, 'fhir_encounters.parquet')

    def store_labs(self, job, labs: pandas.DataFrame) -> None:
        self._write_records(job, labs, 'fhir_labs.parquet')

    def store_conditions(self, job, conditions: pandas.DataFrame) -> None:
        self._write_records(job, conditions, 'fhir_conditions.parquet')

    def store_docrefs(self, job, docrefs: pandas.DataFrame) -> None:
        self._write_records(job, docrefs, 'fhir_documentreferences.parquet')

    def store_notes(self, job, docrefs: pandas.DataFrame) -> None:
        self._write_records(job, docrefs, 'fhir_notes.parquet')
