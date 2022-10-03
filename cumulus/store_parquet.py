"""An implementation of Store that writes to a few flat parquet files"""

import logging
import os

import pandas

from cumulus import store


class ParquetFormat(store.Format):
    """Stores output files in a few flat parquet files"""

    def _write_records(self, job, df: pandas.DataFrame, path: str) -> None:
        job.attempt += len(df)

        try:
            full_path = self.root.joinpath(path)
            self.root.makedirs(os.path.dirname(full_path))
            df.to_parquet(full_path, index=False,
                          storage_options=self.root.fsspec_options())

            job.success += len(df)
            job.success_rate(1)
        except Exception:  # pylint: disable=broad-except
            logging.exception('Could not process data records')

    def store_patients(self, job, patients: pandas.DataFrame) -> None:
        self._write_records(job, patients, 'patient/fhir_patients.parquet')

    def store_encounters(self, job, encounters: pandas.DataFrame) -> None:
        self._write_records(job, encounters, 'encounter/fhir_encounters.parquet')

    def store_labs(self, job, labs: pandas.DataFrame) -> None:
        self._write_records(job, labs, 'observation/fhir_observations.parquet')

    def store_conditions(self, job, conditions: pandas.DataFrame) -> None:
        self._write_records(job, conditions, 'condition/fhir_conditions.parquet')

    def store_docrefs(self, job, docrefs: pandas.DataFrame) -> None:
        self._write_records(job, docrefs, 'documentreference/fhir_documentreferences.parquet')

    def store_observation_list(self, job, observations: pandas.DataFrame) -> None:
        pass
