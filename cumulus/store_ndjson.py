"""An implementation of Store that writes to a few flat ndjson files"""

import logging
import os

import pandas

from cumulus import store


class NdjsonFormat(store.Format):
    """Stores output files in a few flat ndjson files"""

    def _write_records(self, job, df: pandas.DataFrame, path: str) -> None:
        """Writes the whole dataframe to a single ndjson file"""
        job.attempt += len(df)

        try:
            full_path = self.root.joinpath(path)
            self.root.makedirs(os.path.dirname(full_path))
            df.to_json(full_path, orient='records', lines=True,
                       storage_options=self.root.fsspec_options())

            job.success += len(df)
            job.success_rate(1)
        except Exception:  # pylint: disable=broad-except
            logging.exception('Could not process data records')

    def store_patients(self, job, patients: pandas.DataFrame) -> None:
        self._write_records(job, patients, 'patient/fhir_patients.ndjson')

    def store_encounters(self, job, encounters: pandas.DataFrame) -> None:
        self._write_records(job, encounters, 'encounter/fhir_encounters.ndjson')

    def store_labs(self, job, labs: pandas.DataFrame) -> None:
        self._write_records(job, labs, 'observation/fhir_observations.ndjson')

    def store_conditions(self, job, conditions: pandas.DataFrame) -> None:
        self._write_records(job, conditions, 'condition/fhir_conditions.ndjson')

    def store_docrefs(self, job, docrefs: pandas.DataFrame) -> None:
        self._write_records(job, docrefs, 'documentreference/fhir_documentreferences.ndjson')

    def store_observation_list(self, job, observations: pandas.DataFrame) -> None:
        pass
