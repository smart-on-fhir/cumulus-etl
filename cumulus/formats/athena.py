"""An implementation of Format designed to write to AWS Athena"""

import abc
import logging
import os

import pandas

from cumulus import store


class AthenaFormat(store.Format):
    """
    Stores output files in a tree of files designed for easy consumption by AWS Athena

    Still useful in other contexts too (i.e. it's totally reasonable to write to disk instead of S3).
    But the particulars of why we organize files are designed for Athena.
    """

    @property
    @abc.abstractmethod
    def suffix(self) -> str:
        """
        The suffix to use for any files written out

        Honestly, not super necessary, since S3 filetypes are independent of suffix. But useful if writing locally.
        """

    @abc.abstractmethod
    def write_format(self, df: pandas.DataFrame, path: str) -> None:
        """
        Write the data in `df` to the target path file
        """

    ##########################################################################################
    #
    # Implementation details below
    #
    ##########################################################################################

    def _write_records(self, job, df: pandas.DataFrame, path: str) -> None:
        """Writes the whole dataframe to a single file"""
        job.attempt += len(df)

        try:
            full_path = self.root.joinpath(f'{path}.{self.suffix}')
            self.root.makedirs(os.path.dirname(full_path))
            self.write_format(df, full_path)

            job.success += len(df)
            job.success_rate(1)
        except Exception:  # pylint: disable=broad-except
            logging.exception('Could not process data records')

    def store_patients(self, job, patients: pandas.DataFrame) -> None:
        self._write_records(job, patients, 'patient/fhir_patients')

    def store_encounters(self, job, encounters: pandas.DataFrame) -> None:
        self._write_records(job, encounters, 'encounter/fhir_encounters')

    def store_labs(self, job, labs: pandas.DataFrame) -> None:
        self._write_records(job, labs, 'observation/fhir_observations')

    def store_conditions(self, job, conditions: pandas.DataFrame) -> None:
        self._write_records(job, conditions, 'condition/fhir_conditions')

    def store_docrefs(self, job, docrefs: pandas.DataFrame) -> None:
        self._write_records(job, docrefs, 'documentreference/fhir_documentreferences')

    def store_symptoms(self, job, observations: pandas.DataFrame) -> None:
        self._write_records(job, observations, 'symptom/fhir_symptoms')
