"""An implementation of Format that writes to a directory tree"""

import logging
import os
import pandas
from typing import Callable

from cumulus import store


class JsonTreeFormat(store.Format):
    """Stores output files in a tree of json data"""

    def _write_records(self,
                       job,
                       df: pandas.DataFrame,
                       processor: Callable[[pandas.Series], None]):
        """Writes each row of the dataframe to its own custom file"""
        for _, record in df.iterrows():
            try:
                job.attempt += 1

                processor(record)

                job.success += 1
                job.success_rate()
            except Exception:  # pylint: disable=broad-except
                logging.exception('Could not process data record')
                job.failed.append(record.to_json())

    ###########################################################################
    #
    # Patients
    #
    ###########################################################################

    def _dir_output_patient(self, mrn: str) -> str:
        """
        :param mrn: unique patient
        :return: path to patient *folder*
        """
        mrn = mrn.split('/')[-1]

        # practical limit of number of "files" in a folder is 10,000
        prefix = mrn[0:4]

        path = self.root.joinpath(prefix, mrn)
        self.root.makedirs(path)
        return path

    def _write_patient(self, patient: pandas.Series) -> None:
        mrn = patient.id
        path = os.path.join(self._dir_output_patient(mrn),
                            'fhir_patient.json')
        patient.to_json(path, storage_options=self.root.fsspec_options())

    def store_patients(self, job, patients: pandas.DataFrame, batch: int) -> None:
        self._write_records(job, patients, self._write_patient)

    ###########################################################################
    #
    # Encounters
    #
    ###########################################################################

    def _dir_output_encounter(self, mrn: str, encounter_id: str) -> str:
        """
        :param mrn: unique patient
        :param encounter_id: unique encounter
        :return: path to encounter *folder*
        """
        encounter_id = encounter_id.split('/')[-1]
        path = os.path.join(self._dir_output_patient(mrn), encounter_id)
        self.root.makedirs(path)
        return path

    def _write_encounter(self, encounter: pandas.Series) -> None:
        mrn = encounter.subject['reference']
        enc = encounter.id
        path = os.path.join(self._dir_output_encounter(mrn, enc),
                            'fhir_encounter.json')
        encounter.to_json(path, storage_options=self.root.fsspec_options())

    def store_encounters(self, job, encounters: pandas.DataFrame, batch: int) -> None:
        self._write_records(job, encounters, self._write_encounter)

    ###########################################################################
    #
    # Labs
    #
    ###########################################################################

    def _write_lab(self, lab: pandas.Series) -> None:
        mrn = lab.subject['reference']
        enc = lab.encounter['reference']
        path = os.path.join(self._dir_output_encounter(mrn, enc),
                            f'fhir_lab_{lab.id}.json')
        lab.to_json(path, storage_options=self.root.fsspec_options())

    def store_labs(self, job, labs: pandas.DataFrame, batch: int) -> None:
        self._write_records(job, labs, self._write_lab)

    ###########################################################################
    #
    # Conditions
    #
    ###########################################################################

    def _write_condition(self, condition: pandas.Series) -> None:
        mrn = condition.subject['reference']
        enc = condition.encounter['reference']
        path = os.path.join(self._dir_output_encounter(mrn, enc),
                            f'fhir_condition_{condition.id}.json')
        condition.to_json(path, storage_options=self.root.fsspec_options())

    def store_conditions(self, job, conditions: pandas.DataFrame, batch: int) -> None:
        self._write_records(job, conditions, self._write_condition)

    ###########################################################################
    #
    # Document References
    #
    ###########################################################################

    def _write_docref(self, docref: pandas.Series) -> None:
        # TODO: confirm what we should do with multiple/zero encounters
        # (not a problem yet, as i2b2 only ever gives us one)
        if len(docref.context['encounter']) != 1:
            raise ValueError('Cumulus only supports single-encounter '
                             'notes right now')

        mrn = docref.subject['reference']
        enc = docref.context['encounter'][0]['reference']
        path = os.path.join(self._dir_output_encounter(mrn, enc),
                            f'fhir_docref_{docref.id}.json')
        docref.to_json(path, storage_options=self.root.fsspec_options())

    def store_docrefs(self, job, docrefs: pandas.DataFrame, batch: int) -> None:
        self._write_records(job, docrefs, self._write_docref)

    ###########################################################################
    #
    # Symptoms
    #
    ###########################################################################

    def _write_symptom(self, observation: pandas.Series) -> None:
        mrn = observation.subject['reference']
        enc = observation.encounter['reference']
        path = os.path.join(self._dir_output_encounter(mrn, enc), f'fhir_symptom_{observation.id}.json')
        observation.to_json(path, storage_options=self.root.fsspec_options())

    def store_symptoms(self, job, observations: pandas.DataFrame, batch: int) -> None:
        self._write_records(job, observations, self._write_symptom)
