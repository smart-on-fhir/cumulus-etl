"""An implementation of Store that writes to a directory tree"""

import logging
import os
import pandas
from typing import Callable

from cumulus import common, store


class JsonTreeStore(store.Store):
    """Stores output files in a tree of json data"""

    def __init__(self, path: str):
        super().__init__()
        self.dir_output = path

    def _write_records(self,
                       job,
                       df: pandas.DataFrame,
                       processor: Callable[[pandas.Series], None]):
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
        return store.path_patient_dir(self.dir_output, mrn)

    def _write_patient(self, patient: pandas.Series) -> None:
        mrn = patient.id
        path = store.path_file(self._dir_output_patient(mrn),
                               'fhir_patient.json')
        patient.to_json(path)

    def store_patients(self, job, patients: pandas.DataFrame) -> None:
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
        return os.path.join(self._dir_output_patient(mrn), encounter_id)

    def _write_encounter(self, encounter: pandas.Series) -> None:
        mrn = encounter.subject['reference']
        enc = encounter.id
        path = store.path_file(self._dir_output_encounter(mrn, enc),
                               'fhir_encounter.json')
        encounter.to_json(path)

    def store_encounters(self, job, encounters: pandas.DataFrame) -> None:
        self._write_records(job, encounters, self._write_encounter)

    ###########################################################################
    #
    # Labs
    #
    ###########################################################################

    def _write_lab(self, lab: pandas.Series) -> None:
        mrn = lab.subject['reference']
        enc = lab.encounter['reference']
        path = store.path_file(self._dir_output_encounter(mrn, enc),
                               f'fhir_lab_{lab.id}.json')
        lab.to_json(path)

    def store_labs(self, job, labs: pandas.DataFrame) -> None:
        self._write_records(job, labs, self._write_lab)

    ###########################################################################
    #
    # Conditions
    #
    ###########################################################################

    def _write_condition(self, condition: pandas.Series) -> None:
        mrn = condition.subject['reference']
        enc = condition.encounter['reference']
        path = store.path_file(self._dir_output_encounter(mrn, enc),
                               f'fhir_condition_{condition.id}.json')
        condition.to_json(path)

    def store_conditions(self, job, conditions: pandas.DataFrame) -> None:
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
        path = store.path_file(self._dir_output_encounter(mrn, enc),
                               f'fhir_docref_{docref.id}.json')
        docref.to_json(path)

    def store_docrefs(self, job, docrefs: pandas.DataFrame) -> None:
        self._write_records(job, docrefs, self._write_docref)

    def _write_note(self, docref: pandas.Series) -> None:
        if len(docref.content) != 1:
            raise ValueError('Cumulus only supports single-content '
                             'notes right now')

        # TODO: confirm what we should do with multiple/zero encounters
        # (not a problem yet, as i2b2 only ever gives us one)
        if len(docref.context['encounter']) != 1:
            raise ValueError('Cumulus only supports single-encounter '
                             'notes right now')

        note_text = docref.content[0]['attachment']['data']
        md5sum = common.hash_clinical_text(note_text)

        mrn = docref.subject['reference']
        enc = docref.context['encounter'][0]['reference']

        folder = self._dir_output_encounter(mrn, enc)
        os.makedirs(folder, exist_ok=True)

        path_text = os.path.join(folder, f'physician_note_{md5sum}.txt')
        path_ctakes_json = os.path.join(folder, f'ctakes_{md5sum}.json')

        if len(note_text) > 10:
            if not os.path.exists(path_text):
                common.write_text(path_text, note_text)
            if not os.path.exists(path_ctakes_json):
                logging.warning('cTAKES response not found')

    def store_notes(self, job, docrefs: pandas.DataFrame) -> None:
        self._write_records(job, docrefs, self._write_note)
