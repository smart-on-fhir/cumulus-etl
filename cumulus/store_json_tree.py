"""An implementation of Store that writes to a directory tree"""

import logging
import os
from typing import Any, Callable, Iterable

from fhirclient.models.condition import Condition
from fhirclient.models.documentreference import DocumentReference
from fhirclient.models.encounter import Encounter
from fhirclient.models.observation import Observation
from fhirclient.models.patient import Patient

from cumulus import common, store


class JsonTreeStore(store.Store):
    """Stores output files in a tree of json data"""

    def __init__(self, path: str):
        super().__init__()
        self.dir_output = path

    def _write_records(self,
                       job,
                       records: Iterable[Any],
                       processor: Callable[[Any], str]):
        for record in records:
            try:
                job.attempt.append(record)

                success_note = processor(record)

                job.success.append(success_note)
                job.success_rate()
            except Exception:  # pylint: disable=broad-except
                logging.exception('Could not process data record')
                job.failed.append(record.as_json())

    ###########################################################################
    #
    # Patients
    #
    ###########################################################################

    def _dir_output_patient(self, mrn: str) -> str:
        return store.path_patient_dir(self.dir_output, mrn)

    def _write_patient(self, patient):
        mrn = patient.id
        path = store.path_file(self._dir_output_patient(mrn),
                               'fhir_patient.json')
        common.write_json(path, patient.as_json())
        return path

    def store_patients(self, job, patients: Iterable[Patient]) -> None:
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

    def _write_encounter(self, encounter):
        mrn = encounter.subject.reference
        enc = encounter.id
        path = store.path_file(self._dir_output_encounter(mrn, enc),
                               'fhir_encounter.json')
        common.write_json(path, encounter.as_json())
        return path

    def store_encounters(self, job, encounters: Iterable[Encounter]) -> None:
        self._write_records(job, encounters, self._write_encounter)

    ###########################################################################
    #
    # Labs
    #
    ###########################################################################

    def _write_lab(self, lab):
        mrn = lab.subject.reference
        enc = lab.encounter.reference
        path = store.path_file(self._dir_output_encounter(mrn, enc),
                               f'fhir_lab_{lab.id}.json')
        common.write_json(path, lab.as_json())
        return path

    def store_labs(self, job, labs: Iterable[Observation]) -> None:
        self._write_records(job, labs, self._write_lab)

    ###########################################################################
    #
    # Conditions
    #
    ###########################################################################

    def _write_condition(self, condition):
        mrn = condition.subject.reference
        enc = condition.encounter.reference
        path = store.path_file(self._dir_output_encounter(mrn, enc),
                               f'fhir_condition_{condition.id}.json')
        common.write_json(path, condition.as_json())
        return path

    def store_conditions(self, job, conditions: Iterable[Condition]) -> None:
        self._write_records(job, conditions, self._write_condition)

    ###########################################################################
    #
    # Document References
    #
    ###########################################################################

    def _write_docref(self, docref):
        # TODO: confirm what we should do with multiple/zero encounters
        # (not a problem yet, as i2b2 only ever gives us one)
        if len(docref.context.encounter) != 1:
            raise ValueError('Cumulus only supports single-encounter '
                             'notes right now')

        mrn = docref.subject.reference
        enc = docref.context.encounter[0].reference
        path = store.path_file(self._dir_output_encounter(mrn, enc),
                               f'fhir_docref_{docref.id}.json')
        common.write_json(path, docref.as_json())
        return path

    def store_docrefs(self, job, docrefs: Iterable[DocumentReference]) -> None:
        self._write_records(job, docrefs, self._write_docref)

    def _write_note(self, docref):
        if len(docref.content) != 1:
            raise ValueError('Cumulus only supports single-content '
                             'notes right now')

        # TODO: confirm what we should do with multiple/zero encounters
        # (not a problem yet, as i2b2 only ever gives us one)
        if len(docref.context.encounter) != 1:
            raise ValueError('Cumulus only supports single-encounter '
                             'notes right now')

        note_text = docref.content[0].attachment.data
        md5sum = common.hash_clinical_text(note_text)

        mrn = docref.subject.reference
        enc = docref.context.encounter[0].reference

        folder = self._dir_output_encounter(mrn, enc)
        os.makedirs(folder, exist_ok=True)

        path_text = os.path.join(folder, f'physician_note_{md5sum}.txt')
        path_ctakes_json = os.path.join(folder, f'ctakes_{md5sum}.json')

        if len(note_text) > 10:
            if not os.path.exists(path_text):
                common.write_text(path_text, note_text)
            if not os.path.exists(path_ctakes_json):
                logging.warning('cTAKES response not found')

        return path_text

    def store_notes(self, job, docrefs: Iterable[DocumentReference]) -> None:
        self._write_records(job, docrefs, self._write_note)
