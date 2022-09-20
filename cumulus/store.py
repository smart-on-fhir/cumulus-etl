"""Abstraction for where to write and read data"""

import abc
import logging
import os
from typing import Any, Callable, Iterable

from fhirclient.models.condition import Condition
from fhirclient.models.documentreference import DocumentReference
from fhirclient.models.encounter import Encounter
from fhirclient.models.observation import Observation
from fhirclient.models.patient import Patient

from cumulus import common
from cumulus.common import write_json, write_text  # pylint: disable=unused-import


def path_exists(path) -> bool:
    """
    Path exists (currently filesystem path).
    Could be S3 path or other persistence store.
    :param path: location of resource (directory/file)
    :return: true/false path exists
    """
    return os.path.exists(path)


def path_error(root):
    """
    :param root: errors are stored at root of store
    :return: path to errors.json
    """
    return os.path.join(root, 'errors.json')


def path_file(folder, jsonfile: str):
    """
    :param folder: directory
    :param jsonfile: file to load
    :return: path to codebook.json
    """
    if not os.path.exists(folder):
        os.makedirs(folder)

    return os.path.join(folder, jsonfile)


def path_root(root: str, folder=None):
    """
    Alias for os.path.join - useful when this moves to S3
    :param root: root directory
    :param folder: folder optional
    :return: root directory
    """
    if folder:
        return os.path.join(root, folder)
    else:
        return os.path.join(root)


def path_patient_dir(root: str, patient_id: str):
    """
    :param root: folder for patient specific results, note the "prefix" for
                 CPU/MEM optimization.
    :param patient_id: unique patient
    :return: path to patient *folder*
    """
    # practical limit of number of "files" in a folder is 10,000
    prefix = str(patient_id)
    if len(str(patient_id)) >= 4:
        prefix = prefix[0:4]

    return os.path.join(root, prefix, patient_id)


def path_note_dir(root: str, patient_id: str, md5sum: str):
    """
    :param root: directory for messages
    :param patient_id:
    :param md5sum: hash for note text
    :return: notes directory
    """
    folder = os.path.join(path_patient_dir(root, patient_id), md5sum)

    if not path_exists(folder):
        os.makedirs(folder)
    return folder


def path_ctakes(root: str, patient_id: str, md5sum: str):
    """
    :param root: directory for messages
    :param patient_id:
    :param md5sum: hash for note text
    :return: path to ctakes.json
    """
    return os.path.join(path_note_dir(root, patient_id, md5sum), 'ctakes.json')


class Store(abc.ABC):
    """An abstraction for where to place cumulus output"""

    @abc.abstractmethod
    def store_conditions(self, job, conditions: Iterable[Condition]):
        pass

    @abc.abstractmethod
    def store_docrefs(self, job, docrefs: Iterable[DocumentReference]):
        pass

    @abc.abstractmethod
    def store_encounters(self, job, encounters: Iterable[Encounter]):
        pass

    @abc.abstractmethod
    def store_labs(self, job, labs: Iterable[Observation]):
        pass

    @abc.abstractmethod
    def store_notes(self, job, docrefs: Iterable[DocumentReference]):
        pass

    @abc.abstractmethod
    def store_patients(self, job, patients: Iterable[Patient]):
        pass


class JsonTreeStore(Store):
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
        return path_patient_dir(self.dir_output, mrn)

    def _write_patient(self, patient):
        mrn = patient.id
        path = path_file(self._dir_output_patient(mrn), 'fhir_patient.json')
        write_json(path, patient.as_json())
        return path

    def store_patients(self, job, patients: Iterable[Patient]):
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
        path = path_file(self._dir_output_encounter(mrn, enc),
                         'fhir_encounter.json')
        write_json(path, encounter.as_json())
        return path

    def store_encounters(self, job, encounters: Iterable[Encounter]):
        self._write_records(job, encounters, self._write_encounter)

    ###########################################################################
    #
    # Labs
    #
    ###########################################################################

    def _write_lab(self, lab):
        mrn = lab.subject.reference
        enc = lab.context.reference
        path = path_file(self._dir_output_encounter(mrn, enc),
                         f'fhir_lab_{lab.id}.json')
        write_json(path, lab.as_json())
        return path

    def store_labs(self, job, labs: Iterable[Observation]):
        self._write_records(job, labs, self._write_lab)

    ###########################################################################
    #
    # Conditions
    #
    ###########################################################################

    def _write_condition(self, condition):
        mrn = condition.subject.reference
        enc = condition.context.reference
        path = path_file(self._dir_output_encounter(mrn, enc),
                         f'fhir_condition_{condition.id}.json')
        write_json(path, condition.as_json())
        return path

    def store_conditions(self, job, conditions: Iterable[Condition]):
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
        path = path_file(self._dir_output_encounter(mrn, enc),
                         f'fhir_docref_{docref.id}.json')
        write_json(path, docref.as_json())
        return path

    def store_docrefs(self, job, docrefs: Iterable[DocumentReference]):
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

    def store_notes(self, job, docrefs: Iterable[DocumentReference]):
        self._write_records(job, docrefs, self._write_note)
