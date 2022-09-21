"""Abstraction for where to write and read data"""

import abc
import os
from typing import Iterable

from fhirclient.models.condition import Condition
from fhirclient.models.documentreference import DocumentReference
from fhirclient.models.encounter import Encounter
from fhirclient.models.observation import Observation
from fhirclient.models.patient import Patient

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
    """
    An abstraction for where to place cumulus output

    Subclass this to provide a different output format (like ndjson or parquet).
    """

    @abc.abstractmethod
    def store_conditions(self, job, conditions: Iterable[Condition]) -> None:
        pass

    @abc.abstractmethod
    def store_docrefs(self, job, docrefs: Iterable[DocumentReference]) -> None:
        pass

    @abc.abstractmethod
    def store_encounters(self, job, encounters: Iterable[Encounter]) -> None:
        pass

    @abc.abstractmethod
    def store_labs(self, job, labs: Iterable[Observation]) -> None:
        pass

    @abc.abstractmethod
    def store_notes(self, job, docrefs: Iterable[DocumentReference]) -> None:
        pass

    @abc.abstractmethod
    def store_patients(self, job, patients: Iterable[Patient]) -> None:
        pass
