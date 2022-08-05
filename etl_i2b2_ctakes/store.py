import os
import json
import logging

import ctakes
import i2b2
import deid
import codebook

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

def path_codebook(root):
    """
    :param root: codebook is stored at root of store, applies to all patients.
    :return: path to codebook.json
    """
    return os.path.join(root, 'codebook.json')

def path_patient_dir(root:str, observation: i2b2.ObservationFact):
    """
    :param root: folder for patient specific results, note the "prefix" for CPU/MEM optimization.
    :param observation: patient data
    :return: path to patient *folder*
    """
    # practical limit of number of "files" in a folder is 10,000
    prefix = str(observation.patient_num)
    if len(str(observation.patient_num)) >= 4:
        prefix = prefix[0:4]

    return os.path.join(root, prefix, observation.patient_num)

def path_note_dir(root: str, observation: i2b2.ObservationFact):
    """
    :param root: directory for messages
    :param observation: patient note with encounter dates
    :return: path to ctakes.json
    """
    md5sum = deid.hash_clinical_text(observation.observation_blob)
    folder = os.path.join(path_patient_dir(root, observation), md5sum)

    if not path_exists(folder): os.makedirs(folder)
    return folder

def path_ctakes(root: str, observation: i2b2.ObservationFact):
    """
    :param root: directory for messages
    :param observation: patient note with encounter dates
    :return: path to ctakes.json
    """
    return os.path.join(path_note_dir(root, observation), 'ctakes.json')

def path_philter(root: str, observation: i2b2.ObservationFact):
    """
    :param root:
    :param observation:
    :return: path to philter.json
    """
    return os.path.join(path_note_dir(root, observation), 'philter.json')

def path_bsv_semtype(root: str, observation: i2b2.ObservationFact, semtype=ctakes.SemType.SignSymptom):
    """
    :param root:
    :param observation:
    :return: path to philter.json
    """
    return os.path.join(path_note_dir(root, observation), f'{semtype.name}.bsv')

def write(path:str, message:dict) -> str:
    """
    :param path: topic (currently filesystem path)
    :param message: coded message
    :return: path to message
    """
    logging.debug(f'write() {path}')

    with open(path, 'w') as f:
        f.write(json.dumps(message, indent=4))

    return path

def read(path:str) -> dict:
    """
    :param path: (currently filesystem path)
    :return: message: coded message
    """
    logging.debug(f'read() {path}')

    with open(path, 'r') as f:
        message = json.load(f)
    f.close()
    return message

