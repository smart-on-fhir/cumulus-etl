import os
import json
import logging

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

def path_patient_dir(root:str, patient_id:str):
    """
    :param root: folder for patient specific results, note the "prefix" for CPU/MEM optimization.
    :param patient_id: unique patient
    :return: path to patient *folder*
    """
    # practical limit of number of "files" in a folder is 10,000
    prefix = str(patient_id)
    if len(str(patient_id)) >= 4:
        prefix = prefix[0:4]

    return os.path.join(root, prefix, patient_id)

def path_note_dir(root: str, patient_id:str, md5sum: str):
    """
    :param root: directory for messages
    :param patient_id:
    :param md5sum: hash for note text
    :return: notes directory
    """
    folder = os.path.join(path_patient_dir(root, patient_id), md5sum)

    if not path_exists(folder): os.makedirs(folder)
    return folder

def path_ctakes(root: str, patient_id: str, md5sum: str):
    """
    :param root: directory for messages
    :param patient_id:
    :param md5sum: hash for note text
    :return: path to ctakes.json
    """
    return os.path.join(path_note_dir(root, patient_id, md5sum), 'ctakes.json')

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

