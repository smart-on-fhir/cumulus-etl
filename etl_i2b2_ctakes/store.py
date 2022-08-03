import os
import json
import logging
import i2b2
import codebook

def path_exists(path) -> bool:
    return os.path.exists(path)

def path_codebook(root):
    return os.path.join(root, 'codebook.json')

def path_patient(root:str, observation: i2b2.ObservationFact):
    """
    :param root: directory for results
    :param observation: patient data
    :return: path to patient result
    """
    # practical limit of number of "files" in a folder is 10,000
    prefix = str(observation.patient_num)
    if len(str(observation.patient_num)) >= 4:
        prefix = prefix[0:4]

    return os.path.join(root, prefix, observation.patient_num)

def path_note(root: str, observation: i2b2.ObservationFact):
    """
    :param root: directory for results
    :param observation: patient data
    :return: path to note result for this patient
    """
    md5sum = codebook.hash_clinical_text(observation.observation_blob)
    folder = os.path.join(path_patient(root, observation), md5sum)

    if not path_exists(folder): os.makedirs(folder)

    return os.path.join(folder, 'ctakes.json')

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

def read(path:str) -> str:
    """
    :param path: (currently filesystem path)
    :return: message: coded message
    """
    path_topic = os.path.join(path, topic)
    logging.debug(f'read() {path_topic}')

    with open(path_topic, 'r') as f:
        message = json.load(f)
    f.close()
    return message

