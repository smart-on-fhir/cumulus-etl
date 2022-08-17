import logging
import json
import pandas
import uuid
import hashlib

#######################################################################################################################
#
# fhirclient imports
#
#######################################################################################################################

from fhirclient.models.identifier import Identifier
from fhirclient.models.fhirreference import FHIRReference
from fhirclient.models.fhirdate import FHIRDate
from fhirclient.models.meta import Meta
from fhirclient.models.period import Period
from fhirclient.models.duration import Duration
from fhirclient.models.coding import Coding
from fhirclient.models.extension import Extension
from fhirclient.models.patient import Patient
from fhirclient.models.encounter import Encounter
from fhirclient.models.condition import Condition
from fhirclient.models.observation import Observation
from fhirclient.models.documentreference import DocumentReference
from fhirclient.models.documentreference import DocumentReferenceContext, DocumentReferenceContent
from fhirclient.models.attachment import Attachment
from fhirclient.models.codeableconcept import CodeableConcept

#######################################################################################################################
#
# Helper Functions: Pandas / CSV / SQL
#
#######################################################################################################################

def extract_csv(path_csv:str, sample=1.0) -> pandas.DataFrame:
    """
    :param path_csv: /path/to/file.csv
    :param sample: %percentage of file to read
    :return: pandas Dataframe
    """
    logging.info(f'Reading csv {path_csv} ...')
    df = pandas.read_csv(path_csv, dtype=str).sample(frac=sample)
    logging.info(f'Done reading {path_csv} .')
    return df

def fake_id() -> uuid:
    """
    Randomly generate a linked Patient identifier
    :return: long universally unique ID
    """
    return str(uuid.uuid4())

#######################################################################################################################
#
# Helper Functions: Fake Identifiers
#
#######################################################################################################################

def hash_clinical_text(text: str) -> str:
    """
    Get "fingerprint" of clinical text to check if two inputs of the same text
    were both sent to ctakes. This is the intent of this method.
    :param text: clinical text
    :return: md5 digest
    """
    if not isinstance(text, str):
        logging.warning(f'invalid input, expected str but type(text) = {type(text)}')
        logging.warning(f"hash_clinical_text() : invalid input, text= {text}")
        return None

    return hashlib.md5(text.encode('utf-8')).hexdigest()

#######################################################################################################################
#
# Helper Functions: read/write JSON with logging messages
#
#######################################################################################################################

def write_json(path:str, message:dict) -> str:
    """
    :param path: topic (currently filesystem path)
    :param message: coded message
    :return: path to message
    """
    logging.debug(f'write() {path}')

    with open(path, 'w') as f:
        f.write(json.dumps(message, indent=4))

    return path

def read_json(path:str) -> dict:
    """
    :param path: (currently filesystem path)
    :return: message: coded message
    """
    logging.debug(f'read() {path}')

    with open(path, 'r') as f:
        message = json.load(f)
    f.close()
    return message

#######################################################################################################################
#
# Helper Functions: Logging
#
#######################################################################################################################

def info_mode():
    logging.basicConfig()
    logging.getLogger().setLevel(logging.INFO)

def debug_mode():
    logging.basicConfig()
    logging.getLogger().setLevel(logging.DEBUG)

def warn_mode():
    logging.basicConfig()
    logging.getLogger().setLevel(logging.WARN)