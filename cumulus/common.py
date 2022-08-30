import os
import logging
import json
import pandas
import uuid
import hashlib
from datetime import datetime, date
from socket import gethostname

#######################################################################################################################
#
# fhirclient imports
#
#######################################################################################################################
from fhirclient.models.resource import Resource
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
def list_csv(folder: str, mask='.csv') -> list:
    """
    :param folder: folder to select files from
    :param mask: csv is typical
    :return:
    """
    if not os.path.exists(folder):
        logging.warning(f'list_csv() does not exist: folder:{folder}, mask={mask}')
        return list()

    match = list()
    for file in os.listdir(folder):
        if file.endswith(mask):
            match.append(os.path.join(folder, file))
    return match

def find_by_name(folder, path_contains='filemask', progress_bar=1000) -> list:
    """
    :param folder: root folder where JSON files are stored
    :param startswith: ctakes.json default, or other file pattern.
    :param progress_bar: show progress status for every ### files found
    :return:
    """
    found = list()
    for dirpath, dirs, files in os.walk(folder):
        for filename in files:
            path = os.path.join(dirpath,filename)
            if path_contains in path:
                found.append(path)
                if 0 == len(found) % 1000:
                    print(f'found: {len(found)}')
                    print(path)
    return found

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
# Helper Functions: read/write text and JSON with logging messages
#
#######################################################################################################################
def read_text(path:str) -> str:
    """
    :param path: (currently filesystem path)
    :return: message: coded message
    """
    logging.debug(f'read_text() {path}')

    with open(path, 'r') as f:
        message = f.read()
        f.close()
    return message

def write_text(path:str, message:str) -> str:
    """
    :param path: topic (currently filesystem path)
    :param message: contents, usually "physician note" text
    :return: path to message
    """
    logging.debug(f'write_text() {path}')

    with open(path, 'w') as f:
        f.write(message)
    return path

def write_json(path:str, message:dict) -> str:
    """
    :param path: topic (currently filesystem path)
    :param message: coded message
    :return: path to message
    """
    logging.debug(f'write_json() {path}')

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

def error_fhir(fhir_resource):
    if isinstance(fhir_resource, Resource):
        logging.error(json.dumps(fhir_resource.as_json(), indent=4))
    else:
        logging.error(f'expected FHIR Resource got {type(fhir_resource)}')

def print_json(jsonable):
    if isinstance(jsonable, dict):
        print(json.dumps(jsonable, indent=4))
    if isinstance(jsonable, Resource):
        print(json.dumps(jsonable.as_json(), indent=4))


    print('#######################################################')
    print(json.dumps(json, indent=4))

def print_fhir(fhir_resource):
    print('#######################################################')
    print(json.dumps(fhir_resource.as_json(), indent=4))

#######################################################################################################################
#
# Helper Functions: Timedatestamp
#
#######################################################################################################################
def timestamp_date() -> str:
    """
    :return: MMMM-DD-YYY
    """
    return datetime.now().strftime("%Y-%m-%d")

def timestamp_datetime() -> str:
    """
    :return: MMMM-DD-YYY hh:mm:ss
    """
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def timestamp() -> str:
    """
    Human readable without escape characters on filesystem path
    :return: MMMM-DD-YYY__hh.mm.ss
    """
    return datetime.now().strftime("%Y-%m-%d__%H.%M.%S")
