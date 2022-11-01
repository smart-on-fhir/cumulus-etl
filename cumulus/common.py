"""Utility methods"""

import datetime
import os
import logging
import json
import pandas
import uuid
from enum import Enum
from typing import Optional

import fsspec
from fhirclient.models.resource import Resource
from fhirclient.models.fhirabstractbase import FHIRAbstractBase
from fhirclient.models.fhirdate import FHIRDate

from cumulus import store


###############################################################################
#
# Helper Functions: Pandas / CSV / SQL
#
###############################################################################
def list_csv(folder: str, mask='.csv') -> list:
    """
    :param folder: folder to select files from
    :param mask: csv is typical
    :return:
    """
    if not os.path.exists(folder):
        logging.warning('list_csv() does not exist: folder:%s, mask=%s', folder,
                        mask)
        return []

    match = []
    for file in sorted(os.listdir(folder)):
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
    del progress_bar

    found = []
    for dirpath, _, files in os.walk(folder):
        for filename in files:
            path = os.path.join(dirpath, filename)
            if path_contains in path:
                found.append(path)
                if 0 == len(found) % 1000:
                    print(f'found: {len(found)}')
                    print(path)
    return found


def extract_csv(path_csv: str, sample=1.0) -> pandas.DataFrame:
    """
    :param path_csv: /path/to/file.csv
    :param sample: %percentage of file to read
    :return: pandas Dataframe
    """
    logging.info('Reading csv %s ...', path_csv)
    df = pandas.read_csv(path_csv, dtype=str, na_filter=False)
    if sample != 1.0:
        df = df.sample(frac=sample)
    logging.info('Done reading %s .', path_csv)
    return df


def fake_id(category: str) -> str:
    """
    Randomly generate a linked Patient identifier

    This ID is not cryptographically random.

    :param category: the resource type for this ID
    :return: long universally unique ID
    """
    del category  # unused outside of tests
    return str(uuid.uuid4())


###############################################################################
#
# Helper Functions: read/write text and JSON with logging messages
#
###############################################################################

def open_file(path: str, mode: str):
    """A version of open() that handles remote access, like to S3"""
    root = store.Root(path)
    # We pass auto_mkdir because on some backends (like S3), we may not have permissions that fsspec might want,
    # like CreateBucket. We elsewhere call Root.makedirs as needed.
    return fsspec.open(path, mode, encoding='utf8', auto_mkdir=False, **root.fsspec_options())


def read_text(path: str) -> str:
    """
    Reads data from the given path, in text format
    :param path: (currently filesystem path)
    :return: message: coded message
    """
    logging.debug('read_text() %s', path)

    with open_file(path, 'r') as f:
        return f.read()


def write_text(path: str, text: str) -> None:
    """
    Writes data to the given path, in text format
    :param path: filesystem path
    :param text: the text to write to disk
    """
    logging.debug('write_text() %s', path)

    with open_file(path, 'w') as f:
        f.write(text)


def read_json(path: str) -> dict:
    """
    Reads json from a file
    :param path: filesystem path
    :return: message: coded message
    """
    logging.debug('read_json() %s', path)

    with open_file(path, 'r') as f:
        return json.load(f)


def write_json(path: str, data: dict, indent: Optional[int] = None) -> None:
    """
    Writes data to the given path, in json format
    :param path: filesystem path
    :param data: the structure to write to disk
    :param indent: whether and how much to indent the output
    """
    logging.debug('write_json() %s', path)

    with open_file(path, 'w') as f:
        json.dump(data, f, indent=indent)


###############################################################################
#
# Helper Functions: Logging
#
###############################################################################


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
        logging.error('expected FHIR Resource got %s', type(fhir_resource))

def as_json(variable) -> dict:
    out = dict()
    for key, val in variable.items():
        #print(f'type={type(val)}\tkey={key}\tval={val}')
        if isinstance(val, FHIRDate):
            out[key] = str(val.as_json())
        elif isinstance(val, FHIRAbstractBase):
            out[key] = val.as_json()
        elif isinstance(val, Enum):
            out[key] = val.value
        elif isinstance(val, dict):
            out[key] = as_json(val)
        else:
            out[key] = val
    return out

def print_json(jsonable):
    if isinstance(jsonable, dict):
        print(json.dumps(jsonable, indent=4))
    if isinstance(jsonable, list):
        print(json.dumps(jsonable, indent=4))
    if isinstance(jsonable, FHIRAbstractBase):
        print(json.dumps(jsonable.as_json(), indent=4))


###############################################################################
#
# Helper Functions: Timedatestamp
#
###############################################################################

def timestamp_datetime() -> str:
    """
    Human-readable UTC date and time
    :return: MMMM-DD-YYY hh:mm:ss
    """
    return datetime.datetime.now(datetime.timezone.utc).strftime('%Y-%m-%d %H:%M:%S')


def timestamp_filename() -> str:
    """
    Human-readable UTC date and time suitable for a filesystem path

    In particular, there are no characters that need awkward escaping.

    :return: MMMM-DD-YYY__hh.mm.ss
    """
    return datetime.datetime.now(datetime.timezone.utc).strftime('%Y-%m-%d__%H.%M.%S')
