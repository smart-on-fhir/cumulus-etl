import logging
import uuid
import hashlib
import requests

def fake_id() -> uuid:
    """
    Randomly generate a linked Patient identifier
    :return: long universally unique ID
    """
    return str(uuid.uuid4())

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


def philter(clinical_text: str) -> str:
    """
    Protected Health Information filter (Philter): accurately and securely de-identifying free-text clinical notes
    https://www.nature.com/articles/s41746-020-0258-y
    https://github.com/BCHSI/philter-ucsf

    :param clinical_text: text to redact
    :return: redacted text with XXX replacing real PHI
    """
    logging.error('Philter() is not yet implemented.')

    redacted = '*redacted*'

    try:
        redacted = requests.post('https://localhost/not/yet/implemented', data=clinical_text).json()
    except Exception as e:
        pass

    return redacted