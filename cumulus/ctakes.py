"""Interface for talking to a cTAKES server"""

import base64
import cgi
import hashlib
import logging
import os
from typing import List

import ctakesclient
from fhirclient.models.documentreference import DocumentReference
from fhirclient.models.observation import Observation

from cumulus import common, fhir_common, store


def symptoms(cache: store.Root, docref: DocumentReference) -> List[Observation]:
    """
    Extract a list of Observations from NLP-detected symptoms in physician notes

    :param cache: Where to cache NLP results
    :param docref: Physician Note
    :return: list of NLP results encoded as FHIR observations
    """
    docref_id = docref.id
    _, subject_id = fhir_common.unref_resource(docref.subject)

    if not docref.context or not docref.context.encounter:
        logging.warning('No valid encounters for symptoms')  # ideally would print identifier, but it's PHI...
        return []
    _, encounter_id = fhir_common.unref_resource(docref.context.encounter[0])

    # Find the physician note among the attachments
    for content in docref.content:
        if content.attachment.contentType and content.attachment.data:
            mimetype, params = cgi.parse_header(content.attachment.contentType)
            if mimetype == 'text/plain':  # just grab first text we find
                charset = params.get('charset', 'utf8')
                physician_note = base64.standard_b64decode(content.attachment.data).decode(charset)
                break
    else:
        logging.warning('No text/plain content for symptoms')  # ideally would print identifier, but it's PHI...
        return []

    try:
        ctakes_json = extract(cache, physician_note)
    except Exception as exc:  # pylint: disable=broad-except
        logging.error('Could not extract symptoms: %s', exc)
        return []

    observations = []
    for match in ctakes_json.list_sign_symptom(ctakesclient.typesystem.Polarity.pos):
        observation = ctakesclient.text2fhir.nlp_observation(subject_id, encounter_id, docref_id, match)
        observation.id = common.fake_id('Observation')
        observations.append(observation)
    return observations


def extract(cache: store.Root, sentence: str) -> ctakesclient.typesystem.CtakesJSON:
    """
    This is a version of ctakesclient.client.extract() that also uses a cache

    This cache is stored as a series of files in the PHI root. If found, the cached results are used.
    If not found, the cTAKES server is asked to parse the sentence, which can take a while (~20s)
    """
    full_path = cache.joinpath(_target_filename(sentence))

    try:
        cached_response = common.read_json(full_path)
        result = ctakesclient.typesystem.CtakesJSON(source=cached_response)
    except Exception:  # pylint: disable=broad-except
        result = ctakesclient.client.extract(sentence)
        cache.makedirs(os.path.dirname(full_path))
        common.write_json(full_path, result.as_json())

    return result


def _target_filename(sentence: str) -> str:
    """Gives the expected cached-result filename for the given sentence"""

    # There are a few parts to the filename:
    # version: an NLP algorithm version number (ideally this gets changed every time we use a new NLP algorithm)
    # partialsum: first 4 characters of the checksum (to help reduce folder sizes where that matters)
    # hashalgo: which hashing algorithm was used
    # checksum: a checksum hash of the whole sentence
    #
    # Resulting in filenames of the form:
    # ctakes-cache/{version}/{partialsum}/{hashalgo}-{checksum}.json

    # FIXME: Ask ctakesclient for NLP algorithm information. For now, hardcode this and we'll manually update it.
    version = 'version1'

    # MD5 and SHA1 have both been broken. Which might not be super important, since we are putting these files
    # in a PHI-capable folder. But still, why make it easier to brute-force.
    # SHA256 is not yet cryptographically broken and is not much slower. Let's just use it.
    # (Actually, on my machine it is faster. Run 'openssl speed md5 sha1 sha256' to see what you get.)
    checksum = hashlib.sha256(sentence.encode('utf8')).hexdigest()
    partial = checksum[0:4]

    return f'ctakes-cache/{version}/{partial}/sha256-{checksum}.json'
