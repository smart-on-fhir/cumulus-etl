"""Interface for talking to a cTAKES server"""

import hashlib

import ctakesclient

from cumulus import common, store


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
