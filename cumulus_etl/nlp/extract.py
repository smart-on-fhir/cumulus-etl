"""Interface for talking to a cTAKES server"""

import hashlib

import ctakesclient
import cumulus_fhir_support as cfs
import httpx
from ctakesclient.transformer import TransformerModel


def ctakes_httpx_client() -> httpx.AsyncClient:
    timeout = httpx.Timeout(300)  # cTAKES can be a bit slow, so be generous with our timeouts
    return httpx.AsyncClient(timeout=timeout)


async def ctakes_extract(
    cache: cfs.FsPath, namespace: str, sentence: str, client: httpx.AsyncClient = None
) -> ctakesclient.typesystem.CtakesJSON:
    """
    This is a version of ctakesclient.client.extract() that also uses a cache

    This cache is stored as a series of files in the PHI root. If found, the cached results are used.
    If not found, the cTAKES server is asked to parse the sentence, which can take a while (~20s)
    """
    full_path = cache.joinpath(_target_filename(namespace, sentence))

    try:
        cached_response = full_path.read_json()
        result = ctakesclient.typesystem.CtakesJSON(source=cached_response)
    except Exception:
        result = await ctakesclient.client.extract(sentence, client=client)
        full_path.parent.makedirs()
        full_path.write_json(result.as_json())

    return result


async def list_polarity(
    cache: cfs.FsPath,
    namespace: str,
    sentence: str,
    spans: list[tuple],
    client: httpx.AsyncClient = None,
    model: TransformerModel = TransformerModel.NEGATION,
) -> list[ctakesclient.typesystem.Polarity]:
    """
    This is a version of ctakesclient.transformer.list_polarity() that also uses a cache

    This cache is stored as a series of files in the PHI root. If found, the cached results are used.
    If not found, the cTAKES server is asked to parse the sentence, which can take a while (~3s)
    """
    if not spans:
        return []

    full_path = cache.joinpath(_target_filename(namespace, sentence))

    try:
        result = [ctakesclient.typesystem.Polarity(x) for x in full_path.read_json()]
    except Exception:
        result = await ctakesclient.transformer.list_polarity(
            sentence, spans, client=client, model=model
        )
        full_path.parent.makedirs()
        full_path.write_json([x.value for x in result])

    return result


def _target_filename(namespace: str, sentence: str) -> str:
    """Gives the expected cached-result filename for the given sentence"""

    # There are a few parts to the filename:
    # namespace: a study / NLP algorithm namespace (unique per study and per NLP algorithm change)
    # partialsum: first 4 characters of the checksum (to help reduce folder sizes where that matters)
    # hashalgo: which hashing algorithm was used
    # checksum: a checksum hash of the whole sentence
    #
    # Resulting in filenames of the form:
    # ctakes-cache/{namespace}/{partialsum}/{hashalgo}-{checksum}.json

    # MD5 and SHA1 have both been broken. Which might not be super important, since we are putting these files
    # in a PHI-capable folder. But still, why make it easier to brute-force.
    # SHA256 is not yet cryptographically broken and is not much slower. Let's just use it.
    # (Actually, on my machine it is faster. Run 'openssl speed md5 sha1 sha256' to see what you get.)
    checksum = hashlib.sha256(sentence.encode("utf8")).hexdigest()
    partial = checksum[0:4]

    return f"ctakes-cache/{namespace}/{partial}/sha256-{checksum}.json"
