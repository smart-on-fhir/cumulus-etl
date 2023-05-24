"""Interface for talking to a cTAKES server"""

import hashlib
import logging
import os
from typing import List, Optional

import ctakesclient
import httpx

from cumulus_etl import common, fhir_client, fhir_common, store
from cumulus_etl.nlp import watcher


def ctakes_httpx_client() -> httpx.AsyncClient:
    timeout = httpx.Timeout(180)  # cTAKES can be a bit slow, so be generous with our timeouts
    return httpx.AsyncClient(timeout=timeout)


async def covid_symptoms_extract(
    client: fhir_client.FhirClient,
    ctakes_overrides: str,
    cache: store.Root,
    docref: dict,
    ctakes_http_client: httpx.AsyncClient = None,
    cnlp_http_client: httpx.AsyncClient = None,
) -> Optional[List[dict]]:
    """
    Extract a list of Observations from NLP-detected symptoms in physician notes

    :param client: a client ready to talk to a FHIR server
    :param ctakes_overrides: the path to the overrides folder for cTAKES
    :param cache: Where to cache NLP results
    :param docref: Physician Note
    :param ctakes_http_client: HTTPX client to use for the cTAKES server
    :param cnlp_http_client: HTTPX client to use for the cNLP transformer server
    :return: list of NLP results encoded as FHIR observations
    """
    docref_id = docref["id"]
    _, subject_id = fhir_common.unref_resource(docref["subject"])

    encounters = docref.get("context", {}).get("encounter", [])
    if not encounters:
        logging.warning("No encounters for docref %s", docref_id)
        return None
    _, encounter_id = fhir_common.unref_resource(encounters[0])

    # Find the physician note among the attachments
    try:
        physician_note, _ = await fhir_common.get_docref_note(client, docref)
    except Exception as exc:  # pylint: disable=broad-except
        logging.warning("Error getting text for docref %s: %s", docref_id, exc)
        return None

    # cTAKES cache namespace history (and thus, cache invalidation history):
    #   v1: original cTAKES processing
    # TODO: Ideally we'd also be able to ask ctakesclient for NLP algorithm information as part of this namespace.
    #  For now, we'll manually update this namespace if/when the cTAKES algorithm we use changes.
    ctakes_namespace = "covid_symptom_v1"

    # cNLP cache namespace history (and thus, cache invalidation history):
    #   v1: original addition of cNLP filtering
    #   v2: we started dropping non-covid symptoms, which changes the span ordering
    cnlp_namespace = f"{ctakes_namespace}-cnlp_v2"

    try:
        ctakes_json = await ctakes_extract_with_cache(
            ctakes_overrides, cache, ctakes_namespace, physician_note, client=ctakes_http_client
        )
    except Exception as exc:  # pylint: disable=broad-except
        logging.warning("Could not extract symptoms for docref %s (%s): %s", docref_id, type(exc).__name__, exc)
        return None

    matches = ctakes_json.list_sign_symptom(ctakesclient.typesystem.Polarity.pos)

    # Filter out any match that isn't a covid symptom, for speed of NLP & Athena and to keep minimal data around
    covid_symptom_cuis = {s.cui for s in ctakesclient.filesystem.covid_symptoms()}

    def is_covid_match(m: ctakesclient.typesystem.MatchText):
        return bool(covid_symptom_cuis.intersection({attr.cui for attr in m.conceptAttributes}))

    matches = list(filter(is_covid_match, matches))

    # OK we have cTAKES symptoms. But let's also filter through cNLP transformers to remove any that are negated
    # there too. We have found this to yield better results than cTAKES alone.
    try:
        spans = ctakes_json.list_spans(matches)
        polarities_cnlp = await list_polarity(cache, cnlp_namespace, physician_note, spans, client=cnlp_http_client)
    except Exception as exc:  # pylint: disable=broad-except
        logging.warning("Could not check negation for docref %s (%s): %s", docref_id, type(exc).__name__, exc)
        return None

    # Now filter out any non-positive matches
    positive_matches = []
    for i, match in enumerate(matches):
        if polarities_cnlp[i] == ctakesclient.typesystem.Polarity.pos:
            positive_matches.append(
                {
                    "id": f"{docref_id}.{i}",
                    "docref_id": docref_id,
                    "encounter_id": encounter_id,
                    "subject_id": subject_id,
                    "match": match.as_json(),
                }
            )

    return positive_matches


async def ctakes_extract_with_cache(
    ctakes_overrides: str, cache: store.Root, namespace: str, sentence: str, client: httpx.AsyncClient = None
) -> ctakesclient.typesystem.CtakesJSON:
    """
    This is a version of extract() that also uses a cache

    This cache is stored as a series of files in the PHI root. If found, the cached results are used.
    If not found, the cTAKES server is asked to parse the sentence, which can take a while (~20s)
    """
    full_path = cache.joinpath(_target_filename(namespace, sentence))

    try:
        cached_response = common.read_json(full_path)
        result = ctakesclient.typesystem.CtakesJSON(source=cached_response)
    except Exception:  # pylint: disable=broad-except
        result = await ctakes_extract(ctakes_overrides, sentence, client=client)
        cache.makedirs(os.path.dirname(full_path))
        common.write_json(full_path, result.as_json())

    return result


async def ctakes_extract(
    ctakes_overrides: str, sentence: str, *, client: httpx.AsyncClient = None, attempts: int = 3
) -> ctakesclient.typesystem.CtakesJSON:
    """
    This is a version of ctakesclient.client.extract that retries calls in some cases.

    For certain types of known transitory errors, we retry up to 'attempts' times.
    Other exceptions will be passed up if they occur, and the retry exception will still be raised on the final attempt.

    :param ctakes_overrides: the path to the overrides folder for cTAKES
    :param sentence: the text to pass to cTAKES
    :param client: the httpx client to use
    :param attempts: max number of times to try
    """
    bsv_path = os.path.join(ctakes_overrides, "symptoms.bsv")

    for i in range(attempts - 1):
        try:
            return await ctakesclient.client.extract(sentence, client=client)
        except httpx.ReadTimeout:
            # We've found that cTAKES can get in a state where it often fails to respond,
            # but restarting it unsticks it (probably a memory leak?)
            print("MIKE: restarting...   ", end="", flush=True)
            with watcher.wait_for_ctakes_restart():
                os.utime(bsv_path)  # just 'touch' the file, pretending it was updated
            print("done")

    # Final attempt without any exception capturing
    return await ctakesclient.client.extract(sentence, client=client)


async def list_polarity(
    cache: store.Root,
    namespace: str,
    sentence: str,
    spans: List[tuple],
    client: httpx.AsyncClient = None,
) -> List[ctakesclient.typesystem.Polarity]:
    """
    This is a version of ctakesclient.transformer.list_polarity() that also uses a cache

    This cache is stored as a series of files in the PHI root. If found, the cached results are used.
    If not found, the cTAKES server is asked to parse the sentence, which can take a while (~3s)
    """
    if not spans:
        return []

    full_path = cache.joinpath(_target_filename(namespace, sentence))

    try:
        result = [ctakesclient.typesystem.Polarity(x) for x in common.read_json(full_path)]
    except Exception:  # pylint: disable=broad-except
        result = await ctakesclient.transformer.list_polarity(sentence, spans, client=client)
        cache.makedirs(os.path.dirname(full_path))
        common.write_json(full_path, [x.value for x in result])

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
