"""Interface for talking to a cTAKES server"""

import hashlib
import logging
import os
from typing import List

import ctakesclient
import httpx

from cumulus_etl import common, fhir_client, fhir_common, store


def ctakes_httpx_client() -> httpx.AsyncClient:
    timeout = httpx.Timeout(300)  # cTAKES can be a bit slow, so be generous with our timeouts
    return httpx.AsyncClient(timeout=timeout)


async def covid_symptoms_extract(
    client: fhir_client.FhirClient,
    cache: store.Root,
    docref: dict,
    ctakes_http_client: httpx.AsyncClient = None,
    cnlp_http_client: httpx.AsyncClient = None,
) -> List[dict]:
    """
    Extract a list of Observations from NLP-detected symptoms in physician notes

    :param client: a client ready to talk to a FHIR server
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
        return []
    _, encounter_id = fhir_common.unref_resource(encounters[0])

    # Find the physician note among the attachments
    physician_note, _ = await fhir_common.get_docref_note(client, docref)
    if physician_note is None:
        logging.warning("No text content in docref %s", docref_id)
        return []

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
        ctakes_json = await extract(cache, ctakes_namespace, physician_note, client=ctakes_http_client)
    except Exception as exc:  # pylint: disable=broad-except
        logging.warning("Could not extract symptoms for docref %s (%s): %s", docref_id, type(exc).__name__, exc)
        return []

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
        polarities_cnlp = [ctakesclient.typesystem.Polarity.pos] * len(matches)  # fake all positives

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


async def extract(
    cache: store.Root, namespace: str, sentence: str, client: httpx.AsyncClient = None
) -> ctakesclient.typesystem.CtakesJSON:
    """
    This is a version of ctakesclient.client.extract() that also uses a cache

    This cache is stored as a series of files in the PHI root. If found, the cached results are used.
    If not found, the cTAKES server is asked to parse the sentence, which can take a while (~20s)
    """
    full_path = cache.joinpath(_target_filename(namespace, sentence))

    try:
        cached_response = common.read_json(full_path)
        result = ctakesclient.typesystem.CtakesJSON(source=cached_response)
    except Exception:  # pylint: disable=broad-except
        result = await ctakesclient.client.extract(sentence, client=client)
        cache.makedirs(os.path.dirname(full_path))
        common.write_json(full_path, result.as_json())

    return result


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
