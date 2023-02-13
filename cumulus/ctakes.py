"""Interface for talking to a cTAKES server"""

import base64
import cgi
import hashlib
import logging
import os
from typing import List, Optional

import ctakesclient

from cumulus import common, fhir_client, fhir_common, store


async def covid_symptoms_extract(client: fhir_client.FhirClient, cache: store.Root, docref: dict) -> List[dict]:
    """
    Extract a list of Observations from NLP-detected symptoms in physician notes

    :param client: a client ready to talk to a FHIR server
    :param cache: Where to cache NLP results
    :param docref: Physician Note
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
    physician_note = await get_docref_note(client, [content["attachment"] for content in docref["content"]])
    if physician_note is None:
        logging.warning("No text content in docref %s", docref_id)
        return []

    # Strip this "line feed" character that often shows up in notes and is confusing for cNLP.
    physician_note = physician_note.replace("¿", " ")

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
        ctakes_json = extract(cache, ctakes_namespace, physician_note)
    except Exception:  # pylint: disable=broad-except
        logging.exception("Could not extract symptoms for docref: %s", docref_id)
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
        polarities_cnlp = list_polarity(cache, cnlp_namespace, physician_note, spans)
    except Exception:  # pylint: disable=broad-except
        logging.exception("Could not check negation for docref %s", docref_id)
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


def parse_content_type(content_type: str) -> (str, str):
    """Returns (mimetype, encoding)"""
    # TODO: switch to message.Message parsing, since cgi is deprecated
    mimetype, params = cgi.parse_header(content_type)
    return mimetype, params.get("charset", "utf8")


def mimetype_priority(mimetype: str) -> int:
    """
    Returns priority of mimetypes for docref notes.

    0 means "ignore"
    Higher numbers are higher priority
    """
    if mimetype == "text/plain":
        return 3
    elif mimetype.startswith("text/"):
        return 2
    elif mimetype in ("application/xml", "application/xhtml+xml"):
        return 1
    return 0


async def get_docref_note(client: fhir_client.FhirClient, attachments: List[dict]) -> Optional[str]:
    # Find the best attachment to use, based on mimetype.
    # We prefer basic text documents, to avoid confusing cTAKES with extra formatting (like <body>).
    best_attachment_index = -1
    best_attachment_priority = 0
    for index, attachment in enumerate(attachments):
        if "contentType" in attachment:
            mimetype, _ = parse_content_type(attachment["contentType"])
            priority = mimetype_priority(mimetype)
            if priority > best_attachment_priority:
                best_attachment_priority = priority
                best_attachment_index = index

    if best_attachment_index >= 0:
        return await get_docref_note_from_attachment(client, attachments[best_attachment_index])

    # We didn't find _any_ of our target text content types.
    # A content type isn't required by the spec with external URLs... so it's possible an unmarked link could be good.
    # But let's optimistically enforce the need for a content type ourselves by bailing here.
    # If we find a real-world need to be more permissive, we can change this later.
    # But note that if we do, we'll need to handle downloading Binary FHIR objects, in addition to arbitrary URLs.
    return None


async def get_docref_note_from_attachment(client: fhir_client.FhirClient, attachment: dict) -> Optional[str]:
    """
    Decodes or downloads a note from an attachment.

    Note that it is assumed a contentType is provided.

    :returns: the attachment's note text
    """
    mimetype, charset = parse_content_type(attachment["contentType"])

    if "data" in attachment:
        return base64.standard_b64decode(attachment["data"]).decode(charset)

    # TODO: At some point we should centralize the downloading of attachments -- once we have multiple NLP tasks,
    #  we may not want to re-download the overlapping notes. When we do that, it should not be part of our bulk
    #  exporter, since we may be given already-exported ndjson.
    #
    # TODO: There are future optimizations to try to use our ctakes cache to avoid downloading in the first place:
    #   - use attachment["hash"] if available (algorithm mismatch though... maybe we should switch to sha1...)
    #   - send a HEAD request with "Want-Digest: sha-256" but Cerner at least does not support that
    if "url" in attachment:
        # We need to pass Accept to get the raw data, not a Binary object. See https://www.hl7.org/fhir/binary.html
        response = await client.request("GET", attachment["url"], headers={"Accept": mimetype})
        return response.text

    return None


def extract(cache: store.Root, namespace: str, sentence: str) -> ctakesclient.typesystem.CtakesJSON:
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
        result = ctakesclient.client.extract(sentence)
        cache.makedirs(os.path.dirname(full_path))
        common.write_json(full_path, result.as_json())

    return result


def list_polarity(
    cache: store.Root,
    namespace: str,
    sentence: str,
    spans: List[tuple],
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
        result = ctakesclient.transformer.list_polarity(sentence, spans)
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
