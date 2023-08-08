"""Helper code for asking cTAKES & cNLP to process docrefs for covid symptoms"""

import logging

import ctakesclient
import httpx

from cumulus_etl import common, fhir, nlp, store


async def covid_symptoms_extract(
    client: fhir.FhirClient,
    cache: store.Root,
    docref: dict,
    ctakes_http_client: httpx.AsyncClient = None,
    cnlp_http_client: httpx.AsyncClient = None,
) -> list[dict] | None:
    """
    Extract a list of Observations from NLP-detected symptoms in clinical notes

    :param client: a client ready to talk to a FHIR server
    :param cache: Where to cache NLP results
    :param docref: Clinical Note
    :param ctakes_http_client: HTTPX client to use for the cTAKES server
    :param cnlp_http_client: HTTPX client to use for the cNLP transformer server
    :return: list of NLP results encoded as FHIR observations
    """
    docref_id = docref["id"]
    _, subject_id = fhir.unref_resource(docref["subject"])

    encounters = docref.get("context", {}).get("encounter", [])
    if not encounters:
        logging.warning("No encounters for docref %s", docref_id)
        return None
    _, encounter_id = fhir.unref_resource(encounters[0])

    # Find the clinical note among the attachments
    try:
        clinical_note = await fhir.get_docref_note(client, docref)
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

    timestamp = common.datetime_now().isoformat()

    try:
        ctakes_json = await nlp.ctakes_extract(cache, ctakes_namespace, clinical_note, client=ctakes_http_client)
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
        polarities_cnlp = await nlp.list_polarity(cache, cnlp_namespace, clinical_note, spans, client=cnlp_http_client)
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
                    "generated_on": timestamp,
                    "match": match.as_json(),
                }
            )

    return positive_matches
