"""Helper code for asking cTAKES & cNLP to process docrefs for covid symptoms"""

import logging

import ctakesclient
import httpx
from ctakesclient.transformer import TransformerModel

from cumulus_etl import common, nlp, store


async def covid_symptoms_extract(
    cache: store.Root,
    docref: dict,
    clinical_note: str,
    *,
    polarity_model: TransformerModel,
    task_version: int,
    ctakes_http_client: httpx.AsyncClient = None,
    cnlp_http_client: httpx.AsyncClient = None,
) -> list[dict] | None:
    """
    Extract a list of Observations from NLP-detected symptoms in clinical notes

    :param cache: Where to cache NLP results
    :param docref: DocumentReference resource (scrubbed)
    :param clinical_note: the clinical note already extracted from the docref
    :param polarity_model: how to test the polarity of cTAKES responses
    :param task_version: version of task to inject into results
    :param ctakes_http_client: HTTPX client to use for the cTAKES server
    :param cnlp_http_client: HTTPX client to use for the cNLP transformer server
    :return: list of NLP results encoded as FHIR observations
    """
    try:
        docref_id, encounter_id, subject_id = nlp.get_docref_info(docref)
    except KeyError as exc:
        logging.warning(exc)
        return None

    # cTAKES cache namespace history (and thus, cache invalidation history):
    #   v1: original cTAKES processing
    #   v2+: see CovidSymptomNlpResultsTask's version history
    ctakes_namespace = f"covid_symptom_v{task_version}"

    match polarity_model:
        case TransformerModel.NEGATION:  # original
            # cNLP cache namespace history (and thus, cache invalidation history):
            #   v1: original addition of cNLP filtering
            #   v2: we started dropping non-covid symptoms, which changes the span ordering
            cnlp_namespace = f"{ctakes_namespace}-cnlp_v2"
        case TransformerModel.TERM_EXISTS:
            cnlp_namespace = f"{ctakes_namespace}-cnlp_term_exists_v1"
        case _:
            logging.warning("Unknown polarity method: %s", polarity_model)
            return None

    timestamp = common.datetime_now().isoformat()

    try:
        ctakes_json = await nlp.ctakes_extract(
            cache, ctakes_namespace, clinical_note, client=ctakes_http_client
        )
    except Exception as exc:
        logging.warning(
            "Could not extract symptoms for docref %s (%s): %s", docref_id, type(exc).__name__, exc
        )
        return None

    matches = ctakes_json.list_sign_symptom(ctakesclient.typesystem.Polarity.pos)

    # Filter out any match that isn't a covid symptom, for speed of NLP & Athena and to keep minimal data around
    covid_symptom_cuis = {s.cui for s in ctakesclient.filesystem.covid_symptoms()}

    def is_covid_match(m: ctakesclient.typesystem.MatchText):
        return bool(covid_symptom_cuis.intersection({attr.cui for attr in m.conceptAttributes}))

    matches = filter(is_covid_match, matches)

    # For better reliability when regression/unit testing, sort matches by begin / first code.
    # (With stable sorting, we want the primary sort to be done last.)
    matches = sorted(matches, key=lambda x: x.conceptAttributes and x.conceptAttributes[0].code)
    matches = sorted(matches, key=lambda x: x.begin)

    # OK we have cTAKES symptoms. But let's also filter through cNLP transformers to remove any that are negated
    # there too. We have found this to yield better results than cTAKES alone.
    try:
        spans = ctakes_json.list_spans(matches)
        polarities_cnlp = await nlp.list_polarity(
            cache,
            cnlp_namespace,
            clinical_note,
            spans,
            model=polarity_model,
            client=cnlp_http_client,
        )
    except Exception as exc:
        logging.warning(
            "Could not check polarity for docref %s (%s): %s", docref_id, type(exc).__name__, exc
        )
        return None

    # Helper to make a single row (match_value is None if there were no found symptoms at all)
    def _make_covid_symptom_row(row_id: str, match: dict | None) -> dict:
        return {
            "id": row_id,
            "docref_id": docref_id,
            "encounter_id": encounter_id,
            "subject_id": subject_id,
            "generated_on": timestamp,
            "task_version": task_version,
            "match": match,
        }

    # Now filter out any non-positive matches
    positive_matches = []
    for i, match in enumerate(matches):
        if polarities_cnlp[i] == ctakesclient.typesystem.Polarity.pos:
            positive_matches.append(_make_covid_symptom_row(f"{docref_id}.{i}", match.as_json()))

    if not positive_matches:
        # In this case, we write out a single row with a null match,
        # to flag to the downstream SQL that this DocRef was processed,
        # it just had no symptoms (vs a DocRef that we never handled).
        positive_matches = [_make_covid_symptom_row(f"{docref_id}.0", None)]

    return positive_matches
