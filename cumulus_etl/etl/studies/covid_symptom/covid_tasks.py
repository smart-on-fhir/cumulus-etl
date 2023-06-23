"""Define tasks for the covid_symptom study"""

import copy
import itertools
import os
from collections.abc import AsyncIterator

import ctakesclient

from cumulus_etl import common, nlp, store
from cumulus_etl.etl import tasks
from cumulus_etl.etl.studies.covid_symptom import covid_ctakes


# List of recognized emergency department note types. We'll add more as we discover them in use.
ED_CODES = {
    "http://cumulus.smarthealthit.org/i2b2": {
        "NOTE:3710480",
        "NOTE:3807712",
        "NOTE:149798455",
        "NOTE:159552404",
        "NOTE:189094576",
        "NOTE:189094619",
        "NOTE:189094644",
        "NOTE:318198107",
        "NOTE:318198110",
        "NOTE:318198113",
    },
    "http://loinc.org": {
        "18842-5",  # Discharge Summary
        "28568-4",  # Physician Emergency department Note
        "34111-5",  # Emergency department Note
        "34878-9",  # Emergency medicine Note
        "51846-4",  # Emergency department Consult note
        "54094-8",  # Emergency department Triage note
        "57053-1",  # Nurse Emergency department Note
        "57054-9",  # Nurse Emergency department Triage+care note
        "59258-4",  # Emergency department Discharge summary
        "60280-5",  # Emergency department Discharge instructions
        "68552-9",  # Emergency medicine Emergency department Admission evaluation note
        "74187-6",  # InterRAI Emergency Screener for Psychiatry (ESP) Document
        "74211-4",  # Summary of episode note Emergency department+Hospital
    },
}


def is_ed_coding(coding):
    """Returns true if this is a coding for an emergency department note"""
    return coding.get("code") in ED_CODES.get(coding.get("system"), {})


def is_ed_docref(docref):
    """Returns true if this is a coding for an emergency department note"""
    # We check both type and category for safety -- we aren't sure yet how EHRs are using these fields.
    codings = list(itertools.chain.from_iterable([cat.get("coding", []) for cat in docref.get("category", [])]))
    codings += docref.get("type", {}).get("coding", [])
    return any(is_ed_coding(x) for x in codings)


class CovidSymptomNlpResultsTask(tasks.EtlTask):
    """Covid Symptom study task, to generate symptom lists from ED notes using NLP"""

    name = "covid_symptom__nlp_results"
    resource = "DocumentReference"
    tags = {"covid_symptom", "gpu"}
    output_resource = None  # custom format
    group_field = "docref_id"

    async def prepare_task(self) -> bool:
        bsv_path = ctakesclient.filesystem.covid_symptoms_path()
        success = nlp.restart_ctakes_with_bsv(self.task_config.ctakes_overrides, bsv_path)
        if not success:
            print(f"Skipping {self.name}.")
        return success

    def add_error(self, docref: dict) -> None:
        if not self.task_config.dir_errors:
            return

        error_root = store.Root(os.path.join(self.task_config.dir_errors, self.name), create=True)
        error_path = error_root.joinpath("nlp-errors.ndjson")
        with common.NdjsonWriter(error_path, "a") as writer:
            writer.write(docref)

    async def read_entries(self) -> AsyncIterator[dict | list[dict]]:
        """Passes clinical notes through NLP and returns any symptoms found"""
        phi_root = store.Root(self.task_config.dir_phi, create=True)

        # one client for both NLP services for now -- no parallel requests yet, so no need to be fancy
        http_client = nlp.ctakes_httpx_client()

        for docref in self.read_ndjson():
            # Only bother running NLP on docs that are current & finalized
            bad_ref_status = docref.get("status") in ("superseded", "entered-in-error")  # status of DocRef itself
            bad_doc_status = docref.get("docStatus") in ("preliminary", "entered-in-error")  # status of clinical note
            if bad_ref_status or bad_doc_status:
                continue

            # Check that the note is one of our special allow-listed types (we do this here rather than on the output
            # side to save needing to run everything through NLP).
            if not is_ed_docref(docref):
                continue

            orig_docref = copy.deepcopy(docref)
            if not self.scrubber.scrub_resource(docref, scrub_attachments=False):
                continue

            symptoms = await covid_ctakes.covid_symptoms_extract(
                self.task_config.client,
                phi_root,
                docref,
                ctakes_http_client=http_client,
                cnlp_http_client=http_client,
            )
            if symptoms is None:
                self.add_error(orig_docref)
                continue

            # Yield the whole set of symptoms at once, to allow for more easily replacing previous a set of symptoms.
            # This way we don't need to worry about symptoms from the same note crossing batch boundaries.
            # The Format class will replace all existing symptoms from this note at once (because we set group_field).
            yield symptoms
