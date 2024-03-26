"""Define tasks for the covid_symptom study"""

import itertools

import ctakesclient
import pyarrow
import rich.progress
from ctakesclient.transformer import TransformerModel

from cumulus_etl import nlp, store
from cumulus_etl.etl import tasks
from cumulus_etl.etl.studies.covid_symptom import covid_ctakes


# List of recognized emergency department note types. We'll add more as we discover them in use.
ED_CODES = {
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
    # The above _should_ be enough if everything is coded well.
    # But sometimes not every document has a LOINC coding included.
    # Below are some site-specific coding systems and their ED codes, to help fill in those gaps.
    "http://cumulus.smarthealthit.org/i2b2": {  # BCH i2b2
        "NOTE:3710480",  # ED Consultation
        "NOTE:3807712",  # ED Note
        "NOTE:149798455",  # Emergency MD
        "NOTE:159552404",  # ED Note Scanned
        "NOTE:189094576",  # ED Scanned
        "NOTE:189094619",  # SANE Report
        "NOTE:189094644",  # Emergency Dept Scanned Forms
        "NOTE:318198107",  # ED Social Work Assessment
        "NOTE:318198110",  # ED Social Work Brief Screening
        "NOTE:318198113",  # ED Social Work
    },
    "https://fhir.cerner.com/96976f07-eccb-424c-9825-e0d0b887148b/codeSet/72": {  # BCH Cerner
        "3710480",  # ED Consultation
        "3807712",  # ED Note
        "149798455",  # Emergency MD
        "159552404",  # ED Note Scanned
        "189094576",  # ED Scanned
        "189094619",  # SANE Report
        "189094644",  # Emergency Dept Scanned Forms
        "318198107",  # ED Social Work Assessment
        "318198110",  # ED Social Work Brief Screening
        "318198113",  # ED Social Work
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


class BaseCovidSymptomNlpResultsTask(tasks.BaseNlpTask):
    """Covid Symptom study task, to generate symptom lists from ED notes using cTAKES + a polarity check"""

    # Subclasses: set name, tags, and polarity_model yourself
    polarity_model = None

    # Use a shared task_version for subclasses, to make sharing the ctakes cache folder easier
    # (and they use essentially the same services anyway)
    task_version = 4
    # Task Version History:
    # ** 4 (2024-01): Fixed bug preventing our cTAKES symptoms file from having any effect **
    #   cTAKES: smartonfhir/ctakes-covid:1.1.0
    #   cNLP: smartonfhir/cnlp-transformers:negation-0.6.1
    #   cNLP: smartonfhir/cnlp-transformers:termexists-0.6.1
    #   ctakesclient: 5.0
    #
    # ** 3 (2023-09): Updated to cnlpt version 0.6.1 **
    #   cTAKES: smartonfhir/ctakes-covid:1.1.0
    #   cNLP: smartonfhir/cnlp-transformers:negation-0.6.1
    #   cNLP: smartonfhir/cnlp-transformers:termexists-0.6.1
    #   ctakesclient: 5.0
    #
    # ** 2 (2023-08): Corrected the cache location (version 1 results might be using stale cache) **
    #   cTAKES: smartonfhir/ctakes-covid:1.1.0
    #   cNLP: smartonfhir/cnlp-transformers:negation-0.4.0
    #   ctakesclient: 5.0
    #
    # ** 1 (2023-08): Updated ICD10 codes from ctakesclient **
    #   cTAKES: smartonfhir/ctakes-covid:1.1.0
    #   cNLP: smartonfhir/cnlp-transformers:negation-0.4.0
    #   ctakesclient: 5.0
    #
    # ** null (before we added a task version) **
    #   cTAKES: smartonfhir/ctakes-covid:1.1.0
    #   cNLP: smartonfhir/cnlp-transformers:negation-0.4.0
    #   ctakesclient: 3.0

    outputs = [tasks.OutputTable(resource_type=None, group_field="docref_id")]

    async def prepare_task(self) -> bool:
        bsv_path = ctakesclient.filesystem.covid_symptoms_path()
        success = nlp.restart_ctakes_with_bsv(self.task_config.ctakes_overrides, bsv_path)
        if not success:
            print(f"Skipping {self.name}.")
            self.summaries[0].had_errors = True
        return success

    async def read_entries(self, *, progress: rich.progress.Progress = None) -> tasks.EntryIterator:
        """Passes clinical notes through NLP and returns any symptoms found"""
        phi_root = store.Root(self.task_config.dir_phi, create=True)

        # one client for both NLP services for now -- no parallel requests yet, so no need to be fancy
        http_client = nlp.ctakes_httpx_client()

        async for orig_docref, docref, clinical_note in self.read_notes(progress=progress, doc_check=is_ed_docref):
            symptoms = await covid_ctakes.covid_symptoms_extract(
                phi_root,
                docref,
                clinical_note,
                polarity_model=self.polarity_model,
                task_version=self.task_version,
                ctakes_http_client=http_client,
                cnlp_http_client=http_client,
            )
            if symptoms is None:
                self.add_error(orig_docref)
                continue

            # Record this docref as processed (so we can delete it from our table if we don't generate symptoms).
            #
            # We could move this up to the beginning of the loop and delete symptoms for notes now marked as
            # superseded or are no longer considered an ED docref, for database cleanliness.
            # But the current approach instead focuses purely on accuracy and makes sure that we zero-out any dangling
            # entries for groups that we do process.
            # Downstream SQL can ignore the above cases itself, as needed.
            self.seen_docrefs.add(docref["id"])

            # Yield the whole set of symptoms at once, to allow for more easily replacing previous a set of symptoms.
            # This way we don't need to worry about symptoms from the same note crossing batch boundaries.
            # The Format class will replace all existing symptoms from this note at once (because we set group_field).
            yield symptoms

    @classmethod
    def get_schema(cls, resource_type: str | None, rows: list[dict]) -> pyarrow.Schema:
        return pyarrow.schema(
            [
                pyarrow.field("id", pyarrow.string()),
                pyarrow.field("docref_id", pyarrow.string()),
                pyarrow.field("encounter_id", pyarrow.string()),
                pyarrow.field("subject_id", pyarrow.string()),
                pyarrow.field("generated_on", pyarrow.string()),
                pyarrow.field("task_version", pyarrow.int32()),
                pyarrow.field(
                    "match",
                    pyarrow.struct(
                        [
                            pyarrow.field("begin", pyarrow.int32()),
                            pyarrow.field("end", pyarrow.int32()),
                            pyarrow.field("text", pyarrow.string()),
                            pyarrow.field("polarity", pyarrow.int8()),
                            pyarrow.field("type", pyarrow.string()),
                            pyarrow.field(
                                "conceptAttributes",
                                pyarrow.list_(
                                    pyarrow.struct(
                                        [
                                            pyarrow.field("code", pyarrow.string()),
                                            pyarrow.field("codingScheme", pyarrow.string()),
                                            pyarrow.field("cui", pyarrow.string()),
                                            pyarrow.field("tui", pyarrow.string()),
                                        ]
                                    )
                                ),
                            ),
                        ]
                    ),
                ),
            ]
        )


class CovidSymptomNlpResultsTask(BaseCovidSymptomNlpResultsTask):
    """Covid Symptom study task, to generate symptom lists from ED notes using cTAKES and cnlpt negation"""

    name = "covid_symptom__nlp_results"
    tags = {"covid_symptom", "gpu"}
    polarity_model = TransformerModel.NEGATION

    @classmethod
    async def init_check(cls) -> None:
        nlp.check_ctakes()
        nlp.check_negation_cnlpt()


class CovidSymptomNlpResultsTermExistsTask(BaseCovidSymptomNlpResultsTask):
    """Covid Symptom study task, to generate symptom lists from ED notes using cTAKES and cnlpt termexists"""

    name = "covid_symptom__nlp_results_term_exists"
    polarity_model = TransformerModel.TERM_EXISTS

    # Explicitly don't use any tags because this is really a "hidden" task that is mostly for comparing
    # polarity model performance more than running a study. So we don't want it to be accidentally run.
    tags = {}

    @classmethod
    async def init_check(cls) -> None:
        nlp.check_ctakes()
        nlp.check_term_exists_cnlpt()
