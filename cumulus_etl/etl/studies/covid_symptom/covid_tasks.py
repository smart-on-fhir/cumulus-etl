"""Define tasks for the covid_symptom study"""

import itertools
import json
import logging
import os
from typing import ClassVar

import ctakesclient
import openai
import pyarrow
import rich.progress
from ctakesclient.transformer import TransformerModel
from openai.types import chat

from cumulus_etl import common, nlp, store
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
    codings = list(
        itertools.chain.from_iterable([cat.get("coding", []) for cat in docref.get("category", [])])
    )
    codings += docref.get("type", {}).get("coding", [])
    return any(is_ed_coding(x) for x in codings)


class BaseCovidCtakesTask(tasks.BaseNlpTask):
    """Covid Symptom study task, to generate symptom lists from ED notes using cTAKES + a polarity check"""

    tags: ClassVar = {"covid_symptom", "gpu"}

    # Subclasses: set name, tags, and polarity_model yourself
    polarity_model = None

    # Use a shared task_version for subclasses, to make sharing the ctakes cache folder easier
    # (and they use essentially the same services anyway)
    task_version = 4
    # Task Version History:
    # ** 4 (2024-01): Fixed bug preventing our cTAKES symptoms file from having any effect **
    #   cTAKES: smartonfhir/ctakes-covid:1.1.[01]
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

    outputs: ClassVar = [tasks.OutputTable(resource_type=None, group_field="docref_id")]

    async def prepare_task(self) -> bool:
        bsv_path = ctakesclient.filesystem.covid_symptoms_path()
        success = nlp.restart_ctakes_with_bsv(self.task_config.ctakes_overrides, bsv_path)
        if not success:
            print("  Skipping.")
            self.summaries[0].had_errors = True
        return success

    async def read_entries(self, *, progress: rich.progress.Progress = None) -> tasks.EntryIterator:
        """Passes clinical notes through NLP and returns any symptoms found"""
        phi_root = store.Root(self.task_config.dir_phi, create=True)

        # one client for both NLP services for now -- no parallel requests yet, so no need to be fancy
        http_client = nlp.ctakes_httpx_client()

        async for orig_docref, docref, clinical_note in self.read_notes(
            progress=progress, doc_check=is_ed_docref
        ):
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


class CovidSymptomNlpResultsTask(BaseCovidCtakesTask):
    """Covid Symptom study task, to generate symptom lists from ED notes using cTAKES and cnlpt negation"""

    name: ClassVar = "covid_symptom__nlp_results"
    polarity_model: ClassVar = TransformerModel.NEGATION

    @classmethod
    async def init_check(cls) -> None:
        nlp.check_ctakes()
        nlp.check_negation_cnlpt()


class CovidSymptomNlpResultsTermExistsTask(BaseCovidCtakesTask):
    """Covid Symptom study task, to generate symptom lists from ED notes using cTAKES and cnlpt termexists"""

    name: ClassVar = "covid_symptom__nlp_results_term_exists"
    polarity_model: ClassVar = TransformerModel.TERM_EXISTS

    @classmethod
    async def init_check(cls) -> None:
        nlp.check_ctakes()
        nlp.check_term_exists_cnlpt()


class BaseCovidGptTask(tasks.BaseNlpTask):
    """Covid Symptom study task, using GPT"""

    tags: ClassVar = {"covid_symptom", "cpu"}
    outputs: ClassVar = [tasks.OutputTable(resource_type=None)]

    # Overridden by child classes
    model_id: ClassVar = None

    async def prepare_task(self) -> bool:
        api_key = os.environ.get("AZURE_OPENAI_API_KEY")
        endpoint = os.environ.get("AZURE_OPENAI_ENDPOINT")
        if not api_key or not endpoint:
            if not api_key:
                print("  The AZURE_OPENAI_API_KEY environment variable is not set.")
            if not endpoint:
                print("  The AZURE_OPENAI_ENDPOINT environment variable is not set.")
            print("  Skipping.")
            self.summaries[0].had_errors = True
            return False
        return True

    async def read_entries(self, *, progress: rich.progress.Progress = None) -> tasks.EntryIterator:
        """Passes clinical notes through NLP and returns any symptoms found"""
        async for orig_docref, docref, clinical_note in self.read_notes(
            progress=progress, doc_check=is_ed_docref
        ):
            try:
                docref_id, encounter_id, subject_id = nlp.get_docref_info(docref)
            except KeyError as exc:
                logging.warning(exc)
                self.add_error(orig_docref)
                continue

            client = openai.AsyncAzureOpenAI(api_version="2024-06-01")
            try:
                response = await nlp.cache_wrapper(
                    self.task_config.dir_phi,
                    f"{self.name}_v{self.task_version}",
                    clinical_note,
                    lambda x: chat.ChatCompletion.model_validate_json(x),
                    lambda x: x.model_dump_json(
                        indent=None, round_trip=True, exclude_unset=True, by_alias=True
                    ),
                    client.chat.completions.create,
                    messages=[
                        {"role": "system", "content": "You are a helpful assistant."},
                        {"role": "user", "content": self.get_prompt(clinical_note)},
                    ],
                    model=self.model_id,
                    seed=12345,  # arbitrary, only specified to improve reproducibility
                    response_format={"type": "json_object"},
                )
            except openai.APIError as exc:
                logging.warning(f"Could not connect to GPT for DocRef {docref['id']}: {exc}")
                self.add_error(orig_docref)
                continue

            if response.choices[0].finish_reason != "stop":
                logging.warning(
                    f"GPT response didn't complete for DocRef {docref['id']}: "
                    f"{response.choices[0].finish_reason}"
                )
                self.add_error(orig_docref)
                continue

            try:
                symptoms = json.loads(response.choices[0].message.content)
            except json.JSONDecodeError as exc:
                logging.warning(f"Could not parse GPT results for DocRef {docref['id']}: {exc}")
                self.add_error(orig_docref)
                continue

            yield {
                "id": docref_id,  # keep one results entry per docref
                "docref_id": docref_id,
                "encounter_id": encounter_id,
                "subject_id": subject_id,
                "generated_on": common.datetime_now().isoformat(),
                "task_version": self.task_version,
                "system_fingerprint": response.system_fingerprint,
                "symptoms": {
                    "congestion_or_runny_nose": bool(symptoms.get("Congestion or runny nose")),
                    "cough": bool(symptoms.get("Cough")),
                    "diarrhea": bool(symptoms.get("Diarrhea")),
                    "dyspnea": bool(symptoms.get("Dyspnea")),
                    "fatigue": bool(symptoms.get("Fatigue")),
                    "fever_or_chills": bool(symptoms.get("Fever or chills")),
                    "headache": bool(symptoms.get("Headache")),
                    "loss_of_taste_or_smell": bool(symptoms.get("Loss of taste or smell")),
                    "muscle_or_body_aches": bool(symptoms.get("Muscle or body aches")),
                    "nausea_or_vomiting": bool(symptoms.get("Nausea or vomiting")),
                    "sore_throat": bool(symptoms.get("Sore throat")),
                },
            }

    @staticmethod
    def get_prompt(clinical_note: str) -> str:
        instructions = (
            "You are a helpful assistant identifying symptoms from emergency "
            "department notes that could relate to infectious respiratory diseases.\n"
            "Output positively documented symptoms, looking out specifically for the "
            "following: Congestion or runny nose, Cough, Diarrhea, Dyspnea, Fatigue, "
            "Fever or chills, Headache, Loss of taste or smell, Muscle or body aches, "
            "Nausea or vomiting, Sore throat.\nSymptoms only need to be positively "
            "mentioned once to be included.\nDo not mention symptoms that are not "
            "present in the note.\n\nFollow these rules:\nRule (1): Symptoms must be "
            "positively documented and relevant to the presenting illness or reason "
            "for visit.\nRule (2): Medical section headings must be specific to the "
            "present emergency department encounter.\nInclude positive symptoms from "
            'these medical section headings: "Chief Complaint", "History of '
            'Present Illness", "HPI", "Review of Systems", "Physical Exam", '
            '"Vital Signs", "Assessment and Plan", "Medical Decision Making".\n'
            "Rule (3): Positive symptom mentions must be a definite medical synonym.\n"
            'Include positive mentions of: "anosmia", "loss of taste", "loss of '
            'smell", "rhinorrhea", "congestion", "discharge", "nose is '
            'dripping", "runny nose", "stuffy nose", "cough", "tussive or '
            'post-tussive", "cough is unproductive", "productive cough", "dry '
            'cough", "wet cough", "producing sputum", "diarrhea", "watery '
            'stool", "fatigue", "tired", "exhausted", "weary", "malaise", '
            '"feeling generally unwell", "fever", "pyrexia", "chills", '
            '"temperature greater than or equal 100.4 Fahrenheit or 38 celsius", '
            '"Temperature >= 100.4F", "Temperature >= 38C", "headache", "HA", '
            '"migraine", "cephalgia", "head pain", "muscle or body aches", '
            '"muscle aches", "generalized aches and pains", "body aches", '
            '"myalgias", "myoneuralgia", "soreness", "generalized aches and '
            'pains", "nausea or vomiting", "Nausea", "vomiting", "emesis", '
            '"throwing up", "queasy", "regurgitated", "shortness of breath", '
            '"difficulty breathing", "SOB", "Dyspnea", "breathing is short", '
            '"increased breathing", "labored breathing", "distressed '
            'breathing", "sore throat", "throat pain", "pharyngeal pain", '
            '"pharyngitis", "odynophagia".\nYour reply must be parsable as JSON.\n'
            'Format your response using only the following JSON schema: {"Congestion '
            'or runny nose": boolean, "Cough": boolean, "Diarrhea": boolean, '
            '"Dyspnea": boolean, "Fatigue": boolean, "Fever or chills": '
            'boolean, "Headache": boolean, "Loss of taste or smell": boolean, '
            '"Muscle or body aches": boolean, "Nausea or vomiting": boolean, '
            '"Sore throat": boolean}. Each JSON key should correspond to a symptom, '
            "and each value should be true if that symptom is indicated in the "
            "clinical note; false otherwise.\nNever explain yourself, and only reply "
            "with JSON."
        )
        return f"### Instructions ###\n{instructions}\n### Text ###\n{clinical_note}"

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
                pyarrow.field("system_fingerprint", pyarrow.string()),
                pyarrow.field(
                    "symptoms",
                    pyarrow.struct(
                        [
                            pyarrow.field("congestion_or_runny_nose", pyarrow.bool_()),
                            pyarrow.field("cough", pyarrow.bool_()),
                            pyarrow.field("diarrhea", pyarrow.bool_()),
                            pyarrow.field("dyspnea", pyarrow.bool_()),
                            pyarrow.field("fatigue", pyarrow.bool_()),
                            pyarrow.field("fever_or_chills", pyarrow.bool_()),
                            pyarrow.field("headache", pyarrow.bool_()),
                            pyarrow.field("loss_of_taste_or_smell", pyarrow.bool_()),
                            pyarrow.field("muscle_or_body_aches", pyarrow.bool_()),
                            pyarrow.field("nausea_or_vomiting", pyarrow.bool_()),
                            pyarrow.field("sore_throat", pyarrow.bool_()),
                        ],
                    ),
                ),
            ]
        )


class CovidSymptomNlpResultsGpt35Task(BaseCovidGptTask):
    """Covid Symptom study task, using GPT3.5"""

    name: ClassVar = "covid_symptom__nlp_results_gpt35"
    model_id: ClassVar = "gpt-35-turbo-0125"

    task_version: ClassVar = 1
    # Task Version History:
    # ** 1 (2024-08): Initial version **
    #   model: gpt-35-turbo-0125
    #   seed: 12345


class CovidSymptomNlpResultsGpt4Task(BaseCovidGptTask):
    """Covid Symptom study task, using GPT4"""

    name: ClassVar = "covid_symptom__nlp_results_gpt4"
    model_id: ClassVar = "gpt-4"

    task_version: ClassVar = 1
    # Task Version History:
    # ** 1 (2024-08): Initial version **
    #   model: gpt-4
    #   seed: 12345
