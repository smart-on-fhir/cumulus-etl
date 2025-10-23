"""Define tasks for the covid_symptom study"""

import itertools
from typing import ClassVar

import ctakesclient
import pyarrow
import pydantic
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


def is_ed_docref(docref) -> bool:
    """Returns true if this is a coding for an emergency department note"""
    if docref["resourceType"] != "DocumentReference":
        return False

    # We check both type and category for safety -- we aren't sure yet how EHRs are using these fields.
    codings = list(
        itertools.chain.from_iterable([cat.get("coding", []) for cat in docref.get("category", [])])
    )
    codings += docref.get("type", {}).get("coding", [])
    return any(is_ed_coding(x) for x in codings)


class BaseCovidCtakesTask(tasks.BaseNlpTask):
    """Covid Symptom study task, to generate symptom lists from ED notes using cTAKES + a polarity check"""

    # Subclasses: set name and polarity_model yourself
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
            self.seen_groups.add(docref["id"])

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


class CovidSymptoms(pydantic.BaseModel):
    congestion_or_runny_nose: bool = pydantic.Field(alias="Congestion or runny nose")
    cough: bool = pydantic.Field(alias="Cough")
    diarrhea: bool = pydantic.Field(alias="Diarrhea")
    dyspnea: bool = pydantic.Field(alias="Dyspnea")
    fatigue: bool = pydantic.Field(alias="Fatigue")
    fever_or_chills: bool = pydantic.Field(alias="Fever or chills")
    headache: bool = pydantic.Field(alias="Headache")
    loss_of_taste_or_smell: bool = pydantic.Field(alias="Loss of taste or smell")
    muscle_or_body_aches: bool = pydantic.Field(alias="Muscle or body aches")
    nausea_or_vomiting: bool = pydantic.Field(alias="Nausea or vomiting")
    sore_throat: bool = pydantic.Field(alias="Sore throat")


class BaseCovidGptTask(tasks.BaseModelTask):
    """Covid Symptom study task, using GPT"""

    outputs: ClassVar = [tasks.OutputTable(resource_type=None)]
    system_prompt = "You are a helpful assistant."
    user_prompt = (
        "### Instructions ###\n"
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
        "with JSON.\n"
        "### Text ###\n"
        "%CLINICAL-NOTE%"
    )
    response_format = CovidSymptoms

    async def read_entries(self, *, progress: rich.progress.Progress = None) -> tasks.EntryIterator:
        """Passes clinical notes through NLP and returns any symptoms found"""
        # This class predates some of the unified NLP code, so it uses an older format.
        # Convert from new format to the older new one here.
        async for entry in super().read_entries(progress=progress):
            yield {
                "id": entry["note_ref"].removeprefix("DocumentReference/"),
                "docref_id": entry["note_ref"].removeprefix("DocumentReference/"),
                "encounter_id": entry["encounter_ref"].removeprefix("Encounter/"),
                "subject_id": entry["subject_ref"].removeprefix("Patient/"),
                "generated_on": entry["generated_on"],
                "task_version": entry["task_version"],
                "system_fingerprint": entry["system_fingerprint"],
                "symptoms": entry["result"],
            }

    @classmethod
    def get_schema(cls, resource_type: str | None, rows: list[dict]) -> pyarrow.Schema:
        result_schema = cls.convert_pydantic_fields_to_pyarrow(cls.response_format.model_fields)
        return pyarrow.schema(
            [
                pyarrow.field("id", pyarrow.string()),
                pyarrow.field("docref_id", pyarrow.string()),
                pyarrow.field("encounter_id", pyarrow.string()),
                pyarrow.field("subject_id", pyarrow.string()),
                pyarrow.field("generated_on", pyarrow.string()),
                pyarrow.field("task_version", pyarrow.int32()),
                pyarrow.field("system_fingerprint", pyarrow.string()),
                pyarrow.field("symptoms", result_schema),
            ]
        )


class CovidSymptomNlpResultsGpt35Task(BaseCovidGptTask):
    """Covid Symptom study task, using GPT3.5"""

    name: ClassVar = "covid_symptom__nlp_results_gpt35"
    client_class: ClassVar = nlp.Gpt35Model

    task_version: ClassVar = 3
    # Task Version History:
    # ** 3 (2025-10): New serialized format **
    # ** 2 (2025-08): Refactor with some changed params **
    #   Sending pydantic class instead of just asking for JSON
    #   temperature: 0
    # ** 1 (2024-08): Initial version **
    #   model: gpt-35-turbo-0125
    #   seed: 12345


class CovidSymptomNlpResultsGpt4Task(BaseCovidGptTask):
    """Covid Symptom study task, using GPT4"""

    name: ClassVar = "covid_symptom__nlp_results_gpt4"
    client_class: ClassVar = nlp.Gpt4Model

    task_version: ClassVar = 3
    # Task Version History:
    # ** 3 (2025-10): New serialized format **
    # ** 2 (2025-08): Refactor with some changed params **
    #   Sending pydantic class instead of just asking for JSON
    #   temperature: 0
    # ** 1 (2024-08): Initial version **
    #   model: gpt-4
    #   seed: 12345
