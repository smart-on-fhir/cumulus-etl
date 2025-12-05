"""Define tasks for the irae study"""

import datetime
import logging
from collections.abc import Generator, Iterator

import cumulus_fhir_support as cfs

from cumulus_etl import common, nlp, store
from cumulus_etl.etl import tasks
from cumulus_etl.etl.studies.irae.irae_multiple_transplant_history_models import (
    MultipleTransplantHistoryAnnotation,
)
from cumulus_etl.etl.studies.irae.irae_donor_models import (
    KidneyTransplantDonorGroupAnnotation,
)
from cumulus_etl.etl.studies.irae.irae_longitudinal_models import (
    KidneyTransplantLongitudinalAnnotation,
)
from cumulus_etl.etl.studies.irae.irae_med_models import (
    ImmunosuppresiveMedicationsAnnotation,
)


###############################################################################
# Base IRAE Tasks
# These base classes define common behavior and prompts for the IRAE study tasks.
###############################################################################


class BaseIraeTask(tasks.BaseModelTaskWithSpans):
    task_version = 7
    # Task Version History:
    # ** 7 (2025-12): New immunosuppresive medications task **
    # ** 6 (2025-11): Pydantic updates (donors refer to 1st transplant;
    #                 POD inference guidance; new multiple transplant task) **
    # ** 5 (2025-10): Update pydantic model (biological relation;
    #                 Defaults for SpanAugmentedMention properties) **
    # ** 4 (2025-10): Split into donor & longitudinal models **
    # ** 3 (2025-10): New serialized format **
    # ** 2 (2025-09): Updated prompt and pydantic models **
    # ** 1 (2025-08): Updated prompt **
    # ** 0 (2025-08): Initial version **

    system_prompt = (
        "You are a clinical chart reviewer for a kidney transplant outcomes study.\n"
        "Your task is to extract patient-specific information from an unstructured clinical "
        "document and map it into a predefined Pydantic schema.\n"
        "\n"
        "Core Rules:\n"
        "1. Base all assertions ONLY on patient-specific information in the clinical document.\n"
        "   - Never negate or exclude information just because it is not mentioned.\n"
        "   - Never conflate family history or population-level risk with patient findings.\n"
        "   - Do not count past medical history, prior episodes, or family history.\n"
        "2. Do not invent or infer facts beyond what is documented.\n"
        "3. Maintain high fidelity to the clinical document language when citing spans.\n"
        "4. Answer patient outcomes with strongest available documented evidence:\n"
        "    BIOPSY_PROVEN > CONFIRMED > SUSPECTED > NONE_OF_THE_ABOVE.\n"
        "5. Always produce structured JSON that conforms to the Pydantic schema provided below.\n"
        "\n"
        "Pydantic Schema:\n"
        "%JSON-SCHEMA%"
    )
    user_prompt = (
        "Evaluate the following clinical document for kidney transplant variables and outcomes.\n"
        "Here is the clinical document for you to analyze:\n"
        "\n"
        "%CLINICAL-NOTE%"
    )

    @staticmethod
    def ndjson_in_order(input_root: store.Root, resource: str) -> Generator[dict]:
        # To avoid loading all the notes into memory, we'll first go through each note, and keep
        # track of their byte offset on disk and their date. Then we'll grab each from disk in
        # order.

        # Get a list of all files we're going to be working with here
        filenames = common.ls_resources(input_root, {resource})

        # Go through all files, keeping a record of each line's dates and offsets.
        note_info = []
        for file_index, path in enumerate(filenames):
            for row in cfs.read_multiline_json_with_details(path, fsspec_fs=input_root.fs):
                date = nlp.get_note_date(row["json"]) or datetime.datetime.max
                if not date.tzinfo:  # to compare, we need everything to be aware
                    date = date.replace(tzinfo=datetime.UTC)
                note_info.append((date, file_index, row["byte_offset"]))

        # Now yield each note again in order, reading each from disk
        note_info.sort()
        for _date, file_index, offset in note_info:
            rows = cfs.read_multiline_json_with_details(
                filenames[file_index],
                offset=offset,
                fsspec_fs=input_root.fs,
            )
            # StopIteration errors shouldn't happen here, because we just went through these
            # files above, but just to be safe, we'll gracefully intercept it.
            try:
                yield next(rows)["json"]
            except StopIteration:  # pragma: no cover
                logging.warning(
                    f"File '{filenames[file_index]}' changed while reading, skipping some notes."
                )
                continue

    # Override the read-from-disk portion, so we can order notes in oldest-to-newest order
    def read_ndjson_from_disk(self, input_root: store.Root, resource: str) -> Iterator[dict]:
        yield from self.ndjson_in_order(input_root, resource)


class BaseImmunosuppressiveMedicationsIraeTask(BaseIraeTask):
    response_format = ImmunosuppresiveMedicationsAnnotation


class BaseMultipleTransplantHistoryIraeTask(BaseIraeTask):
    response_format = MultipleTransplantHistoryAnnotation


class BaseDonorIraeTask(BaseIraeTask):
    response_format = KidneyTransplantDonorGroupAnnotation


class BaseLongitudinalIraeTask(BaseIraeTask):
    response_format = KidneyTransplantLongitudinalAnnotation

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.subject_refs_to_skip = set()

    def should_skip(self, orig_note: dict) -> bool:
        subject_ref = nlp.get_note_subject_ref(orig_note)
        return subject_ref in self.subject_refs_to_skip or super().should_skip(orig_note)


###############################################################################
# Model-Specific Tasks
#
# For each base IRAE task, we define specific tasks for each NLP model.
# Models supported include:
#   - Gpt4o
#   - Gpt5
#   - GptOss120b
#   - Llama4Scout
#   - ClaudeSonnet45
###############################################################################


class IraeImmunosuppressiveMedicationsGpt4oTask(BaseImmunosuppressiveMedicationsIraeTask):
    name = "irae__nlp_immunosuppresive_medications_gpt4o"
    client_class = nlp.Gpt4oModel


class IraeImmunosuppressiveMedicationsGpt5Task(BaseImmunosuppressiveMedicationsIraeTask):
    name = "irae__nlp_immunosuppresive_medications_gpt5"
    client_class = nlp.Gpt5Model


class IraeImmunosuppressiveMedicationsGptOss120bTask(BaseImmunosuppressiveMedicationsIraeTask):
    name = "irae__nlp_immunosuppresive_medications_gpt_oss_120b"
    client_class = nlp.GptOss120bModel


class IraeImmunosuppressiveMedicationsLlama4ScoutTask(BaseImmunosuppressiveMedicationsIraeTask):
    name = "irae__nlp_immunosuppresive_medications_llama4_scout"
    client_class = nlp.Llama4ScoutModel


class IraeImmunosuppressiveMedicationsClaudeSonnet45Task(BaseImmunosuppressiveMedicationsIraeTask):
    name = "irae__nlp_immunosuppresive_medications_claude_sonnet45"
    client_class = nlp.ClaudeSonnet45Model


class IraeMultipleTransplantHistoryGpt4oTask(BaseMultipleTransplantHistoryIraeTask):
    name = "irae__nlp_multiple_transplant_history_gpt4o"
    client_class = nlp.Gpt4oModel


class IraeMultipleTransplantHistoryGpt5Task(BaseMultipleTransplantHistoryIraeTask):
    name = "irae__nlp_multiple_transplant_history_gpt5"
    client_class = nlp.Gpt5Model


class IraeMultipleTransplantHistoryGptOss120bTask(BaseMultipleTransplantHistoryIraeTask):
    name = "irae__nlp_multiple_transplant_history_gpt_oss_120b"
    client_class = nlp.GptOss120bModel


class IraeMultipleTransplantHistoryLlama4ScoutTask(BaseMultipleTransplantHistoryIraeTask):
    name = "irae__nlp_multiple_transplant_history_llama4_scout"
    client_class = nlp.Llama4ScoutModel


class IraeMultipleTransplantHistoryClaudeSonnet45Task(BaseMultipleTransplantHistoryIraeTask):
    name = "irae__nlp_multiple_transplant_history_claude_sonnet45"
    client_class = nlp.ClaudeSonnet45Model


class IraeDonorGpt4oTask(BaseDonorIraeTask):
    name = "irae__nlp_donor_gpt4o"
    client_class = nlp.Gpt4oModel


class IraeDonorGpt5Task(BaseDonorIraeTask):
    name = "irae__nlp_donor_gpt5"
    client_class = nlp.Gpt5Model


class IraeDonorGptOss120bTask(BaseDonorIraeTask):
    name = "irae__nlp_donor_gpt_oss_120b"
    client_class = nlp.GptOss120bModel


class IraeDonorLlama4ScoutTask(BaseDonorIraeTask):
    name = "irae__nlp_donor_llama4_scout"
    client_class = nlp.Llama4ScoutModel


class IraeDonorClaudeSonnet45Task(BaseDonorIraeTask):
    name = "irae__nlp_donor_claude_sonnet45"
    client_class = nlp.ClaudeSonnet45Model


class IraeLongitudinalGpt4oTask(BaseLongitudinalIraeTask):
    name = "irae__nlp_gpt4o"
    client_class = nlp.Gpt4oModel


class IraeLongitudinalGpt5Task(BaseLongitudinalIraeTask):
    name = "irae__nlp_gpt5"
    client_class = nlp.Gpt5Model


class IraeLongitudinalGptOss120bTask(BaseLongitudinalIraeTask):
    name = "irae__nlp_gpt_oss_120b"
    client_class = nlp.GptOss120bModel


class IraeLongitudinalLlama4ScoutTask(BaseLongitudinalIraeTask):
    name = "irae__nlp_llama4_scout"
    client_class = nlp.Llama4ScoutModel


class IraeLongitudinalClaudeSonnet45Task(BaseLongitudinalIraeTask):
    name = "irae__nlp_claude_sonnet45"
    client_class = nlp.ClaudeSonnet45Model
