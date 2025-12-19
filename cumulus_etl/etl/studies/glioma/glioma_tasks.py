"""Define tasks for the irae study"""

import datetime
import logging
from collections.abc import Generator, Iterator

import cumulus_fhir_support as cfs

from cumulus_etl import common, nlp, store
from cumulus_etl.etl import tasks
from cumulus_etl.etl.studies.glioma.glioma_models import (
    GliomaCaseAnnotation,
)

###############################################################################
# Base IRAE Tasks
# These base classes define common behavior and prompts for the IRAE study tasks.
###############################################################################


class BaseGliomaTask(tasks.BaseModelTaskWithSpans):
    # Task Version History:
    # ** 0 (2025-25): Initial version **
    task_version = 0

    system_prompt = (
        "You are a clinical chart reviewer for a study examining the efficacy of various "
        "treatments for pediatric low-grade glioma (pLGG) across various pathological/genetic "
        "subtypes.\n"
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
        "5. Always produce structured JSON that conforms to the Pydantic schema provided below.\n"
        "\n"
        "Pydantic Schema:\n"
        "%JSON-SCHEMA%"
    )
    user_prompt = (
        "Evaluate the following clinical document for glioma variables and outcomes.\n"
        "Here is the clinical document for you to analyze:\n"
        "\n"
        "%CLINICAL-NOTE%"
    )

    response_format = GliomaCaseAnnotation

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


###############################################################################
# Model-Specific Tasks
#
# For each base Glioma task, we define specific tasks for each NLP model.
# Models supported include:
#   - Gpt4o
#   - Gpt5
#   - GptOss120b
#   - Llama4Scout
#   - ClaudeSonnet45
###############################################################################


class GliomaGpt4oTask(BaseGliomaTask):
    name = "glioma__nlp_gpt4o"
    client_class = nlp.Gpt4oModel


class GliomaGpt5Task(BaseGliomaTask):
    name = "glioma__nlp_gpt5"
    client_class = nlp.Gpt5Model


class GliomaGptOss120bTask(BaseGliomaTask):
    name = "glioma__nlp_gpt_oss_120b"
    client_class = nlp.GptOss120bModel


class GliomaLlama4ScoutTask(BaseGliomaTask):
    name = "glioma__nlp_llama4_scout"
    client_class = nlp.Llama4ScoutModel


class GliomaClaudeSonnet45Task(BaseGliomaTask):
    name = "glioma__nlp_claude_sonnet45"
    client_class = nlp.ClaudeSonnet45Model
