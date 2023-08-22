"""Define tasks for the hftest study"""

import logging

import httpx
import pyarrow
import rich.progress

from cumulus_etl import common, fhir, formats, nlp
from cumulus_etl.etl import tasks


class HuggingFaceTestTask(tasks.EtlTask):
    """Hugging Face Test study task, to generate a summary from text"""

    name = "hftest__summary"
    resource = "DocumentReference"
    needs_bulk_deid = False
    outputs = [tasks.OutputTable(schema=None)]

    # Task Version
    # The "task_version" field is a simple integer that gets incremented any time an NLP-relevant parameter is changed.
    # This is a reference to a bundle of metadata (model revision, container revision, prompt string).
    # We could combine all that info into a field we save with the results. But it's more human-friendly to have a
    # simple version to refer to. So anytime these properties get changed, bump the version and record the old bundle
    # of metadata too. Also update the safety checks in prepare_task()
    task_version = 0

    # Task Version History:
    # ** 0 **
    #   This is fluid until we actually promote this to a real task - feel free to update without bumping the version.
    #   container: ghcr.io/huggingface/text-generation-inference
    #   container reversion: 09eca6422788b1710c54ee0d05dd6746f16bb681
    #   container properties:
    #     QUANTIZE=bitsandbytes-nf4
    #   model: meta-llama/Llama-2-13b-chat-hf
    #   model revision: 0ba94ac9b9e1d5a0037780667e8b219adde1908c
    #   system prompt:
    #     "You will be given a clinical note, and you should reply with a short summary of that note."
    #   user prompt: a clinical note

    async def prepare_task(self) -> bool:
        try:
            raw_info = await nlp.hf_info()
        except httpx.HTTPError:
            logging.warning(
                " Skipping task: NLP server is unreachable.\n Try running 'docker compose up llama2 --wait'."
            )
            self.summaries[0].had_errors = True
            return False

        # Sanity check a few of the properties, to make sure we don't accidentally get pointed at an unexpected model.
        expected_info_present = (
            raw_info.get("model_id") == "meta-llama/Llama-2-13b-chat-hf"
            and raw_info.get("model_sha") == "0ba94ac9b9e1d5a0037780667e8b219adde1908c"
            and raw_info.get("sha") == "09eca6422788b1710c54ee0d05dd6746f16bb681"
        )
        if not expected_info_present:
            logging.warning(" Skipping task: NLP server is using an unexpected model setup.")
            self.summaries[0].had_errors = True
            return False

        return True

    async def read_entries(self, *, progress: rich.progress.Progress = None) -> tasks.EntryIterator:
        """Passes clinical notes through HF and returns any symptoms found"""
        http_client = httpx.AsyncClient(timeout=300)

        for docref in self.read_ndjson(progress=progress):
            can_process = nlp.is_docref_valid(docref) and self.scrubber.scrub_resource(docref, scrub_attachments=False)
            if not can_process:
                continue

            try:
                clinical_note = await fhir.get_docref_note(self.task_config.client, docref)
            except Exception as exc:  # pylint: disable=broad-except
                logging.warning("Error getting text for docref %s: %s", docref["id"], exc)
                self.summaries[0].had_errors = True
                continue

            timestamp = common.datetime_now().isoformat()

            # If you change this prompt, consider updating task_version.
            system_prompt = "You will be given a clinical note, and you should reply with a short summary of that note."
            user_prompt = clinical_note

            summary = await nlp.cache_wrapper(
                self.task_config.dir_phi,
                f"{self.name}_v{self.task_version}",
                user_prompt,
                nlp.llama2_prompt,
                system_prompt,
                user_prompt,
                client=http_client,
            )

            # Debugging
            # logging.warning("\n\n\n\n" "**********************************************************")
            # logging.warning(user_prompt)
            # logging.warning("==========================================================")
            # logging.warning(summary)
            # logging.warning("**********************************************************")

            yield {
                "id": docref["id"],  # just copy the docref
                "docref_id": docref["id"],
                "summary": summary,
                "generated_on": timestamp,
                "task_version": self.task_version,
            }

    @classmethod
    def get_schema(cls, formatter: formats.Format, rows: list[dict]) -> pyarrow.Schema:
        return pyarrow.schema(
            [
                pyarrow.field("id", pyarrow.string()),
                pyarrow.field("docref_id", pyarrow.string()),
                pyarrow.field("generated_on", pyarrow.string()),
                pyarrow.field("task_version", pyarrow.int32()),
                pyarrow.field("summary", pyarrow.string()),
            ]
        )
