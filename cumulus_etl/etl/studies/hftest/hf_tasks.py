"""Define tasks for the hftest study"""

import httpx
import pyarrow
import rich.progress

from cumulus_etl import common, errors, nlp
from cumulus_etl.etl import tasks


class HuggingFaceTestTask(tasks.BaseNlpTask):
    """Hugging Face Test study task, to generate a summary from text"""

    name = "hftest__summary"

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

    @classmethod
    async def init_check(cls) -> None:
        try:
            raw_info = await nlp.hf_info()
        except errors.NetworkError:
            errors.fatal(
                "Llama2 NLP server is unreachable.\n Try running 'docker compose up llama2 --wait'.",
                errors.SERVICE_MISSING,
            )

        # Sanity check a few of the properties, to make sure we don't accidentally get pointed at an unexpected model.
        expected_info_present = (
            raw_info.get("model_id") == "meta-llama/Llama-2-13b-chat-hf"
            and raw_info.get("model_sha") == "0ba94ac9b9e1d5a0037780667e8b219adde1908c"
            and raw_info.get("sha") == "09eca6422788b1710c54ee0d05dd6746f16bb681"
        )
        if not expected_info_present:
            errors.fatal(
                "LLama2 NLP server is using an unexpected model setup.",
                errors.SERVICE_MISSING,
            )

    async def read_entries(self, *, progress: rich.progress.Progress = None) -> tasks.EntryIterator:
        """Passes clinical notes through HF and returns any symptoms found"""
        http_client = httpx.AsyncClient(timeout=300)

        async for _, docref, clinical_note in self.read_notes(progress=progress):
            timestamp = common.datetime_now().isoformat()

            # If you change this prompt, consider updating task_version.
            system_prompt = "You will be given a clinical note, and you should reply with a short summary of that note."
            user_prompt = clinical_note

            summary = await nlp.cache_wrapper(
                self.task_config.dir_phi,
                f"{self.name}_v{self.task_version}",
                clinical_note,
                lambda x: x,  # from file: just store the string
                lambda x: x,  # to file: just read it back
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
    def get_schema(cls, resource_type: str | None, rows: list[dict]) -> pyarrow.Schema:
        return pyarrow.schema(
            [
                pyarrow.field("id", pyarrow.string()),
                pyarrow.field("docref_id", pyarrow.string()),
                pyarrow.field("generated_on", pyarrow.string()),
                pyarrow.field("task_version", pyarrow.int32()),
                pyarrow.field("summary", pyarrow.string()),
            ]
        )
