"""Define tasks for the hftest study"""

import pyarrow
import rich.progress

from cumulus_etl import common, nlp
from cumulus_etl.etl import tasks


class HuggingFaceTestTask(tasks.BaseNlpTask):
    """Hugging Face Test study task, to generate a summary from text"""

    name = "hftest__summary"

    task_version = 0
    # Task Version History:
    # ** 0 **
    #   This is fluid until we actually promote this to a real task - feel free to update without
    #   bumping the version.
    #   container: vllm/vllm-openai
    #   container revision: v0.10.0
    #   container properties:
    #     QUANTIZE=bitsandbytes
    #   model: meta-llama/Llama-2-13b-chat-hf
    #   model revision: a2cb7a712bb6e5e736ca7f8cd98167f81a0b5bd8
    #   system prompt:
    #     "You will be given a clinical note, and you should reply with a short summary of that
    #     note."
    #   user prompt: a clinical note

    @classmethod
    async def init_check(cls) -> None:
        await nlp.Llama2Model().check()

    async def read_entries(self, *, progress: rich.progress.Progress = None) -> tasks.EntryIterator:
        """Passes clinical notes through HF and returns any symptoms found"""
        client = nlp.Llama2Model()

        async for _, docref, clinical_note in self.read_notes(progress=progress):
            timestamp = common.datetime_now().isoformat()

            # If you change this prompt, consider updating task_version.
            system_prompt = (
                "You will be given a clinical note, "
                "and you should reply with a short summary of that note."
            )
            user_prompt = clinical_note

            summary = await nlp.cache_wrapper(
                self.task_config.dir_phi,
                f"{self.name}_v{self.task_version}",
                clinical_note,
                lambda x: x,  # from file: just store the string
                lambda x: x,  # to file: just read it back
                client.prompt,
                system_prompt,
                user_prompt,
            )

            # Debugging
            # logging.warning("\n\n\n\n" "********************************************************")
            # logging.warning(user_prompt)
            # logging.warning("========================================================")
            # logging.warning(summary)
            # logging.warning("********************************************************")

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
