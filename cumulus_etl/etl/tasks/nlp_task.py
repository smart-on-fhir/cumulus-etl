"""Base NLP task support"""

import copy
import logging
import os
import re
import string
import sys
import typing
from collections.abc import AsyncIterator, Callable
from typing import ClassVar

import cumulus_fhir_support as cfs
import openai
import pyarrow
import pydantic
import rich.progress
from openai.types import chat

from cumulus_etl import common, fhir, nlp, store
from cumulus_etl.etl import tasks

ESCAPED_WHITESPACE = re.compile(r"(\\\s)+")
TRAILING_WHITESPACE = re.compile(r"\s+$", flags=re.MULTILINE)


class BaseNlpTask(tasks.EtlTask):
    """Base class for any clinical-notes-based NLP task."""

    resource: ClassVar = "DocumentReference"
    needs_bulk_deid: ClassVar = False

    # You may want to override these in your subclass
    outputs: ClassVar = [
        # maybe add a group_field? (remember to call self.seen_docrefs.add() if so)
        tasks.OutputTable(resource_type=None)
    ]
    tags: ClassVar = {"gpu"}  # maybe a study identifier?

    # Task Version
    # The "task_version" field is a simple integer that gets incremented any time an NLP-relevant parameter is changed.
    # This is a reference to a bundle of metadata (model revision, container revision, prompt string).
    # We could combine all that info into a field we save with the results. But it's more human-friendly to have a
    # simple version to refer to.
    #
    # CONSIDERATIONS WHEN CHANGING THIS:
    # - Record the new bundle of metadata in your class documentation
    # - Update any safety checks in prepare_task() or elsewhere that check the NLP versioning
    # - Be aware that your caching will be reset
    task_version: ClassVar = 1
    # Task Version History:
    # ** 1 (20xx-xx): First version **
    #   CHANGE ME

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.seen_docrefs = set()

    def pop_current_group_values(self, table_index: int) -> set[str]:
        values = self.seen_docrefs
        self.seen_docrefs = set()
        return values

    def add_error(self, docref: dict) -> None:
        self.summaries[0].had_errors = True

        if not self.task_config.dir_errors:
            return
        error_root = store.Root(os.path.join(self.task_config.dir_errors, self.name), create=True)
        error_path = error_root.joinpath("nlp-errors.ndjson")
        with common.NdjsonWriter(error_path, append=True) as writer:
            writer.write(docref)

    async def read_notes(
        self,
        *,
        doc_check: Callable[[dict], bool] | None = None,
        progress: rich.progress.Progress = None,
    ) -> AsyncIterator[tuple[dict, dict, str]]:
        """
        Iterate through clinical notes.

        :returns: a tuple of original-docref, scrubbed-docref, and clinical note
        """
        warned_connection_error = False

        note_filter = self.task_config.resource_filter or nlp.is_docref_valid

        for docref in self.read_ndjson(progress=progress):
            orig_docref = copy.deepcopy(docref)
            can_process = (
                note_filter(self.scrubber.codebook, docref)
                and (doc_check is None or doc_check(docref))
                and self.scrubber.scrub_resource(docref, scrub_attachments=False, keep_stats=False)
            )
            if not can_process:
                continue

            try:
                clinical_note = await fhir.get_clinical_note(self.task_config.client, docref)
            except cfs.BadAuthArguments as exc:
                if not warned_connection_error:
                    # Only warn user about a misconfiguration once per task.
                    # It's not fatal because it might be intentional (partially inlined DocRefs
                    # and the other DocRefs are known failures - BCH hits this with Cerner data).
                    print(exc, file=sys.stderr)
                    warned_connection_error = True
                self.add_error(orig_docref)
                continue
            except Exception as exc:
                logging.warning("Error getting text for docref %s: %s", docref["id"], exc)
                self.add_error(orig_docref)
                continue

            yield orig_docref, docref, clinical_note

    @staticmethod
    def remove_trailing_whitespace(note: str) -> str:
        """Sometimes NLP can be mildly confused by trailing whitespace, so this removes it"""
        return TRAILING_WHITESPACE.sub("", note)


class BaseOpenAiTask(BaseNlpTask):
    """Base class for any NLP task talking to OpenAI."""

    outputs: ClassVar = [tasks.OutputTable(resource_type=None, uniqueness_fields={"note_ref"})]

    # If you change these prompts, consider updating task_version.
    system_prompt: ClassVar = None
    user_prompt: ClassVar = None
    client_class: ClassVar = None
    response_format: ClassVar = None

    @classmethod
    async def init_check(cls) -> None:
        await cls.client_class.pre_init_check()
        await cls.client_class().post_init_check()

    async def read_entries(self, *, progress: rich.progress.Progress = None) -> tasks.EntryIterator:
        client = self.client_class()

        async for orig_docref, docref, orig_clinical_note in self.read_notes(progress=progress):
            try:
                docref_id, encounter_id, subject_id = nlp.get_docref_info(docref)
            except KeyError as exc:
                logging.warning(exc)
                self.add_error(orig_docref)
                continue

            clinical_note = self.remove_trailing_whitespace(orig_clinical_note)

            try:
                completion_class = chat.ParsedChatCompletion[self.response_format]
                response = await nlp.cache_wrapper(
                    self.task_config.dir_phi,
                    f"{self.name}_v{self.task_version}",
                    clinical_note,
                    lambda x: completion_class.model_validate_json(x),  # from file
                    lambda x: x.model_dump_json(  # to file
                        indent=None, round_trip=True, exclude_unset=True, by_alias=True
                    ),
                    client.prompt,
                    self.system_prompt,
                    self.get_user_prompt(clinical_note),
                    self.response_format,
                )
            except openai.APIError as exc:
                logging.warning(
                    f"Could not connect to NLP server for DocRef {orig_docref['id']}: {exc}"
                )
                self.add_error(orig_docref)
                continue
            except pydantic.ValidationError as exc:
                logging.warning(
                    f"Could not process answer from NLP server for DocRef {orig_docref['id']}: {exc}"
                )
                self.add_error(orig_docref)
                continue

            choice = response.choices[0]

            if choice.finish_reason != "stop" or not choice.message.parsed:
                logging.warning(
                    f"NLP server response didn't complete for DocRef {orig_docref['id']}: "
                    f"{choice.finish_reason}"
                )
                self.add_error(orig_docref)
                continue

            parsed = choice.message.parsed.model_dump(mode="json")
            self.post_process(parsed, orig_clinical_note, orig_docref)

            yield {
                "note_ref": f"DocumentReference/{docref_id}",
                "encounter_ref": f"Encounter/{encounter_id}",
                "subject_ref": f"Patient/{subject_id}",
                # Since this date is stored as a string, use UTC time for easy comparisons
                "generated_on": common.datetime_now().isoformat(),
                "task_version": self.task_version,
                "system_fingerprint": response.system_fingerprint,
                "result": parsed,
            }

    @classmethod
    def get_user_prompt(cls, clinical_note: str) -> str:
        prompt = cls.user_prompt or "%CLINICAL-NOTE%"
        return prompt.replace("%CLINICAL-NOTE%", clinical_note)

    def post_process(self, parsed: dict, orig_clinical_note: str, orig_docref: dict) -> None:
        """Subclasses can fill this out if they like"""

    @classmethod
    def get_schema(cls, resource_type: str | None, rows: list[dict]) -> pyarrow.Schema:
        result_schema = cls.convert_pydantic_fields_to_pyarrow(cls.response_format.model_fields)
        return pyarrow.schema(
            [
                pyarrow.field("note_ref", pyarrow.string()),
                pyarrow.field("encounter_ref", pyarrow.string()),
                pyarrow.field("subject_ref", pyarrow.string()),
                pyarrow.field("generated_on", pyarrow.string()),
                pyarrow.field("task_version", pyarrow.int32()),
                pyarrow.field("system_fingerprint", pyarrow.string()),
                pyarrow.field("result", result_schema),
            ]
        )

    @classmethod
    def convert_pydantic_fields_to_pyarrow(
        cls, fields: dict[str, pydantic.fields.FieldInfo]
    ) -> pyarrow.DataType:
        return pyarrow.struct(
            [
                pyarrow.field(name, cls._convert_type_to_pyarrow(info.annotation))
                for name, info in fields.items()
            ]
        )

    @classmethod
    def _convert_type_to_pyarrow(cls, annotation) -> pyarrow.DataType:
        # Since we only need to handle a small amount of possible types, we just do this ourselves
        # rather than relying on an external library.
        if issubclass(annotation, str):
            return pyarrow.string()
        elif issubclass(annotation, bool):
            return pyarrow.bool_()
        elif issubclass(typing.get_origin(annotation), list):
            sub_type = typing.get_args(annotation)[0]
            # Note: does not handle struct types underneath yet
            return pyarrow.list_(cls._convert_type_to_pyarrow(sub_type))
        else:
            raise ValueError(f"Unsupported type {annotation}")  # pragma: no cover


class BaseOpenAiTaskWithSpans(BaseOpenAiTask):
    """
    When the response includes spans, we need to do extra processing.

    1. We need to convert the text spans into integer spans, to avoid PHI hitting Athena.
    2. We need to ensure the pyarrow schema shows a list of ints not strings for spans.

    It assumes the field is named "spans" in the top level of the pydantic model.
    """

    def post_process(self, parsed: dict, orig_clinical_note: str, orig_docref: dict) -> None:
        new_spans = []
        missed_some = False

        for span in parsed["spans"]:
            # Now we need to find this span in the original text.
            # However, LLMs like to mess with us, and the span is not always accurate to the
            # original text (e.g. whitespace, case, punctuation differences). So be a little fuzzy.
            orig_span = span
            span = span.strip(string.punctuation + string.whitespace)
            span = re.escape(span)
            # Replace sequences of whitespace with a whitespace regex, to allow the span returned
            # by the LLM to match regardless of what the LLM does with whitespace and to ignore
            # how we trim trailing whitespace from the original note.
            span = ESCAPED_WHITESPACE.sub(r"\\s+", span)

            found = False
            for match in re.finditer(span, orig_clinical_note, re.IGNORECASE):
                found = True
                new_spans.append(match.span())
            if not found:
                missed_some = True
                logging.warning(
                    "Could not match span received from NLP server for "
                    f"DocRef {orig_docref['id']}: {orig_span}"
                )

        if missed_some:
            self.add_error(orig_docref)

        parsed["spans"] = new_spans

    @classmethod
    def get_schema(cls, resource_type: str | None, rows: list[dict]) -> pyarrow.Schema:
        schema = super().get_schema(resource_type, rows)

        result_index = schema.get_field_index("result")
        result_struct = schema.field(result_index).type

        children = result_struct.fields
        new_spans_type = pyarrow.list_(pyarrow.list_(pyarrow.int32(), 2))
        spans_index = result_struct.get_field_index("spans")
        children[spans_index] = pyarrow.field("spans", new_spans_type)
        schema = schema.set(result_index, pyarrow.field("result", pyarrow.struct(children)))

        return schema
