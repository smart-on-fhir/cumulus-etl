"""Base NLP task support"""

import copy
import logging
import os
import re
import string
import sys
import types
import typing
from collections.abc import AsyncIterator, Callable
from typing import ClassVar

import cumulus_fhir_support as cfs
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

    resource: ClassVar = {"DiagnosticReport", "DocumentReference"}
    needs_bulk_deid: ClassVar = False

    # You may want to override these in your subclass
    outputs: ClassVar = [
        # maybe add a group_field? (remember to call self.seen_docrefs.add() if so)
        tasks.OutputTable(resource_type=None)
    ]

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

        :returns: a tuple of original-resource, scrubbed-resource, and note text
        """
        warned_connection_error = False

        note_filter = self.task_config.resource_filter or nlp.is_note_valid

        for note in self.read_ndjson(progress=progress):
            orig_note = copy.deepcopy(note)
            can_process = (
                await note_filter(self.scrubber.codebook, note)
                and (doc_check is None or doc_check(note))
                and self.scrubber.scrub_resource(note, scrub_attachments=False, keep_stats=False)
            )
            if not can_process:
                continue

            try:
                note_text = await fhir.get_clinical_note(self.task_config.client, note)
            except cfs.BadAuthArguments as exc:
                if not warned_connection_error:
                    # Only warn user about a misconfiguration once per task.
                    # It's not fatal because it might be intentional (partially inlined DocRefs
                    # and the other DocRefs are known failures - BCH hits this with Cerner data).
                    print(exc, file=sys.stderr)
                    warned_connection_error = True
                self.add_error(orig_note)
                continue
            except Exception as exc:
                orig_note_ref = f"{orig_note['resourceType']}/{orig_note['id']}"
                logging.warning("Error getting text for note %s: %s", orig_note_ref, exc)
                self.add_error(orig_note)
                continue

            yield orig_note, note, note_text

    @staticmethod
    def remove_trailing_whitespace(note_text: str) -> str:
        """Sometimes NLP can be mildly confused by trailing whitespace, so this removes it"""
        return TRAILING_WHITESPACE.sub("", note_text)


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
        await cls.client_class().post_init_check()

    async def read_entries(self, *, progress: rich.progress.Progress = None) -> tasks.EntryIterator:
        client = self.client_class()

        async for orig_note, note, orig_note_text in self.read_notes(progress=progress):
            try:
                note_ref, encounter_id, subject_id = nlp.get_note_info(note)
            except KeyError as exc:
                logging.warning(exc)
                self.add_error(orig_note)
                continue

            note_text = self.remove_trailing_whitespace(orig_note_text)
            orig_note_ref = f"{orig_note['resourceType']}/{orig_note['id']}"

            try:
                completion_class = chat.ParsedChatCompletion[self.response_format]
                response = await nlp.cache_wrapper(
                    self.task_config.dir_phi,
                    f"{self.name}_v{self.task_version}",
                    note_text,
                    lambda x: completion_class.model_validate_json(x),  # from file
                    lambda x: x.model_dump_json(  # to file
                        indent=None, round_trip=True, exclude_unset=True, by_alias=True
                    ),
                    client.prompt,
                    self.system_prompt,
                    self.get_user_prompt(note_text),
                    self.response_format,
                )
            except Exception as exc:
                logging.warning(f"NLP failed for {orig_note_ref}: {exc}")
                self.add_error(orig_note)
                continue

            choice = response.choices[0]

            if choice.finish_reason != "stop" or not choice.message.parsed:
                logging.warning(
                    f"NLP server response didn't complete for {orig_note_ref}: "
                    f"{choice.finish_reason}"
                )
                self.add_error(orig_note)
                continue

            parsed = choice.message.parsed.model_dump(mode="json")
            self.post_process(parsed, orig_note_text, orig_note)

            yield {
                "note_ref": note_ref,
                "encounter_ref": f"Encounter/{encounter_id}",
                "subject_ref": f"Patient/{subject_id}",
                # Since this date is stored as a string, use UTC time for easy comparisons
                "generated_on": common.datetime_now().isoformat(),
                "task_version": self.task_version,
                "system_fingerprint": response.system_fingerprint,
                "result": parsed,
            }

    @classmethod
    def get_user_prompt(cls, note_text: str) -> str:
        prompt = cls.user_prompt or "%CLINICAL-NOTE%"
        return prompt.replace("%CLINICAL-NOTE%", note_text)

    def post_process(self, parsed: dict, orig_note_text: str, orig_note: dict) -> None:
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
                pyarrow.field(name, cls._convert_type_to_pyarrow(info.annotation), nullable=True)
                for name, info in fields.items()
            ]
        )

    @classmethod
    def _convert_type_to_pyarrow(cls, annotation) -> pyarrow.DataType:
        # Since we only need to handle a small amount of possible types, we just do this ourselves
        # rather than relying on an external library.
        if origin := typing.get_origin(annotation):  # e.g. "UnionType" or "list"
            sub_type = typing.get_args(annotation)[0]
            if issubclass(origin, types.UnionType):
                # This is gonna be something like "str | None" so just grab first arg.
                # We mark all our fields are nullable at the pyarrow layer.
                return cls._convert_type_to_pyarrow(sub_type)
            elif issubclass(origin, list):
                # Note: does not handle struct types underneath yet
                return pyarrow.list_(cls._convert_type_to_pyarrow(sub_type))
        elif issubclass(annotation, str):
            return pyarrow.string()
        elif issubclass(annotation, bool):
            return pyarrow.bool_()
        elif issubclass(annotation, int):
            return pyarrow.int32()
        elif issubclass(annotation, pydantic.BaseModel):
            return cls.convert_pydantic_fields_to_pyarrow(annotation.model_fields)

        raise ValueError(f"Unsupported type {annotation}")  # pragma: no cover


class BaseOpenAiTaskWithSpans(BaseOpenAiTask):
    """
    When the response includes spans, we need to do extra processing.

    1. We need to convert the text spans into integer spans, to avoid PHI hitting Athena.
    2. We need to ensure the pyarrow schema shows a list of ints not strings for spans.

    It assumes any field named "spans" in the hierarchy of the pydantic model should be converted.
    """

    def post_process(self, parsed: dict, orig_note_text: str, orig_note: dict) -> None:
        if not self._process_dict(parsed, orig_note_text, orig_note):
            self.add_error(orig_note)

    def _process_dict(self, parsed: dict, orig_note_text: str, orig_note: dict) -> bool:
        """Returns False if any span couldn't be matched"""
        all_found = True

        for key, value in parsed.items():
            if key != "spans":
                if isinstance(value, dict):
                    all_found &= self._process_dict(value, orig_note_text, orig_note)  # descend
                continue

            new_spans = []
            for span in value:
                # Now we need to find this span in the original text.
                # However, LLMs like to mess with us, and the span is not always accurate to the
                # original text (e.g. whitespace, case, punctuation differences).
                # So be a little fuzzy.
                orig_span = span
                span = span.strip(string.punctuation + string.whitespace)
                span = re.escape(span)
                # Replace sequences of whitespace with a whitespace regex, to allow the span
                # returned by the LLM to match regardless of what the LLM does with whitespace and
                # to ignore how we trim trailing whitespace from the original note.
                span = ESCAPED_WHITESPACE.sub(r"\\s+", span)

                found = False
                for match in re.finditer(span, orig_note_text, re.IGNORECASE):
                    found = True
                    new_spans.append(match.span())
                if not found:
                    all_found = False
                    logging.warning(
                        "Could not match span received from NLP server for "
                        f"{orig_note['resourceType']}/{orig_note['id']}: {orig_span}"
                    )

            parsed[key] = new_spans

        return all_found

    @classmethod
    def get_schema(cls, resource_type: str | None, rows: list[dict]) -> pyarrow.Schema:
        """Convert lists of string textual spans into lists of index pairs"""
        schema = super().get_schema(resource_type, rows)

        result_index = schema.get_field_index("result")
        result_struct = schema.field(result_index).type
        result_struct = cls._convert_schema(result_struct)

        schema = schema.set(result_index, pyarrow.field("result", result_struct))
        return schema

    @classmethod
    def _convert_schema(cls, schema: pyarrow.StructType) -> pyarrow.StructType:
        children = schema.fields  # copy field list

        for index in range(schema.num_fields):
            field = children[index]
            if field.name == "spans":
                new_spans_type = pyarrow.list_(pyarrow.list_(pyarrow.int32(), 2))
                children[index] = field.with_type(new_spans_type)
            elif isinstance(field.type, pyarrow.StructType):
                children[index] = field.with_type(cls._convert_schema(field.type))

        return pyarrow.struct(children)
