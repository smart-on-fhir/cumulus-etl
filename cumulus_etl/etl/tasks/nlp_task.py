"""Base NLP task support"""

import copy
import dataclasses
import json
import logging
import os
import re
import string
import sys
import types
import typing
from collections.abc import AsyncGenerator, AsyncIterator, Callable
from typing import ClassVar

import cumulus_fhir_support as cfs
import pyarrow
import pydantic
import rich.progress
import rich.table

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
        self.seen_groups = set()

    def pop_current_group_values(self, table_index: int) -> set[str]:
        values = self.seen_groups
        self.seen_groups = set()
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


@dataclasses.dataclass(kw_only=True)
class NoteDetails:
    note_ref: str
    encounter_id: str
    subject_ref: str

    note_text: str
    note: dict

    orig_note_ref: str
    orig_note_text: str
    orig_note: dict


class BaseModelTask(BaseNlpTask):
    """Base class for any NLP task talking to LLM models."""

    outputs: ClassVar = [
        tasks.OutputTable(
            resource_type=None, uniqueness_fields={"note_ref"}, group_field="note_ref"
        )
    ]

    # If you change these prompts, consider updating task_version.
    system_prompt: str = None
    user_prompt: str = None
    client_class: type[nlp.Model] = None
    response_format: type[pydantic.BaseModel] = None

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.model = self.client_class(max_batch_count=self.task_config.batch_size)

    @classmethod
    async def init_check(cls) -> None:
        await cls.client_class().post_init_check()

    async def read_entries(self, *, progress: rich.progress.Progress = None) -> tasks.EntryIterator:
        cache_namespace = f"{self.name}_v{self.task_version}"

        # Step 1: if model supports batching, we create batches and send them off
        async def prompt_iter() -> AsyncGenerator[nlp.Prompt]:
            async for note_details in self.prepared_notes():
                yield self.make_prompt(note_details)

        await self.model.prompt_via_batches_if_possible(
            schema=self.response_format,
            cache_dir=self.task_config.dir_phi,
            cache_namespace=cache_namespace,
            prompt_iter=prompt_iter(),
        )

        # Step 2: go through notes, processing as we go (getting batch results above from cache)
        async for details in self.prepared_notes(progress=progress):
            try:
                if result := await self.process_note(details):
                    yield result
            except Exception as exc:
                logging.warning(f"NLP failed for {details.orig_note_ref}: {exc}")
                self.add_error(details.orig_note)

    async def prepared_notes(
        self,
        *,
        progress: rich.progress.Progress = None,
    ) -> AsyncGenerator[NoteDetails]:
        async for orig_note, note, orig_note_text in self.read_notes(progress=progress):
            try:
                note_ref, encounter_id, subject_ref = nlp.get_note_info(note)
            except KeyError as exc:
                logging.warning(exc)
                self.add_error(orig_note)
                continue

            note_text = self.remove_trailing_whitespace(orig_note_text)
            orig_note_ref = f"{orig_note['resourceType']}/{orig_note['id']}"

            yield NoteDetails(
                note_ref=note_ref,
                encounter_id=encounter_id,
                subject_ref=subject_ref,
                note_text=note_text,
                note=note,
                orig_note_ref=orig_note_ref,
                orig_note_text=orig_note_text,
                orig_note=orig_note,
            )

    def make_prompt(self, details: NoteDetails) -> nlp.Prompt:
        return nlp.Prompt(
            system=self.get_system_prompt(),
            user=self.get_user_prompt(details.note_text),
            schema=self.response_format,
            cache_dir=self.task_config.dir_phi,
            cache_namespace=f"{self.name}_v{self.task_version}",
            cache_checksum=nlp.cache_checksum(details.note_text),
        )

    # May be overridden if necessary by subclasses
    async def process_note(self, details: NoteDetails) -> tasks.EntryBundle | None:
        response = await self.model.prompt(self.make_prompt(details))

        parsed = response.answer.model_dump(mode="json")
        self.post_process(parsed, details)

        return {
            "note_ref": details.note_ref,
            "encounter_ref": f"Encounter/{details.encounter_id}",
            "subject_ref": details.subject_ref,
            # Since this date is stored as a string, use UTC time for easy comparisons
            "generated_on": common.datetime_now().isoformat(),
            "task_version": self.task_version,
            "system_fingerprint": response.fingerprint,
            "result": parsed,
        }

    def finish_task(self) -> None:
        stats = self.model.stats
        rich.print("\n Token usage:")
        table = rich.table.Table("", "", box=None, show_header=False)
        table.add_row(" New input tokens:", f"{stats.new_input_tokens:,}")
        table.add_row(" Input tokens read from cache:", f"{stats.cache_read_input_tokens:,}")
        if stats.cache_written_input_tokens:
            # This stat is only relevant for bedrock, so only show it if it's used
            table.add_row(
                " Input tokens written to cache:", f"{stats.cache_written_input_tokens:,}"
            )
        table.add_row(" Output tokens:", f"{stats.output_tokens:,}")

        # Estimate cost, if provided
        if prices := self.model.prices:
            cost = (
                stats.new_input_tokens * prices.new_input_tokens
                + stats.cache_read_input_tokens * prices.cache_read_input_tokens
                + stats.cache_written_input_tokens * prices.cache_written_input_tokens
                + stats.output_tokens * prices.output_tokens
            )
            cost /= 1_000  # all prices are "per 1,000 tokens"
            cost *= prices.multiplier
            when = prices.date.strftime("%b %Y")
            table.add_row(f" Estimated cost (as of {when}):", f"${cost:.2f}")

        rich.get_console().print(table)

    @classmethod
    def get_system_prompt(cls) -> str:
        return cls.system_prompt.replace(
            "%JSON-SCHEMA%", json.dumps(cls.response_format.model_json_schema())
        )

    def get_user_prompt(self, note_text: str) -> str:
        prompt = self.user_prompt or "%CLINICAL-NOTE%"
        return prompt.replace("%CLINICAL-NOTE%", note_text)

    def post_process(self, parsed: dict, details: NoteDetails) -> None:
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


class BaseModelTaskWithSpans(BaseModelTask):
    """
    When the response includes spans, we need to do extra processing.

    1. We need to convert the text spans into integer spans, to avoid PHI hitting Athena.
    2. We need to ensure the pyarrow schema shows a list of ints not strings for spans.

    It assumes any field named "spans" in the hierarchy of the pydantic model should be converted.
    """

    def post_process(self, parsed: dict, details: NoteDetails) -> None:
        if not self._process_dict(parsed, details):
            self.add_error(details.orig_note)

    def _process_dict(self, parsed: dict, details: NoteDetails) -> bool:
        """Returns False if any span couldn't be matched"""
        all_found = True

        for key, value in parsed.items():
            if key != "spans":
                # descend as needed
                if isinstance(value, dict):
                    all_found &= self._process_dict(value, details)
                if isinstance(value, list) and value and isinstance(value[0], dict):
                    all_found &= all([self._process_dict(v, details) for v in value])
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
                for match in re.finditer(span, details.orig_note_text, re.IGNORECASE):
                    found = True
                    new_spans.append(match.span())
                if not found:
                    all_found = False
                    logging.warning(
                        "Could not match span received from NLP server for "
                        f"{details.orig_note_ref}: {orig_span}"
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
            elif isinstance(field.type, pyarrow.ListType):
                sub_type = field.type.value_type
                if isinstance(sub_type, pyarrow.StructType):
                    children[index] = field.with_type(pyarrow.list_(cls._convert_schema(sub_type)))

        return pyarrow.struct(children)
