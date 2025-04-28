"""Base NLP task support"""

import copy
import logging
import os
import sys
from collections.abc import Callable
from typing import ClassVar

import rich.progress

from cumulus_etl import common, errors, fhir, nlp, store
from cumulus_etl.etl.tasks.base import EtlTask, OutputTable


class BaseNlpTask(EtlTask):
    """Base class for any clinical-notes-based NLP task."""

    resource: ClassVar = "DocumentReference"
    needs_bulk_deid: ClassVar = False

    # You may want to override these in your subclass
    outputs: ClassVar = [
        # maybe add a group_field? (remember to call self.seen_docrefs.add() if so)
        OutputTable(resource_type=None)
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
    ) -> (dict, dict, str):
        """
        Iterate through clinical notes.

        :returns: a tuple of original-docref, scrubbed-docref, and clinical note
        """
        warned_connection_error = False

        for docref in self.read_ndjson(progress=progress):
            orig_docref = copy.deepcopy(docref)
            can_process = (
                nlp.is_docref_valid(docref)
                and (doc_check is None or doc_check(docref))
                and self.scrubber.scrub_resource(docref, scrub_attachments=False, keep_stats=False)
            )
            if not can_process:
                continue

            try:
                clinical_note = await fhir.get_clinical_note(self.task_config.client, docref)
            except errors.FhirConnectionConfigError as exc:
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
