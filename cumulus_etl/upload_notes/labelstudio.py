"""LabelStudio document annotation"""

import dataclasses
import datetime
from collections.abc import Collection, Iterable

import ctakesclient
import label_studio_sdk
import label_studio_sdk.data_manager as lsdm

from cumulus_etl import batching, errors

###############################################################################
#
# LabelStudio : Document Annotation
#
###############################################################################


@dataclasses.dataclass
class LabelStudioNote:
    """Holds all the data that Label Studio will need for a single note (or a single grouped encounter note)"""

    patient_id: str
    anon_patient_id: str
    encounter_id: str  # real Encounter ID
    anon_encounter_id: str  # anonymized Encounter ID
    text: str = ""  # text of the note, sent to Label Studio
    date: datetime.datetime | None = None  # date of the note

    # A title is only used when combining notes into one big encounter note. It's not sent to Label Studio.
    title: str = ""

    # Doc mappings is a dict of real DocRef ID -> anonymized DocRef ID of all contained notes, in order
    doc_mappings: dict[str, str] = dataclasses.field(default_factory=dict)

    # Doc spans indicate which bits of the text come from which DocRef - it will map real DocRef ID to a pair of
    # "first character" (0-based) and "last character" (0-based, exclusive) - just like cTAKES match text spans.
    doc_spans: dict[str, tuple[int, int]] = dataclasses.field(default_factory=dict)

    # Matches found by cTAKES
    ctakes_matches: list[ctakesclient.typesystem.MatchText] = dataclasses.field(
        default_factory=list
    )

    # Matches found by word search, list of found spans
    highlights: list[ctakesclient.typesystem.Span] = dataclasses.field(default_factory=list)

    # Matches found by Philter
    philter_map: dict[int, int] = dataclasses.field(default_factory=dict)


class LabelStudioClient:
    """Client to talk to Label Studio"""

    def __init__(self, url: str, api_key: str, project_id: int, cui_labels: dict[str, str]):
        self._client = label_studio_sdk.Client(url, api_key)
        self._client.check_connection()
        self._project = self._client.get_project(project_id)
        self._labels_name, self._labels_config = self._get_labels_config()
        self._cui_labels = dict(cui_labels)

    async def push_tasks(
        self, notes: Collection[LabelStudioNote], *, overwrite: bool = False
    ) -> None:
        # Get any existing tasks that we might be updating
        enc_ids = [note.encounter_id for note in notes]
        enc_id_filter = lsdm.Filters.create(
            lsdm.Filters.AND,
            [
                lsdm.Filters.item(
                    lsdm.Column.data("enc_id"), lsdm.Operator.IN_LIST, lsdm.Type.List, enc_ids
                )
            ],
        )
        existing_tasks = self._project.get_tasks(filters=enc_id_filter)
        new_task_count = len(notes) - len(existing_tasks)

        # Should we delete existing entries?
        if existing_tasks:
            if overwrite:
                print(f"  Overwriting {len(existing_tasks)} existing tasks")
                self._project.delete_tasks([t["id"] for t in existing_tasks])
            else:
                print(f"  Skipping {len(existing_tasks)} existing tasks")
                existing_enc_ids = {t["data"]["enc_id"] for t in existing_tasks}
                notes = [note for note in notes if note.encounter_id not in existing_enc_ids]

        # OK, import away!
        if notes:
            new_notes = (self._format_task_for_note(note) for note in notes)
            # Upload notes in batches, to avoid making one giant request that times out.
            # I've seen batches of 700 fail, but 600 succeed. So we give ourselves plenty of
            # headroom here and use batches of 300.
            async for batch in batching.batch_iterate(new_notes, 300):
                self._project.import_tasks(batch)
            if new_task_count:
                print(f"  Imported {new_task_count} new tasks")

    def _get_labels_config(self) -> tuple[str, dict]:
        """Finds the first <Labels> tag in the config and returns its name and values, falling back to <Choices>"""
        for k, v in self._project.parsed_label_config.items():
            if v.get("type") == "Labels":
                return k, v

        for k, v in self._project.parsed_label_config.items():
            if v.get("type") == "Choices":
                return k, v

        errors.fatal(
            "Could not find a Labels or Choices config in the Label Studio project.\n"
            "Add one in your project’s Settings → Labeling Interface page.\n"
            "If you want a basic dynamic config, use this:\n"
            "<View>\n"
            '  <Labels name="label" toName="text" value="$label"/>\n'
            '  <Text name="text" value="$text"/>\n'
            "</View>",
            errors.LABEL_STUDIO_CONFIG_INVALID,
        )

    def _format_task_for_note(self, note: LabelStudioNote) -> dict:
        task = {
            "data": {
                "text": note.text,
                "patient_id": note.patient_id,
                "anon_patient_id": note.anon_patient_id,
                # Be careful when changing this name. We use this enc_id field to determine which
                # notes to override during upload, so it's best if it doesn't change.
                "enc_id": note.encounter_id,
                "anon_enc_id": note.anon_encounter_id,
                "docref_mappings": note.doc_mappings,
                # json doesn't natively have tuples, so convert spans to lists
                "docref_spans": {k: list(v) for k, v in note.doc_spans.items()},
            },
            "predictions": [],
        }

        # Initialize any used labels in case we have a dynamic label config.
        # Label Studio needs to see *something* here
        self._update_used_labels(task, [])

        self._format_ctakes_predictions(task, note)
        self._format_highlights_predictions(task, note)
        self._format_philter_predictions(task, note)

        return task

    def _format_match(self, begin: int, end: int, text: str, labels: Iterable[str]) -> dict:
        return {
            "from_name": self._labels_name,
            "to_name": self._labels_config["to_name"][0],
            "type": "labels",
            "value": {
                "start": begin,
                "end": end,
                "score": 1.0,
                "text": text,
                "labels": list(labels),
            },
        }

    def _format_ctakes_predictions(self, task: dict, note: LabelStudioNote) -> None:
        if not note.ctakes_matches:
            return

        prediction = {
            "model_version": "Cumulus cTAKES",
        }

        used_labels = set()
        results = []
        for match in note.ctakes_matches:
            matched_labels = {
                self._cui_labels.get(concept.cui) for concept in match.conceptAttributes
            }
            # drop the result of a concept not being in our bsv label set
            matched_labels.discard(None)
            if matched_labels:
                results.append(
                    self._format_match(match.begin, match.end, match.text, matched_labels)
                )
                used_labels.update(matched_labels)
        prediction["result"] = results
        task["predictions"].append(prediction)

        self._update_used_labels(task, used_labels)

    def _format_highlights_predictions(self, task: dict, note: LabelStudioNote) -> None:
        if not note.highlights:
            return

        prediction = {
            "model_version": "Cumulus Highlights",
        }

        results = []
        for span in note.highlights:
            results.append(
                self._format_match(span.begin, span.end, note.text[span.begin : span.end], ["Tag"])
            )
        prediction["result"] = results
        task["predictions"].append(prediction)

        self._update_used_labels(task, ["Tag"])

    def _format_philter_predictions(self, task: dict, note: LabelStudioNote) -> None:
        """
        Adds a predication layer with philter spans.

        Note that this does *not* update the running list of used labels.
        This sets a "secret" / non-human-oriented label of "_philter".
        Label Studio will still highlight the spans, and this way we won't
        conflict with any existing labels.
        """
        if not note.philter_map:
            return

        prediction = {
            "model_version": "Cumulus Philter",
        }

        results = []
        for start, stop in sorted(note.philter_map.items()):
            # We hardcode the label "_philter" - Label Studio will still highlight unknown labels,
            # and this is unlikely to collide with existing labels.
            results.append(self._format_match(start, stop, note.text[start:stop], ["_philter"]))
        prediction["result"] = results

        task["predictions"].append(prediction)

    def _update_used_labels(self, task: dict, used_labels: Iterable[str]) -> None:
        # This path supports configs like <Labels name="label" toName="text" value="$label"/> where
        # the labels can be dynamically set by us. (This is still safe to do even without dynamic
        # labels - and since we can't really tell from the label config whether there are dynamic
        # labels, we just always act like there are. If not, these terms will still come in as
        # highlighted words, just without an official label attached.)
        #
        # Unfortunately, the variable name for value= (which is what we need to use for the key in
        # data[]) is not actually kept in the config, so we have to make some assumptions about how
        # the user set up their project.
        #
        # The rule that Cumulus uses is that the value= variable must equal the name= of the
        # <Labels> element.
        existing_labels = task["data"].get(self._labels_name, [])
        existing_labels = {d["value"] for d in existing_labels}
        existing_labels.update(used_labels)
        task["data"][self._labels_name] = [{"value": x} for x in sorted(existing_labels)]
