"""LabelStudio document annotation"""

import dataclasses
from collections.abc import Collection, Iterable

import ctakesclient.typesystem
import label_studio_sdk
import label_studio_sdk.data_manager as lsdm
from ctakesclient.typesystem import MatchText

from cumulus_etl import errors


###############################################################################
#
# LabelStudio : Document Annotation
#
###############################################################################


@dataclasses.dataclass
class LabelStudioNote:
    """Holds all the data that Label Studio will need for a single note (or a single grouped encounter note)"""

    enc_id: str  # real Encounter ID
    anon_id: str  # anonymized Encounter ID
    text: str = ""  # text of the note, sent to Label Studio

    # A title is only used when combining notes into one big encounter note. It's not sent to Label Studio.
    title: str = ""

    # Doc mappings is a dict of real DocRef ID -> anonymized DocRef ID of all contained notes, in order
    doc_mappings: dict[str, str] = dataclasses.field(default_factory=dict)

    # Doc spans indicate which bits of the text come from which DocRef - it will map real DocRef ID to a pair of
    # "first character" (0-based) and "last character" (0-based, exclusive) - just like cTAKES match text spans.
    doc_spans: dict[str, tuple[int, int]] = dataclasses.field(default_factory=dict)

    # Matches found by cTAKES
    matches: list[ctakesclient.typesystem.MatchText] = dataclasses.field(default_factory=list)


class LabelStudioClient:
    """Client to talk to Label Studio"""

    def __init__(self, url: str, api_key: str, project_id: int, cui_labels: dict[str, str]):
        self._client = label_studio_sdk.Client(url, api_key)
        self._client.check_connection()
        self._project = self._client.get_project(project_id)
        self._labels_name, self._labels_config = self._get_labels_config()
        self._cui_labels = dict(cui_labels)

    def push_tasks(self, notes: Collection[LabelStudioNote], *, overwrite: bool = False) -> None:
        # Get any existing tasks that we might be updating
        enc_ids = [note.enc_id for note in notes]
        enc_id_filter = lsdm.Filters.create(
            lsdm.Filters.AND,
            [lsdm.Filters.item(lsdm.Column.data("enc_id"), lsdm.Operator.IN_LIST, lsdm.Type.List, enc_ids)],
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
                notes = [note for note in notes if note.enc_id not in existing_enc_ids]

        # OK, import away!
        if notes:
            self._project.import_tasks([self._format_task_for_note(note) for note in notes])
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
            "Could not find a Labels or Choices config in the Label Studio project.", errors.LABEL_STUDIO_CONFIG_INVALID
        )

    def _format_task_for_note(self, note: LabelStudioNote) -> dict:
        task = {
            "data": {
                "text": note.text,
                "enc_id": note.enc_id,
                "anon_id": note.anon_id,
                "docref_mappings": note.doc_mappings,
                "docref_spans": {k: list(v) for k, v in note.doc_spans.items()},  # json doesn't natively have tuples
            },
        }

        self._format_prediction(task, note)

        return task

    def _format_prediction(self, task: dict, note: LabelStudioNote) -> None:
        prediction = {
            "model_version": "Cumulus",  # TODO: do we want more specificity here?
        }

        used_labels = set()
        results = []
        count = 0
        for match in note.matches:
            matched_labels = {self._cui_labels.get(concept.cui) for concept in match.conceptAttributes}
            matched_labels.discard(None)  # drop the result of a concept not being in our bsv label set
            if matched_labels:
                results.append(self._format_match(count, match, matched_labels))
                used_labels.update(matched_labels)
                count += 1
        prediction["result"] = results

        if self._labels_config.get("dynamic_labels"):
            # This path supports configs like <Labels name="label" toName="text" value="$label"/> where the labels
            # can be dynamically set by us.
            #
            # Unfortunately, the variable name for value= (which is what we need to use for the key in data[]) is not
            # actually kept in the config, so we have to make some assumptions about how the user set up their project.
            #
            # The rule that Cumulus uses is that the value= variable must equal the name= of the <Labels> element.
            task["data"][self._labels_name] = [{"value": x} for x in sorted(used_labels)]

        task["predictions"] = [prediction]

    def _format_match(self, count: int, match: MatchText, labels: Iterable[str]) -> dict:
        return {
            "id": f"match{count}",
            "from_name": self._labels_name,
            "to_name": self._labels_config["to_name"][0],
            "type": "labels",
            "value": {"start": match.begin, "end": match.end, "score": 1.0, "text": match.text, "labels": list(labels)},
        }
