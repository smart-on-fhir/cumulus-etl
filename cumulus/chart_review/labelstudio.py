"""LabelStudio document annotation"""

from typing import Collection, Dict, Iterable, List, Tuple

import ctakesclient.typesystem
import label_studio_sdk
import label_studio_sdk.data_manager as lsdm
from ctakesclient.typesystem import MatchText

from cumulus import errors


###############################################################################
#
# LabelStudio : Document Annotation
#
###############################################################################


class LabelStudioNote:
    def __init__(self, ref_id: str, text: str):
        self.ref_id = ref_id
        self.text = text
        self.matches: List[ctakesclient.typesystem.MatchText] = []


class LabelStudioClient:
    """Client to talk to Label Studio"""

    def __init__(self, url: str, api_key: str, project_id: int, cui_labels: Dict[str, str]):
        self._client = label_studio_sdk.Client(url, api_key)
        self._client.check_connection()
        self._project = self._client.get_project(project_id)
        self._labels_name, self._labels_config = self._get_labels_config()
        self._cui_labels = dict(cui_labels)

    def push_tasks(self, notes: Collection[LabelStudioNote], *, overwrite: bool = False) -> None:
        # Get any existing tasks that we might be updating
        ref_ids = [note.ref_id for note in notes]
        ref_id_filter = lsdm.Filters.create(
            lsdm.Filters.AND,
            [lsdm.Filters.item(lsdm.Column.data("ref_id"), lsdm.Operator.IN_LIST, lsdm.Type.List, ref_ids)],
        )
        existing_tasks = self._project.get_tasks(filters=ref_id_filter)
        new_task_count = len(notes) - len(existing_tasks)

        # Should we delete existing entries?
        if existing_tasks:
            if overwrite:
                print(f"  Overwriting {len(existing_tasks)} existing tasks")
                self._project.delete_tasks([t["id"] for t in existing_tasks])
            else:
                print(f"  Skipping {len(existing_tasks)} existing tasks")
                existing_ref_ids = {t["data"]["ref_id"] for t in existing_tasks}
                notes = [note for note in notes if note.ref_id not in existing_ref_ids]

        # OK, import away!
        if notes:
            self._project.import_tasks([self._format_task_for_note(note) for note in notes])
            if new_task_count:
                print(f"  Imported {new_task_count} new tasks")

    def _get_labels_config(self) -> Tuple[str, dict]:
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
                "ref_id": note.ref_id,
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
            # TODO: add this quirk to our eventual documentation for this feature.
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
