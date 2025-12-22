"""LabelStudio document annotation"""

import dataclasses
import datetime
import hashlib
import json
import math
import re
from collections.abc import AsyncIterator, Collection, Iterable

import ctakesclient
import label_studio_sdk as ls
import label_studio_sdk.data_manager as lsdm

from cumulus_etl import batching, cli_utils, errors

###############################################################################
#
# LabelStudio : Document Annotation
#
###############################################################################

NON_ALPHANUM = re.compile("[^a-z0-9]")
UNDERSCORES = re.compile("_+")


@dataclasses.dataclass
class Highlight:
    """Describes a label, a span, and some extra metadata"""

    label: str
    span: tuple[int, int]
    origin: str
    sublabel_name: str | None = None
    sublabel_value: str | None = None


@dataclasses.dataclass
class LabelStudioNote:
    """Holds all the data that Label Studio will need for a single note (or a grouped note)"""

    unique_id: str  # used to uniquely identify this note on the server
    patient_id: str
    anon_patient_id: str
    encounter_id: str | None  # real Encounter ID
    anon_encounter_id: str | None  # anonymized Encounter ID
    text: str = ""  # text of the note, sent to Label Studio
    date: datetime.datetime | None = None  # date of the note

    # A header holds some user-visible metadata to show above each note.
    # It's not sent to Label Studio.
    header: str = ""

    # Doc mappings is an ordered dict of real ID -> anonymized ID of all contained notes
    doc_mappings: dict[str, str] = dataclasses.field(default_factory=dict)

    # Doc spans indicate which bits of the text come from which note - it will map a real ID to a
    # pair of "first character" (0-based) and "last character" (0-based, exclusive) - just like
    # cTAKES match text spans.
    doc_spans: dict[str, tuple[int, int]] = dataclasses.field(default_factory=dict)

    # Matches found by cTAKES
    ctakes_matches: list[ctakesclient.typesystem.MatchText] = dataclasses.field(
        default_factory=list
    )

    # Matches found by word search or csv
    highlights: list[Highlight] = dataclasses.field(default_factory=list)

    # Matches found by Philter
    philter_map: dict[int, int] = dataclasses.field(default_factory=dict)


class LabelStudioClient:
    """Client to talk to Label Studio"""

    def __init__(self, url: str, api_key: str, project_id: int, cui_labels: dict[str, str]):
        self._client = ls.LabelStudio(base_url=url, api_key=api_key)
        self._project = self._client.projects.get(project_id)
        self._labels_name = self._get_first_labels_config_name()
        self._cui_labels = dict(cui_labels)

    async def push_tasks(
        self, notes: Collection[LabelStudioNote], *, overwrite: bool = False
    ) -> None:
        # Get any existing tasks that we might be updating
        unique_ids = {note.unique_id for note in notes}
        existing_tasks = await self._search_for_existing_tasks(unique_ids)
        new_task_count = len(notes) - len(existing_tasks)

        # Should we delete existing entries?
        if existing_tasks:
            if overwrite:
                print(f"Overwriting {len(existing_tasks):,} existing charts.")
                for task_id in existing_tasks.values():
                    self._client.tasks.delete(task_id)
            else:
                print(f"Skipping {len(existing_tasks):,} existing charts.")
                notes = [note for note in notes if note.unique_id not in existing_tasks]

        # OK, import away!
        if notes:
            new_notes = [self._format_task_for_note(note) for note in notes]
            # Upload notes in batches, to avoid making one giant request that times out.
            # I've seen batches of 700 fail, but 600 succeed. So we give ourselves plenty of
            # headroom here and use batches of 300.
            async for batch in self._batch_with_progress("Uploading charts…", new_notes, 300):
                self._client.projects.import_tasks(self._project.id, request=batch)
            if new_task_count:
                print(f"Imported {new_task_count:,} new charts.")

    async def _search_for_existing_tasks(self, unique_ids: Collection[str]) -> dict[str, int]:
        existing_tasks = []

        # Batch our requests, because if there are a lot of notes, a single search with all the
        # target IDs in it will be too large for a server's URI limits.
        # I picked 500 because in my testing, we started seeing those errors at around 1000 IDs
        # of moderate size (DocumentReference/uuid) - so I halved it and that should be safe.
        async for batch in self._batch_with_progress(
            "Searching for existing charts…", unique_ids, 500
        ):
            col = lsdm.Column.data("unique_id")
            batch_search = lsdm.Filters.item(col, lsdm.Operator.IN_LIST, lsdm.Type.List, batch)
            batch_filter = lsdm.Filters.create(lsdm.Filters.AND, [batch_search])
            results = self._client.tasks.list(
                project=self._project.id,
                query=json.dumps({"filters": batch_filter}),
                include="data,id",
            )
            existing_tasks.extend(results)

        return {t.data["unique_id"]: t.id for t in existing_tasks}

    async def _batch_with_progress(
        self, label: str, collection: Collection, batch_size: int
    ) -> AsyncIterator[list]:
        num_batches = math.ceil(len(collection) / batch_size)
        with cli_utils.make_progress_bar() as progress:
            progress_task = progress.add_task(label, total=num_batches)
            async for batch in batching.batch_iterate(collection, batch_size):
                yield batch
                progress.advance(progress_task)

    def _get_first_labels_config_name(self) -> str:
        """Finds the first labels-type tag in the config and returns its name and values"""
        tags = self._project.get_label_interface().find_tags(
            match_fn=lambda x: x.tag in {"Labels", "Choices"}
        )
        for tag in tags:
            return tag.name  # just get first one

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

    def _format_task_for_note(self, note: LabelStudioNote) -> ls.ImportApiRequest:
        task = ls.ImportApiRequest(
            data={
                "text": note.text,
                "unique_id": note.unique_id,
                "patient_id": note.patient_id,
                "anon_patient_id": note.anon_patient_id,
                "encounter_id": note.encounter_id,
                "anon_encounter_id": note.anon_encounter_id,
                "date": note.date and note.date.isoformat(),
                "docref_mappings": note.doc_mappings,
                # json doesn't natively have tuples, so convert spans to lists
                "docref_spans": {k: list(v) for k, v in note.doc_spans.items()},
            },
            predictions=[],
        )

        # Initialize any used labels in case we have a dynamic label config.
        # Label Studio needs to see *something* here
        self._update_used_labels(task, [])

        self._format_ctakes_predictions(task, note)
        self._format_highlights_predictions(task, note)
        self._format_philter_predictions(task, note)

        return task

    def _format_match(
        self,
        begin: int,
        end: int,
        text: str,
        labels: Iterable[str],
        from_name: str | None = None,
        label_id: str | None = None,
    ) -> dict:
        from_name = from_name or self._labels_name
        interface = self._project.get_label_interface()
        config_labels, _dynamic = interface.get_all_labels()
        config = from_name in config_labels and interface.get_tag(from_name)
        if not config:
            errors.fatal(f"Unrecognized label name '{from_name}'.", errors.LABEL_UNKNOWN)

        match = {
            "from_name": from_name,
            "to_name": config.to_name[0],
            "type": config.tag.casefold(),
            "value": {
                "start": begin,
                "end": end,
                "score": 1.0,
                "text": text,
            },
        }
        if label_id:
            match["id"] = label_id

        match config.tag.casefold():
            case "labels":
                field = "labels"
            case "choices":
                field = "choices"
            case "textarea":
                field = "text"
            case _:
                errors.fatal(
                    f"Unrecognized Label Studio config type '{config.tag}'.",
                    errors.LABEL_CONFIG_TYPE_UNKNOWN,
                )

        match["value"][field] = list(labels)
        return match

    def _format_ctakes_predictions(self, task: ls.ImportApiRequest, note: LabelStudioNote) -> None:
        if not note.ctakes_matches:
            return

        prediction = ls.PredictionRequest.construct(model_version="cTAKES", result=[])

        used_labels = set()
        for match in note.ctakes_matches:
            matched_labels = {
                self._cui_labels.get(concept.cui) for concept in match.conceptAttributes
            }
            # drop the result of a concept not being in our bsv label set
            matched_labels.discard(None)
            if matched_labels:
                prediction.result.append(
                    self._format_match(match.begin, match.end, match.text, matched_labels)
                )
                used_labels.update(matched_labels)
        task.predictions.append(prediction)

        self._update_used_labels(task, used_labels)

    def _format_highlights_predictions(
        self, task: ls.ImportApiRequest, note: LabelStudioNote
    ) -> None:
        # Group up the highlights by parent label.
        # Then we'll see how many sublabels it has.
        grouped_highlights = {}  # key-tuple -> sublabel name -> sublabel value list
        for highlight in note.highlights:
            key = (highlight.label, highlight.span, highlight.origin)
            sublabels = grouped_highlights.setdefault(key, {})
            sublabels.setdefault(highlight.sublabel_name, []).append(highlight.sublabel_value)

        # Also keep track of all the different sublabel values, for adding as a new data column.
        sublabel_col_vals = {}  # (label, sublabel_name) -> (sublabel value, uncased text) -> text

        predictions = {}  # dict of origin -> prediction request
        for key, sublabels in grouped_highlights.items():
            label, span, origin = key
            default_prediction = ls.PredictionRequest.construct(model_version=origin, result=[])
            prediction = predictions.setdefault(origin, default_prediction)

            label_id = "__".join(str(k) for k in key)
            label_id = hashlib.md5(label_id.encode(), usedforsecurity=False).hexdigest()
            text = note.text[span[0] : span[1]]

            # First, add the parent label
            prediction.result.append(
                self._format_match(span[0], span[1], text, [label], label_id=label_id)
            )

            # Now add sublabels
            for sublabel_name, sublabel_values in sublabels.items():
                if not sublabel_name:
                    continue
                prediction.result.append(
                    self._format_match(
                        span[0],
                        span[1],
                        text,
                        sublabel_values,
                        label_id=label_id,
                        from_name=sublabel_name,
                    )
                )
                # Keep track of values for the sublabel data columns
                vals = sublabel_col_vals.setdefault((label, sublabel_name), {})
                for value in sublabel_values:
                    vals[(value, text.casefold())] = text

        task.predictions.extend(predictions.values())
        self._update_used_labels(task, {x.label for x in note.highlights})

        # Add sublabel data cols
        for (label, sublabel_name), vals_with_text in sublabel_col_vals.items():
            # Label Studio doesn't love col names with spaces, so we convert the label to a
            # SQL-safe version. The names will be unique (i.e. not conflict with our other data
            # columns like `docref_mappings` because we add a _label or _text suffix below.
            # There is a light chance of collision because of how we're modifying these labels,
            # but if that were to happen, the consequences are minor (one of the labels doesn't
            # get a Label Studio column).
            slug = label
            sublabel_name = sublabel_name.removeprefix(f"{label} ")
            if sublabel_name and slug != sublabel_name:
                slug += f"_{sublabel_name}"
            slug = NON_ALPHANUM.sub("_", slug.casefold())  # replace all non-alphanums with sunders
            slug = UNDERSCORES.sub("_", slug)  # replace multiple underscores with one

            # Now mash together any separate label values, using a little visual separator
            # (newlines will be removed by Label Studio, so we need something visual)
            vals_keys = sorted(vals_with_text)
            vals = " ✦ ".join([x[0].strip() for x in vals_keys])
            task.data[f"{slug}_label"] = vals
            texts = " ✦ ".join([vals_with_text[x].strip() for x in vals_keys])
            task.data[f"{slug}_text"] = texts

    def _format_philter_predictions(self, task: ls.ImportApiRequest, note: LabelStudioNote) -> None:
        """
        Adds a predication layer with philter spans.

        Note that this does *not* update the running list of used labels.
        This sets a "secret" / non-human-oriented label of "_philter".
        Label Studio will still highlight the spans, and this way we won't
        conflict with any existing labels.
        """
        if not note.philter_map:
            return

        prediction = ls.PredictionRequest.construct(model_version="Philter", result=[])

        for start, stop in sorted(note.philter_map.items()):
            # We hardcode the label "_philter" - Label Studio will still highlight unknown labels,
            # and this is unlikely to collide with existing labels.
            result = self._format_match(start, stop, note.text[start:stop], ["_philter"])
            prediction.result.append(result)

        task.predictions.append(prediction)

    def _update_used_labels(self, task: ls.ImportApiRequest, used_labels: Iterable[str]) -> None:
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
        existing_labels = task.data.get(self._labels_name, [])
        existing_labels = {d["value"] for d in existing_labels}
        existing_labels.update(used_labels)
        task.data[self._labels_name] = [{"value": x} for x in sorted(existing_labels)]
