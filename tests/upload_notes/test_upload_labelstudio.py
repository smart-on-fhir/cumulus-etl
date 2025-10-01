"""Tests for cumulus.upload_notes.labelstudio.py"""

import datetime
from unittest import mock

import ddt
from ctakesclient.typesystem import Polarity

from cumulus_etl import errors
from cumulus_etl.upload_notes.labelstudio import Highlight, LabelStudioClient, LabelStudioNote
from tests import ctakesmock
from tests.utils import AsyncTestCase


@ddt.ddt
class TestUploadLabelStudio(AsyncTestCase):
    """Test case for label studio support"""

    def setUp(self):
        super().setUp()

        self.ls_mock = self.patch("cumulus_etl.upload_notes.labelstudio.label_studio_sdk.Client")
        self.ls_client = self.ls_mock.return_value
        self.ls_project = self.ls_client.get_project.return_value
        self.ls_project.get_tasks.return_value = []
        self.ls_project.parsed_label_config = {"mylabel": {"type": "Labels", "to_name": ["mytext"]}}

    @staticmethod
    def make_note(
        *,
        unique_id: str = "unique",
        ctakes: bool = True,
        philter_label: bool = True,
        **kwargs,
    ) -> LabelStudioNote:
        text = "Normal note text"
        note = LabelStudioNote(
            unique_id,
            "patient",
            "patient-anon",
            "enc",
            "enc-anon",
            doc_mappings={"doc": "doc-anon"},
            doc_spans={"doc": (0, len(text))},
            text=text,
            **kwargs,
        )
        if ctakes:
            note.ctakes_matches = ctakesmock.fake_ctakes_extract(note.text).list_match(
                polarity=Polarity.pos
            )
        if philter_label:
            matches = ctakesmock.fake_ctakes_extract(note.text).list_match(polarity=Polarity.pos)
            note.philter_map = {m.begin: m.end for m in matches}
        return note

    @staticmethod
    async def push_tasks(*notes, **kwargs) -> None:
        client = LabelStudioClient(
            "https://localhost/ls",
            "apikey",
            14,
            {
                # These two CUIs are in our standard mock cTAKES response
                "C0033774": "Itch",
                "C0027497": "Nausea",
                # The third is demonstrates that unmatched CUIs are not generally pushed
                "C0028081": "Night Sweats",
            },
        )
        await client.push_tasks(notes, **kwargs)

    def get_pushed_task(self) -> dict:
        self.assertEqual(1, self.ls_project.import_tasks.call_count)
        imported_tasks = self.ls_project.import_tasks.call_args[0][0]
        self.assertEqual(1, len(imported_tasks))
        return imported_tasks[0]

    async def test_basic_push(self):
        await self.push_tasks(self.make_note(date=datetime.datetime(2010, 10, 10)))
        self.assertEqual(
            {
                "data": {
                    "text": "Normal note text",
                    "unique_id": "unique",
                    "patient_id": "patient",
                    "anon_patient_id": "patient-anon",
                    "encounter_id": "enc",
                    "anon_encounter_id": "enc-anon",
                    "date": "2010-10-10T00:00:00",
                    "docref_mappings": {"doc": "doc-anon"},
                    "docref_spans": {"doc": [0, 16]},
                    "mylabel": [{"value": "Itch"}, {"value": "Nausea"}],
                },
                "predictions": [
                    {
                        "model_version": "cTAKES",
                        "result": [
                            # Note that fever does not show up,
                            # as it was not in our initial CUI mapping (in push_tasks)
                            {
                                "from_name": "mylabel",
                                "to_name": "mytext",
                                "type": "labels",
                                "value": {
                                    "end": 11,
                                    "labels": ["Nausea"],
                                    "score": 1.0,
                                    "start": 7,
                                    "text": "note",
                                },
                            },
                            {
                                "from_name": "mylabel",
                                "to_name": "mytext",
                                "type": "labels",
                                "value": {
                                    "end": 16,
                                    "labels": ["Itch"],
                                    "score": 1.0,
                                    "start": 12,
                                    "text": "text",
                                },
                            },
                        ],
                    },
                    {
                        "model_version": "Philter",
                        "result": [
                            {
                                "from_name": "mylabel",
                                "to_name": "mytext",
                                "type": "labels",
                                "value": {
                                    "end": 6,
                                    "labels": ["_philter"],
                                    "score": 1.0,
                                    "start": 0,
                                    "text": "Normal",
                                },
                            },
                            {
                                "from_name": "mylabel",
                                "to_name": "mytext",
                                "type": "labels",
                                "value": {
                                    "end": 11,
                                    "labels": ["_philter"],
                                    "score": 1.0,
                                    "start": 7,
                                    "text": "note",
                                },
                            },
                            {
                                "from_name": "mylabel",
                                "to_name": "mytext",
                                "type": "labels",
                                "value": {
                                    "end": 16,
                                    "labels": ["_philter"],
                                    "score": 1.0,
                                    "start": 12,
                                    "text": "text",
                                },
                            },
                        ],
                    },
                ],
            },
            self.get_pushed_task(),
        )

    async def test_no_predictions(self):
        await self.push_tasks(self.make_note(ctakes=False, philter_label=False))
        self.assertEqual(
            {
                "data": {
                    "text": "Normal note text",
                    "unique_id": "unique",
                    "patient_id": "patient",
                    "anon_patient_id": "patient-anon",
                    "encounter_id": "enc",
                    "anon_encounter_id": "enc-anon",
                    "date": None,
                    "docref_mappings": {"doc": "doc-anon"},
                    "docref_spans": {"doc": [0, 16]},
                    "mylabel": [],
                },
                "predictions": [],
            },
            self.get_pushed_task(),
        )

    @ddt.data("Choices", "Labels")
    async def test_dynamic_labels(self, label_type):
        """Verify we send dynamic labels"""
        self.ls_project.parsed_label_config = {
            "mylabel": {"type": label_type, "to_name": ["mytext"]},
        }
        await self.push_tasks(self.make_note())
        self.assertEqual(
            {
                "text": "Normal note text",
                "unique_id": "unique",
                "patient_id": "patient",
                "anon_patient_id": "patient-anon",
                "encounter_id": "enc",
                "anon_encounter_id": "enc-anon",
                "date": None,
                "docref_mappings": {"doc": "doc-anon"},
                "docref_spans": {"doc": [0, 16]},
                "mylabel": [
                    {"value": "Itch"},
                    {"value": "Nausea"},
                ],
            },
            self.get_pushed_task()["data"],
        )

    async def test_dynamic_labels_no_predictions(self):
        await self.push_tasks(self.make_note(ctakes=False, philter_label=False))
        self.assertEqual(
            {
                "text": "Normal note text",
                "unique_id": "unique",
                "patient_id": "patient",
                "anon_patient_id": "patient-anon",
                "encounter_id": "enc",
                "anon_encounter_id": "enc-anon",
                "date": None,
                "docref_mappings": {"doc": "doc-anon"},
                "docref_spans": {"doc": [0, 16]},
                "mylabel": [],  # this needs to be sent, or the server will complain
            },
            self.get_pushed_task()["data"],
        )

    async def test_no_label_config(self):
        self.ls_project.parsed_label_config = {}
        with self.assert_fatal_exit(errors.LABEL_STUDIO_CONFIG_INVALID):
            await self.push_tasks(self.make_note())

    async def test_overwrite(self):
        self.ls_project.get_tasks.return_value = [{"id": 1, "data": {"unique_id": "unique"}}]

        # Try once without overwrite
        await self.push_tasks(self.make_note())
        self.assertFalse(self.ls_project.import_tasks.called)
        self.assertFalse(self.ls_project.delete_tasks.called)

        # Now overwrite
        await self.push_tasks(self.make_note(), overwrite=True)
        self.assertEqual([mock.call([1])], self.ls_project.delete_tasks.call_args_list)
        self.assertTrue(self.ls_project.import_tasks.called)

    async def test_overwrite_partial(self):
        """Verify that we push what we can and ignore any existing tasks by default"""
        self.ls_project.get_tasks.return_value = [{"id": 1, "data": {"unique_id": "unique"}}]

        await self.push_tasks(self.make_note(), self.make_note(unique_id="unique2"))
        self.assertFalse(self.ls_project.delete_tasks.called)
        self.assertEqual("unique2", self.get_pushed_task()["data"]["unique_id"])

    async def test_push_highlights(self):
        self.ls_project.parsed_label_config = {
            "mylabel": {"type": "Labels", "to_name": ["mytext"]},
            "Sub1": {"type": "Choices", "to_name": ["mytext"]},
            "Sub2": {"type": "TextArea", "to_name": ["mytext"]},
        }
        note = self.make_note(philter_label=False, ctakes=False)
        note.highlights = [
            Highlight("Label1", (7, 11), "First Source", sublabel_name="Sub1", sublabel_value="A"),
            Highlight("Label1", (12, 16), "First Source", sublabel_name="Sub1", sublabel_value="A"),
            Highlight("Label1", (12, 16), "First Source", sublabel_name="Sub1", sublabel_value="B"),
            Highlight("Label1", (12, 16), "First Source", sublabel_name="Sub2", sublabel_value="C"),
            Highlight("Label2", (7, 11), "First Source"),
            Highlight("Label1", (7, 11), "Second Source", sublabel_name="Sub2", sublabel_value="C"),
        ]
        await self.push_tasks(note)
        self.assertEqual(
            {
                "data": {
                    "text": "Normal note text",
                    "unique_id": "unique",
                    "patient_id": "patient",
                    "anon_patient_id": "patient-anon",
                    "encounter_id": "enc",
                    "anon_encounter_id": "enc-anon",
                    "date": None,
                    "docref_mappings": {"doc": "doc-anon"},
                    "docref_spans": {"doc": [0, 16]},
                    "mylabel": [{"value": "Label1"}, {"value": "Label2"}],
                },
                "predictions": [
                    {
                        "model_version": "First Source",
                        "result": [
                            {
                                "id": "fce1bc68a2a6418f24e1f35f1b827431",
                                "from_name": "mylabel",
                                "to_name": "mytext",
                                "type": "labels",
                                "value": {
                                    "end": 11,
                                    "labels": ["Label1"],
                                    "score": 1.0,
                                    "start": 7,
                                    "text": "note",
                                },
                            },
                            {
                                "id": "fce1bc68a2a6418f24e1f35f1b827431",
                                "from_name": "Sub1",
                                "to_name": "mytext",
                                "type": "choices",
                                "value": {
                                    "choices": ["A"],
                                    "end": 11,
                                    "score": 1.0,
                                    "start": 7,
                                    "text": "note",
                                },
                            },
                            {
                                "id": "88ffbfff9d62ad338d71469f66508fea",
                                "from_name": "mylabel",
                                "to_name": "mytext",
                                "type": "labels",
                                "value": {
                                    "end": 16,
                                    "labels": ["Label1"],
                                    "score": 1.0,
                                    "start": 12,
                                    "text": "text",
                                },
                            },
                            {
                                "id": "88ffbfff9d62ad338d71469f66508fea",
                                "from_name": "Sub1",
                                "to_name": "mytext",
                                "type": "choices",
                                "value": {
                                    "choices": ["A", "B"],
                                    "end": 16,
                                    "score": 1.0,
                                    "start": 12,
                                    "text": "text",
                                },
                            },
                            {
                                "id": "88ffbfff9d62ad338d71469f66508fea",
                                "from_name": "Sub2",
                                "to_name": "mytext",
                                "type": "textarea",
                                "value": {
                                    "end": 16,
                                    "score": 1.0,
                                    "start": 12,
                                    "text": ["C"],
                                },
                            },
                            {
                                "id": "d57979ec1c633b690e13b1e3d182c3e3",
                                "from_name": "mylabel",
                                "to_name": "mytext",
                                "type": "labels",
                                "value": {
                                    "end": 11,
                                    "labels": ["Label2"],
                                    "score": 1.0,
                                    "start": 7,
                                    "text": "note",
                                },
                            },
                        ],
                    },
                    {
                        "model_version": "Second Source",
                        "result": [
                            {
                                "id": "3b5b3791ca8e471832ea07eb5f81163e",
                                "from_name": "mylabel",
                                "to_name": "mytext",
                                "type": "labels",
                                "value": {
                                    "end": 11,
                                    "labels": ["Label1"],
                                    "score": 1.0,
                                    "start": 7,
                                    "text": "note",
                                },
                            },
                            {
                                "id": "3b5b3791ca8e471832ea07eb5f81163e",
                                "from_name": "Sub2",
                                "to_name": "mytext",
                                "type": "textarea",
                                "value": {
                                    "end": 11,
                                    "score": 1.0,
                                    "start": 7,
                                    "text": ["C"],
                                },
                            },
                        ],
                    },
                ],
            },
            self.get_pushed_task(),
        )

    async def test_unrecognized_label_name(self):
        self.ls_project.parsed_label_config = {
            "mylabel": {"type": "Labels", "to_name": ["mytext"]},
        }
        note = self.make_note(philter_label=False, ctakes=False)
        note.highlights = [
            Highlight("Label1", (7, 11), "First Source", sublabel_name="Sub1", sublabel_value="A"),
        ]
        with self.assert_fatal_exit(errors.LABEL_UNKNOWN):
            await self.push_tasks(note)

    async def test_unrecognized_config_type(self):
        self.ls_project.parsed_label_config = {
            "mylabel": {"type": "Labels", "to_name": ["mytext"]},
            "Sub1": {"type": "Bogus", "to_name": ["mytext"]},
        }
        note = self.make_note(philter_label=False, ctakes=False)
        note.highlights = [
            Highlight("Label1", (7, 11), "First Source", sublabel_name="Sub1", sublabel_value="A"),
        ]
        with self.assert_fatal_exit(errors.LABEL_CONFIG_TYPE_UNKNOWN):
            await self.push_tasks(note)

    async def test_push_in_batches(self):
        notes = [
            self.make_note(philter_label=False, ctakes=False, unique_id=str(num))
            for num in range(501)
        ]
        await self.push_tasks(*notes)

        # Confirm we searched in batches
        self.assertEqual(2, self.ls_project.get_tasks.call_count)
        filters = self.ls_project.get_tasks.call_args_list[0][1]["filters"]
        self.assertEqual(500, len(filters["items"][0]["value"]))
        filters = self.ls_project.get_tasks.call_args_list[1][1]["filters"]
        self.assertEqual(1, len(filters["items"][0]["value"]))

        # Confirm we imported in batches
        self.assertEqual(2, self.ls_project.import_tasks.call_count)
        imported_tasks = self.ls_project.import_tasks.call_args_list[0][0][0]
        self.assertEqual(300, len(imported_tasks))
        imported_tasks = self.ls_project.import_tasks.call_args_list[1][0][0]
        self.assertEqual(201, len(imported_tasks))
