"""Tests for cumulus.upload_notes.labelstudio.py"""

from unittest import mock

import ctakesclient
import ddt
from ctakesclient.typesystem import Polarity

from cumulus_etl import errors
from cumulus_etl.upload_notes.labelstudio import LabelStudioClient, LabelStudioNote
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
        *, encounter_id: str = "enc", ctakes: bool = True, philter_label: bool = True
    ) -> LabelStudioNote:
        text = "Normal note text"
        note = LabelStudioNote(
            "patient",
            "patient-anon",
            encounter_id,
            "enc-anon",
            doc_mappings={"doc": "doc-anon"},
            doc_spans={"doc": (0, len(text))},
            text=text,
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
        await self.push_tasks(self.make_note())
        self.assertEqual(
            {
                "data": {
                    "text": "Normal note text",
                    "patient_id": "patient",
                    "anon_patient_id": "patient-anon",
                    "enc_id": "enc",
                    "anon_enc_id": "enc-anon",
                    "docref_mappings": {"doc": "doc-anon"},
                    "docref_spans": {"doc": [0, 16]},
                    "mylabel": [{"value": "Itch"}, {"value": "Nausea"}],
                },
                "predictions": [
                    {
                        "model_version": "Cumulus cTAKES",
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
                        "model_version": "Cumulus Philter",
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
                    "patient_id": "patient",
                    "anon_patient_id": "patient-anon",
                    "enc_id": "enc",
                    "anon_enc_id": "enc-anon",
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
                "patient_id": "patient",
                "anon_patient_id": "patient-anon",
                "enc_id": "enc",
                "anon_enc_id": "enc-anon",
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
                "patient_id": "patient",
                "anon_patient_id": "patient-anon",
                "enc_id": "enc",
                "anon_enc_id": "enc-anon",
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
        self.ls_project.get_tasks.return_value = [{"id": 1, "data": {"enc_id": "enc"}}]

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
        self.ls_project.get_tasks.return_value = [{"id": 1, "data": {"enc_id": "enc"}}]

        await self.push_tasks(self.make_note(), self.make_note(encounter_id="enc2"))
        self.assertFalse(self.ls_project.delete_tasks.called)
        self.assertEqual("enc2", self.get_pushed_task()["data"]["enc_id"])

    async def test_push_highlights(self):
        note = self.make_note(philter_label=False, ctakes=False)
        note.highlights = [
            ctakesclient.typesystem.Span(7, 11),
            ctakesclient.typesystem.Span(12, 16),
        ]
        await self.push_tasks(note)
        self.assertEqual(
            {
                "data": {
                    "text": "Normal note text",
                    "patient_id": "patient",
                    "anon_patient_id": "patient-anon",
                    "enc_id": "enc",
                    "anon_enc_id": "enc-anon",
                    "docref_mappings": {"doc": "doc-anon"},
                    "docref_spans": {"doc": [0, 16]},
                    "mylabel": [{"value": "Tag"}],
                },
                "predictions": [
                    {
                        "model_version": "Cumulus Highlights",
                        "result": [
                            {
                                "from_name": "mylabel",
                                "to_name": "mytext",
                                "type": "labels",
                                "value": {
                                    "end": 11,
                                    "labels": ["Tag"],
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
                                    "labels": ["Tag"],
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

    async def test_push_in_batches(self):
        notes = [self.make_note(philter_label=False, ctakes=False) for _ in range(301)]
        await self.push_tasks(*notes)
        self.assertEqual(2, self.ls_project.import_tasks.call_count)
        imported_tasks = self.ls_project.import_tasks.call_args_list[0][0][0]
        self.assertEqual(300, len(imported_tasks))
        imported_tasks = self.ls_project.import_tasks.call_args_list[1][0][0]
        self.assertEqual(1, len(imported_tasks))
