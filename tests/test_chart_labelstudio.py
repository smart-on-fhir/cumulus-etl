"""Tests for cumulus.chart_review.labelstudio.py"""

from unittest import mock

import ddt
from ctakesclient.typesystem import Polarity

from cumulus_etl.chart_review.labelstudio import LabelStudioClient, LabelStudioNote

from tests import ctakesmock
from tests.utils import AsyncTestCase


@ddt.ddt
class TestChartLabelStudio(AsyncTestCase):
    """Test case for label studio support"""

    def setUp(self):
        super().setUp()

        self.ls_mock = self.patch("cumulus_etl.chart_review.labelstudio.label_studio_sdk.Client")
        self.ls_client = self.ls_mock.return_value
        self.ls_project = self.ls_client.get_project.return_value
        self.ls_project.get_tasks.return_value = []
        self.ls_project.parsed_label_config = {"mylabel": {"type": "Labels", "to_name": ["mytext"]}}

    @staticmethod
    def make_note(*, enc_id: str = "enc", matches: bool = True) -> LabelStudioNote:
        note = LabelStudioNote(enc_id, "enc-anon", {"doc": "doc-anon"}, "Ignored Title", "Normal note text")
        if matches:
            note.matches = ctakesmock.fake_ctakes_extract(note.text).list_match(polarity=Polarity.pos)
        return note

    @staticmethod
    def push_tasks(*notes, **kwargs) -> None:
        client = LabelStudioClient(
            "https://localhost/ls",
            "apikey",
            14,
            {
                # These two CUIs are in our standard mock cTAKES response
                "C0033774": "Itch",
                "C0027497": "Nausea",
                "C0028081": "Night Sweats",  # to demonstrate that unmatched CUIs are not generally pushed
            },
        )
        client.push_tasks(notes, **kwargs)

    def get_pushed_task(self) -> dict:
        self.assertEqual(1, self.ls_project.import_tasks.call_count)
        imported_tasks = self.ls_project.import_tasks.call_args[0][0]
        self.assertEqual(1, len(imported_tasks))
        return imported_tasks[0]

    def test_basic_push(self):
        self.push_tasks(self.make_note())
        self.assertEqual(
            {
                "data": {
                    "text": "Normal note text",
                    "enc_id": "enc",
                    "anon_id": "enc-anon",
                    "docref_mappings": {"doc": "doc-anon"},
                },
                "predictions": [
                    {
                        "model_version": "Cumulus",
                        "result": [
                            # Note that fever does not show up, as it was not in our initial CUI mapping (in push_tasks)
                            {
                                "from_name": "mylabel",
                                "id": "match0",
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
                                "id": "match1",
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
                    }
                ],
            },
            self.get_pushed_task(),
        )

    def test_no_matches(self):
        self.push_tasks(self.make_note(matches=False))
        self.assertEqual(
            {
                "data": {
                    "text": "Normal note text",
                    "enc_id": "enc",
                    "anon_id": "enc-anon",
                    "docref_mappings": {"doc": "doc-anon"},
                },
                "predictions": [
                    {
                        "model_version": "Cumulus",
                        "result": [],
                    }
                ],
            },
            self.get_pushed_task(),
        )

    @ddt.data("Choices", "Labels")
    def test_dynamic_labels(self, label_type):
        self.ls_project.parsed_label_config = {
            "mylabel": {"type": label_type, "to_name": ["mytext"], "dynamic_labels": True},
        }
        self.push_tasks(self.make_note())
        self.assertEqual(
            {
                "text": "Normal note text",
                "enc_id": "enc",
                "anon_id": "enc-anon",
                "docref_mappings": {"doc": "doc-anon"},
                "mylabel": [
                    {"value": "Itch"},
                    {"value": "Nausea"},
                ],
            },
            self.get_pushed_task()["data"],
        )

    def test_dynamic_labels_no_matches(self):
        self.ls_project.parsed_label_config = {
            "mylabel": {"type": "Labels", "to_name": ["mytext"], "dynamic_labels": True},
        }
        self.push_tasks(self.make_note(matches=False))
        self.assertEqual(
            {
                "text": "Normal note text",
                "enc_id": "enc",
                "anon_id": "enc-anon",
                "docref_mappings": {"doc": "doc-anon"},
                "mylabel": [],  # this needs to be sent, or the server will complain
            },
            self.get_pushed_task()["data"],
        )

    def test_overwrite(self):
        self.ls_project.get_tasks.return_value = [{"id": 1, "data": {"enc_id": "enc"}}]

        # Try once without overwrite
        self.push_tasks(self.make_note())
        self.assertFalse(self.ls_project.import_tasks.called)
        self.assertFalse(self.ls_project.delete_tasks.called)

        # Now overwrite
        self.push_tasks(self.make_note(), overwrite=True)
        self.assertEqual([mock.call([1])], self.ls_project.delete_tasks.call_args_list)
        self.assertTrue(self.ls_project.import_tasks.called)

    def test_overwrite_partial(self):
        """Verify that we push what we can and ignore any existing tasks by default"""
        self.ls_project.get_tasks.return_value = [{"id": 1, "data": {"enc_id": "enc"}}]

        self.push_tasks(self.make_note(), self.make_note(enc_id="enc2"))
        self.assertFalse(self.ls_project.delete_tasks.called)
        self.assertEqual("enc2", self.get_pushed_task()["data"]["enc_id"])
