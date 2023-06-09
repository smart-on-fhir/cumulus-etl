"""Tests for etl/tasks.py"""

import os
import shutil
import tempfile
from typing import AsyncIterator, List
from unittest import mock

import ddt
import respx

from cumulus_etl import common, deid, errors, fhir_client
from cumulus_etl.etl import config, tasks

from tests.ctakesmock import CtakesMixin
from tests import i2b2_mock_data
from tests.utils import AsyncTestCase


@ddt.ddt
class TestTasks(CtakesMixin, AsyncTestCase):
    """Test case for task methods"""

    def setUp(self) -> None:
        super().setUp()

        client = fhir_client.FhirClient("http://localhost/", [])
        self.tmpdir = tempfile.TemporaryDirectory()  # pylint: disable=consider-using-with
        self.input_dir = os.path.join(self.tmpdir.name, "input")
        self.phi_dir = os.path.join(self.tmpdir.name, "phi")
        self.errors_dir = os.path.join(self.tmpdir.name, "errors")
        os.makedirs(self.input_dir)
        os.makedirs(self.phi_dir)

        self.job_config = config.JobConfig(
            self.input_dir,
            self.input_dir,
            self.tmpdir.name,
            self.phi_dir,
            "ndjson",
            "ndjson",
            client,
            ctakes_overrides=self.ctakes_overrides.name,
            batch_size=5,
            dir_errors=self.errors_dir,
        )

        self.format = mock.MagicMock()
        self.job_config.create_formatter = mock.MagicMock(return_value=self.format)

        self.scrubber = deid.Scrubber()
        self.codebook = self.scrubber.codebook

        # Keeps consistent IDs
        shutil.copy(os.path.join(self.datadir, "simple/codebook.json"), self.phi_dir)

    def tearDown(self) -> None:
        super().tearDown()
        self.tmpdir = None

    def make_json(self, filename, resource_id, **kwargs):
        common.write_json(
            os.path.join(self.input_dir, f"{filename}.ndjson"), {"resourceType": "Test", **kwargs, "id": resource_id}
        )

    async def test_batch_iterate(self):
        """Check a bunch of edge cases for the _batch_iterate helper"""
        # pylint: disable=protected-access

        # Tiny little convenience method to be turn sync lists into async iterators.
        async def async_iter(values: List) -> AsyncIterator:
            for x in values:
                yield x

        # Handles converting all the async code into synchronous lists for ease of testing
        async def assert_batches_equal(expected: List, values: List, batch_size: int) -> None:
            collected = []
            async for batch in tasks._batch_iterate(async_iter(values), batch_size):
                batch_list = []
                async for item in batch:
                    batch_list.append(item)
                collected.append(batch_list)
            self.assertEqual(expected, collected)

        await assert_batches_equal([], [], 2)

        await assert_batches_equal(
            [
                [1, 2.1, 2.2],
                [3, 4],
            ],
            [1, [2.1, 2.2], 3, 4],
            2,
        )

        await assert_batches_equal(
            [
                [1.1, 1.2],
                [2.1, 2.2],
                [3, 4],
            ],
            [[1.1, 1.2], [2.1, 2.2], 3, 4],
            2,
        )

        await assert_batches_equal(
            [
                [1, 2.1, 2.2],
                [3, 4],
                [5],
            ],
            [1, [2.1, 2.2], 3, 4, 5],
            2,
        )

        await assert_batches_equal(
            [
                [1, 2.1, 2.2],
                [3, 4],
            ],
            [1, [2.1, 2.2], 3, 4],
            3,
        )

        await assert_batches_equal(
            [
                [1],
                [2.1, 2.2],
                [3],
            ],
            [1, [2.1, 2.2], 3],
            1,
        )

        with self.assertRaises(ValueError):
            await assert_batches_equal([], [1, 2, 3], 0)

        with self.assertRaises(ValueError):
            await assert_batches_equal([], [1, 2, 3], -1)

    def test_read_ndjson(self):
        """Verify we recognize all expected ndjson filename formats"""
        self.make_json("11.Condition", "11")
        self.make_json("Condition.12", "12")
        self.make_json("13.Condition.13", "13")
        self.make_json("Patient.14", "14")

        resources = tasks.ConditionTask(self.job_config, self.scrubber).read_ndjson()
        self.assertEqual({"11", "12", "13"}, {r["id"] for r in resources})

        resources = tasks.PatientTask(self.job_config, self.scrubber).read_ndjson()
        self.assertEqual({"14"}, {r["id"] for r in resources})

        resources = tasks.EncounterTask(self.job_config, self.scrubber).read_ndjson()
        self.assertEqual([], list(resources))

    async def test_unknown_modifier_extensions_skipped_for_patients(self):
        """Verify we ignore unknown modifier extensions during a normal task (like patients)"""
        self.make_json("Patient.0", "0")
        self.make_json("Patient.1", "1", modifierExtension=[{"url": "unrecognized"}])

        await tasks.PatientTask(self.job_config, self.scrubber).run()

        # Confirm that only patient 0 got stored
        self.assertEqual(1, self.format.write_records.call_count)
        df = self.format.write_records.call_args[0][0]
        self.assertEqual([self.codebook.db.patient("0")], list(df.id))

    async def test_unknown_modifier_extensions_skipped_for_nlp_symptoms(self):
        """Verify we ignore unknown modifier extensions during a custom read task (nlp symptoms)"""
        docref0 = i2b2_mock_data.documentreference()
        docref0["subject"]["reference"] = "Patient/1234"
        self.make_json("DocumentReference.0", "0", **docref0)
        docref1 = i2b2_mock_data.documentreference()
        docref1["subject"]["reference"] = "Patient/5678"
        docref1["modifierExtension"] = [{"url": "unrecognized"}]
        self.make_json("DocumentReference.1", "1", **docref1)

        await tasks.CovidSymptomNlpResultsTask(self.job_config, self.scrubber).run()

        # Confirm that only symptoms from docref 0 got stored
        self.assertEqual(1, self.format.write_records.call_count)
        df = self.format.write_records.call_args[0][0]
        expected_subject = self.codebook.db.patient("1234")
        self.assertEqual({expected_subject}, set(df.subject_id))

    @ddt.data(
        # (coding, expected valid note)
        # Invalid codes
        ([], False),
        ([{"system": "http://cumulus.smarthealthit.org/i2b2", "code": "NOTE:0"}], False),
        ([{"system": "http://loinc.org", "code": "00000-0"}], False),
        ([{"system": "http://example.org", "code": "nope"}], False),
        # Valid codes
        ([{"system": "http://cumulus.smarthealthit.org/i2b2", "code": "NOTE:3710480"}], True),
        ([{"system": "http://loinc.org", "code": "57053-1"}], True),
        ([{"system": "nope", "code": "nope"}, {"system": "http://loinc.org", "code": "57053-1"}], True),
    )
    @ddt.unpack
    async def test_ed_note_filtering_for_nlp(self, codings, expected):
        """Verify we filter out any non-emergency-department note"""
        # Use one doc with category set, and one with type set. Either should work.
        docref0 = i2b2_mock_data.documentreference()
        docref0["category"] = [{"coding": codings}]
        del docref0["type"]
        self.make_json("DocumentReference.0", "0", **docref0)
        docref1 = i2b2_mock_data.documentreference()
        docref1["type"] = {"coding": codings}
        self.make_json("DocumentReference.1", "1", **docref1)

        await tasks.CovidSymptomNlpResultsTask(self.job_config, self.scrubber).run()

        self.assertEqual(1 if expected else 0, self.format.write_records.call_count)
        if expected:
            df = self.format.write_records.call_args[0][0]
            self.assertEqual(4, len(df))

    async def test_non_ed_visit_is_skipped_for_covid_symptoms(self):
        """Verify we ignore non ED visits for the covid symptoms NLP"""
        docref0 = i2b2_mock_data.documentreference()
        docref0["type"]["coding"][0]["code"] = "NOTE:nope"  # pylint: disable=unsubscriptable-object
        self.make_json("DocumentReference.0", "skipped", **docref0)
        docref1 = i2b2_mock_data.documentreference()
        docref1["type"]["coding"][0]["code"] = "NOTE:149798455"  # pylint: disable=unsubscriptable-object
        self.make_json("DocumentReference.1", "present", **docref1)

        await tasks.CovidSymptomNlpResultsTask(self.job_config, self.scrubber).run()

        # Confirm that only symptoms from docref 'present' got stored
        self.assertEqual(1, self.format.write_records.call_count)
        df = self.format.write_records.call_args[0][0]
        expected_docref = self.codebook.db.resource_hash("present")
        self.assertEqual({expected_docref}, set(df.docref_id))

    @ddt.data(
        ({"status": "entered-in-error"}, False),
        ({"status": "superseded"}, False),
        ({"status": "current"}, True),
        ({"docStatus": "preliminary"}, False),
        ({"docStatus": "entered-in-error"}, False),
        ({"docStatus": "final"}, True),
        ({"docStatus": "amended"}, True),
        ({}, True),  # without any docStatus, we still run NLP on it ("status" is required and can't be skipped)
    )
    @ddt.unpack
    async def test_bad_doc_status_is_skipped_for_covid_symptoms(self, status: dict, should_process: bool):
        """Verify we ignore certain docStatus codes for the covid symptoms NLP"""
        docref = i2b2_mock_data.documentreference()
        docref.update(status)
        self.make_json("DocumentReference.0", "doc", **docref)

        await tasks.CovidSymptomNlpResultsTask(self.job_config, self.scrubber).run()
        self.assertEqual(1 if should_process else 0, self.format.write_records.call_count)

    def test_unknown_task(self):
        with self.assertRaises(SystemExit) as cm:
            tasks.EtlTask.get_selected_tasks(names=["blarg"])
        self.assertEqual(errors.TASK_UNKNOWN, cm.exception.code)

    def test_over_filtered(self):
        """Verify that we catch when the user filters out all tasks for themselves"""
        with self.assertRaises(SystemExit) as cm:
            tasks.EtlTask.get_selected_tasks(filter_tags=["cpu", "gpu"])
        self.assertEqual(errors.TASK_SET_EMPTY, cm.exception.code)

    def test_filtered_but_named_task(self):
        with self.assertRaises(SystemExit) as cm:
            tasks.EtlTask.get_selected_tasks(names=["condition"], filter_tags=["gpu"])
        self.assertEqual(errors.TASK_FILTERED_OUT, cm.exception.code)

    @ddt.data(
        # list of (URL, contentType), expected text
        ([("http://localhost/file-cough", "text/plain")], "cough"),  # handles absolute URL
        ([("file-cough", "text/html")], "cough"),  # handles text/*
        ([("file-cough", "application/xhtml+xml")], "cough"),  # handles xhtml
        ([("file-cough", "text/html"), ("file-fever", "text/plain")], "fever"),  # prefers text/plain to text/*
        ([("file-cough", "application/xhtml+xml"), ("file-fever", "text/blarg")], "fever"),  # prefers text/* to xhtml
        ([("file-cough", "nope/nope")], None),  # ignores unsupported mimetypes
    )
    @ddt.unpack
    @respx.mock
    async def test_note_urls_downloaded(self, attachments, expected_text):
        """Verify that we download any attachments with URLs"""
        # We return three words due to how our cTAKES mock works. It wants 3 words -- fever word is in middle.
        respx.get("http://localhost/file-cough").respond(text="has cough bad")
        respx.get("http://localhost/file-fever").respond(text="has fever bad")
        respx.post(os.environ["URL_CTAKES_REST"]).pass_through()  # ignore cTAKES

        docref0 = i2b2_mock_data.documentreference()
        docref0["content"] = [{"attachment": {"url": a[0], "contentType": a[1]}} for a in attachments]
        self.make_json("DocumentReference.0", "doc0", **docref0)

        async with self.job_config.client:
            await tasks.CovidSymptomNlpResultsTask(self.job_config, self.scrubber).run()

        if expected_text:
            self.assertEqual(1, self.format.write_records.call_count)
            df = self.format.write_records.call_args[0][0]
            self.assertEqual(expected_text, df.iloc[0].match["text"])
        else:
            self.assertEqual(0, self.format.write_records.call_count)

    async def test_drop_duplicates(self):
        """Verify that we run() will drop duplicate rows inside an input batch."""
        # Two "A" ids and one "B" id
        self.make_json("Patient.01", "A")
        self.make_json("Patient.02", "A")
        self.make_json("Patient.1", "B")

        await tasks.PatientTask(self.job_config, self.scrubber).run()

        # Confirm that only one version of patient A got stored
        self.assertEqual(1, self.format.write_records.call_count)
        df = self.format.write_records.call_args[0][0]
        self.assertEqual(2, len(df.id))
        self.assertEqual(sorted([self.codebook.db.patient("A"), self.codebook.db.patient("B")]), sorted(df.id))

    async def test_batch_write_errors_saved(self):
        self.make_json("Patient.1", "A")
        self.make_json("Patient.2", "B")
        self.make_json("Patient.3", "C")
        self.job_config.batch_size = 1
        self.format.write_records.side_effect = [False, True, False]  # First and third will fail

        await tasks.PatientTask(self.job_config, self.scrubber).run()

        self.assertEqual(
            ["write-error.000.ndjson", "write-error.002.ndjson"], list(sorted(os.listdir(f"{self.errors_dir}/patient")))
        )
        self.assertEqual(
            {"resourceType": "Test", "id": "30d95f17d9f51f3a151c51bf0a7fcb1717363f3a87d2dbace7d594ee68d3a82f"},
            common.read_json(f"{self.errors_dir}/patient/write-error.000.ndjson"),
        )
        self.assertEqual(
            {"resourceType": "Test", "id": "ed9ab553005a7c9bdb26ecf9f612ea996ad99b1a96a34bf88c260f1c901d8289"},
            common.read_json(f"{self.errors_dir}/patient/write-error.002.ndjson"),
        )

    async def test_nlp_errors_saved(self):
        docref = i2b2_mock_data.documentreference()
        self.make_json("DocumentReference.2", "B", **docref)
        del docref["context"]  # this will cause this docref to fail
        self.make_json("DocumentReference.1", "A", **docref)
        self.make_json("DocumentReference.3", "C", **docref)

        await tasks.CovidSymptomNlpResultsTask(self.job_config, self.scrubber).run()

        self.assertEqual(["nlp-errors.ndjson"], os.listdir(f"{self.errors_dir}/covid_symptom__nlp_results"))
        self.assertEqual(
            ["A", "C"],  # pre-scrubbed versions of the docrefs are stored, for easier debugging
            [x["id"] for x in common.read_ndjson(f"{self.errors_dir}/covid_symptom__nlp_results/nlp-errors.ndjson")],
        )
