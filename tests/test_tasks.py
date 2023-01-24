"""Tests for tasks.py"""

import os
import shutil
import tempfile
import unittest
from unittest import mock

from fhirclient.models.extension import Extension

from cumulus import common, config, deid, errors, store, tasks

from tests.ctakesmock import CtakesMixin
from tests import i2b2_mock_data


class TestTasks(CtakesMixin, unittest.TestCase):
    """Test case for task methods"""

    def setUp(self) -> None:
        super().setUp()

        script_dir = os.path.dirname(__file__)
        data_dir = os.path.join(script_dir, "data/simple")
        self.tmpdir = tempfile.TemporaryDirectory()  # pylint: disable=consider-using-with
        self.input_dir = os.path.join(self.tmpdir.name, "input")
        self.phi_dir = os.path.join(self.tmpdir.name, "phi")
        os.makedirs(self.input_dir)
        os.makedirs(self.phi_dir)

        self.loader = mock.MagicMock()
        self.format = mock.MagicMock()
        phi_root = store.Root(self.phi_dir)
        self.job_config = config.JobConfig(self.loader, self.input_dir, self.format, phi_root, batch_size=5)

        self.scrubber = deid.Scrubber()
        self.codebook = self.scrubber.codebook

        # Keeps consistent IDs
        shutil.copy(os.path.join(data_dir, "codebook.json"), self.phi_dir)

    def tearDown(self) -> None:
        super().tearDown()
        self.tmpdir = None

    def make_json(self, filename, resource_id, **kwargs):
        common.write_json(os.path.join(self.input_dir, f"{filename}.ndjson"), {**kwargs, "id": resource_id})

    def test_batch_iterate(self):
        """Check a bunch of edge cases for the _batch_iterate helper"""
        # pylint: disable=protected-access

        self.assertEqual([], [list(x) for x in tasks._batch_iterate([], 2)])

        self.assertEqual(
            [
                [1, 2.1, 2.2],
                [3, 4],
            ],
            [list(x) for x in tasks._batch_iterate([1, [2.1, 2.2], 3, 4], 2)],
        )

        self.assertEqual(
            [
                [1.1, 1.2],
                [2.1, 2.2],
                [3, 4],
            ],
            [list(x) for x in tasks._batch_iterate([[1.1, 1.2], [2.1, 2.2], 3, 4], 2)],
        )

        self.assertEqual(
            [
                [1, 2.1, 2.2],
                [3, 4],
                [5],
            ],
            [list(x) for x in tasks._batch_iterate([1, [2.1, 2.2], 3, 4, 5], 2)],
        )

        self.assertEqual(
            [
                [1, 2.1, 2.2],
                [3, 4],
            ],
            [list(x) for x in tasks._batch_iterate([1, [2.1, 2.2], 3, 4], 3)],
        )

        self.assertEqual(
            [
                [1],
                [2.1, 2.2],
                [3],
            ],
            [list(x) for x in tasks._batch_iterate([1, [2.1, 2.2], 3], 1)],
        )

        with self.assertRaises(ValueError):
            list(tasks._batch_iterate([1, 2, 3], 0))

        with self.assertRaises(ValueError):
            list(tasks._batch_iterate([1, 2, 3], -1))

    def test_read_ndjson(self):
        """Verify we recognize all expected ndjson filename formats"""
        self.make_json("11.Condition", "11")
        self.make_json("Condition.12", "12")
        self.make_json("13.Condition.13", "13")
        self.make_json("Patient.14", "14")

        resources = tasks.ConditionTask(self.job_config, self.scrubber).read_ndjson()
        self.assertEqual({"11", "12", "13"}, {r.id for r in resources})

        resources = tasks.PatientTask(self.job_config, self.scrubber).read_ndjson()
        self.assertEqual({"14"}, {r.id for r in resources})

        resources = tasks.EncounterTask(self.job_config, self.scrubber).read_ndjson()
        self.assertEqual([], list(resources))

    def test_unknown_modifier_extensions_skipped_for_patients(self):
        """Verify we ignore unknown modifier extensions during a normal task (like patients)"""
        self.make_json("Patient.0", "0")
        self.make_json("Patient.1", "1", modifierExtension=[{"url": "unrecognized"}])

        tasks.PatientTask(self.job_config, self.scrubber).run()

        # Confirm that only patient 0 got stored
        self.assertEqual(1, self.format.write_records.call_count)
        df = self.format.write_records.call_args[0][1]
        self.assertEqual([self.codebook.db.patient("0")], list(df.id))

    def test_unknown_modifier_extensions_skipped_for_nlp_symptoms(self):
        """Verify we ignore unknown modifier extensions during a custom read task (nlp symptoms)"""
        docref0 = i2b2_mock_data.documentreference()
        docref0.subject.reference = "Patient/1234"
        self.make_json("DocumentReference.0", "0", **docref0.as_json())
        docref1 = i2b2_mock_data.documentreference()
        docref1.subject.reference = "Patient/5678"
        docref1.modifierExtension = [Extension({"url": "unrecognized"})]
        self.make_json("DocumentReference.1", "1", **docref1.as_json())

        tasks.CovidSymptomNlpResultsTask(self.job_config, self.scrubber).run()

        # Confirm that only symptoms from docref 0 got stored
        self.assertEqual(1, self.format.write_records.call_count)
        df = self.format.write_records.call_args[0][1]
        expected_subject = self.codebook.db.patient("1234")
        self.assertEqual({expected_subject}, set(df.subject_id))

    def test_non_ed_visit_is_skipped_for_covid_symptoms(self):
        """Verify we ignore non ED visits for the covid symptoms NLP"""
        docref0 = i2b2_mock_data.documentreference()
        docref0.type.coding[0].code = "NOTE:nope"  # pylint: disable=unsubscriptable-object
        self.make_json("DocumentReference.0", "skipped", **docref0.as_json())
        docref1 = i2b2_mock_data.documentreference()
        docref1.type.coding[0].code = "NOTE:149798455"  # pylint: disable=unsubscriptable-object
        self.make_json("DocumentReference.1", "present", **docref1.as_json())

        tasks.CovidSymptomNlpResultsTask(self.job_config, self.scrubber).run()

        # Confirm that only symptoms from docref 'present' got stored
        self.assertEqual(1, self.format.write_records.call_count)
        df = self.format.write_records.call_args[0][1]
        expected_docref = self.codebook.db.resource_hash("present")
        self.assertEqual({expected_docref}, set(df.docref_id))

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
