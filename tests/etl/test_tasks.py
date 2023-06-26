"""Tests for etl/tasks/"""

import os
import shutil
import tempfile
from unittest import mock

import ddt

from cumulus_etl import common, deid, errors, fhir
from cumulus_etl.etl import config, tasks
from cumulus_etl.etl.tasks import basic_tasks

from tests.utils import AsyncTestCase


class TaskTestCase(AsyncTestCase):
    """Base class for task-focused test suites"""

    def setUp(self) -> None:
        super().setUp()

        client = fhir.FhirClient("http://localhost/", [])
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
            batch_size=5,
            dir_errors=self.errors_dir,
        )

        self.format = mock.MagicMock(dbname="table")
        self.format2 = mock.MagicMock(dbname="table2")  # for tasks that have multiple output streams
        self.create_formatter_mock = mock.MagicMock(side_effect=[self.format, self.format2])
        self.job_config.create_formatter = self.create_formatter_mock

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


@ddt.ddt
class TestTasks(TaskTestCase):
    """Test case for basic task methods"""

    def test_read_ndjson(self):
        """Verify we recognize all expected ndjson filename formats"""
        self.make_json("11.Condition", "11")
        self.make_json("Condition.12", "12")
        self.make_json("13.Condition.13", "13")
        self.make_json("Patient.14", "14")

        resources = basic_tasks.ConditionTask(self.job_config, self.scrubber).read_ndjson()
        self.assertEqual({"11", "12", "13"}, {r["id"] for r in resources})

        resources = basic_tasks.PatientTask(self.job_config, self.scrubber).read_ndjson()
        self.assertEqual({"14"}, {r["id"] for r in resources})

        resources = basic_tasks.EncounterTask(self.job_config, self.scrubber).read_ndjson()
        self.assertEqual([], list(resources))

    async def test_unknown_modifier_extensions_skipped_for_patients(self):
        """Verify we ignore unknown modifier extensions during a normal task (like patients)"""
        self.make_json("Patient.0", "0")
        self.make_json("Patient.1", "1", modifierExtension=[{"url": "unrecognized"}])

        await basic_tasks.PatientTask(self.job_config, self.scrubber).run()

        # Confirm that only patient 0 got stored
        self.assertEqual(1, self.format.write_records.call_count)
        df = self.format.write_records.call_args[0][0]
        self.assertEqual([self.codebook.db.patient("0")], list(df.id))

    def test_unknown_task(self):
        with self.assertRaises(SystemExit) as cm:
            tasks.get_selected_tasks(names=["blarg"])
        self.assertEqual(errors.TASK_UNKNOWN, cm.exception.code)

    def test_over_filtered(self):
        """Verify that we catch when the user filters out all tasks for themselves"""
        with self.assertRaises(SystemExit) as cm:
            tasks.get_selected_tasks(filter_tags=["cpu", "gpu"])
        self.assertEqual(errors.TASK_SET_EMPTY, cm.exception.code)

    def test_filtered_but_named_task(self):
        with self.assertRaises(SystemExit) as cm:
            tasks.get_selected_tasks(names=["condition"], filter_tags=["gpu"])
        self.assertEqual(errors.TASK_FILTERED_OUT, cm.exception.code)

    async def test_drop_duplicates(self):
        """Verify that we run() will drop duplicate rows inside an input batch."""
        # Two "A" ids and one "B" id
        self.make_json("Patient.01", "A")
        self.make_json("Patient.02", "A")
        self.make_json("Patient.1", "B")

        await basic_tasks.PatientTask(self.job_config, self.scrubber).run()

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

        await basic_tasks.PatientTask(self.job_config, self.scrubber).run()

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


@ddt.ddt
class TestMedicationRequestTask(TaskTestCase):
    """Test case for MedicationRequestTask, which has some extra logic than normal FHIR resources"""

    async def test_inline_codes(self):
        """Verify that we handle basic normal inline codes (no external fetching) as a baseline"""
        self.make_json("MedicationRequest.1", "InlineCode", medicationCodeableConcept={"text": "Old but checks out"})
        self.make_json("MedicationRequest.2", "NoCode")

        await basic_tasks.MedicationRequestTask(self.job_config, self.scrubber).run()

        self.assertEqual(1, self.format.write_records.call_count)
        df1 = self.format.write_records.call_args[0][0]
        self.assertEqual(
            {
                self.codebook.db.resource_hash("InlineCode"),
                self.codebook.db.resource_hash("NoCode"),
            },
            set(df1.id),
        )

        # Confirm we wrote an empty dataframe to the medication table
        self.assertEqual(1, self.format2.write_records.call_count)
        df2 = self.format2.write_records.call_args[0][0]
        self.assertTrue(df2.empty)

    @mock.patch("cumulus_etl.fhir.download_reference")
    async def test_external_medications(self, mock_download):
        """Verify that we download referenced medications"""
        self.make_json("MedicationRequest.1", "A", medicationReference={"reference": "Medication/123"})
        mock_download.return_value = {"resourceType": "Medication", "id": "med1"}

        await basic_tasks.MedicationRequestTask(self.job_config, self.scrubber).run()

        # Confirm we made both formatters correctly
        self.assertEqual(2, self.create_formatter_mock.call_count)
        self.assertEqual(
            [
                mock.call("medicationrequest", group_field=None, resource_type="MedicationRequest"),
                mock.call("medication", group_field=None, resource_type="Medication"),
            ],
            self.create_formatter_mock.call_args_list,
        )

        # Confirm we wrote the basic MedicationRequest
        self.assertEqual(1, self.format.write_records.call_count)
        df = self.format.write_records.call_args[0][0]
        self.assertEqual([self.codebook.db.resource_hash("A")], list(df.id))
        self.assertEqual(
            f'Medication/{self.codebook.db.resource_hash("123")}', df.iloc[0].medicationReference["reference"]
        )

        # AND that we wrote the downloaded resource!
        self.assertEqual(1, self.format2.write_records.call_count)
        df = self.format2.write_records.call_args[0][0]
        self.assertEqual([self.codebook.db.resource_hash("med1")], list(df.id))  # meds should be scrubbed too

    @mock.patch("cumulus_etl.fhir.download_reference")
    async def test_external_medication_scrubbed(self, mock_download):
        """Verify that we scrub referenced medications as we download them"""
        self.make_json("MedicationRequest.1", "A", medicationReference={"reference": "Medication/123"})
        mock_download.return_value = {
            "resourceType": "Medication",
            "id": "med1",
            "extension": [{"url": "cool-extension"}],
            "identifier": [{"value": "hospital-identifier"}],
            "text": {"div": "<div>html</div>"},
            "status": "active",
        }

        await basic_tasks.MedicationRequestTask(self.job_config, self.scrubber).run()

        # Check result
        self.assertEqual(1, self.format2.write_records.call_count)
        df = self.format2.write_records.call_args[0][0]
        self.assertEqual(
            {
                "resourceType": "Medication",
                "id": self.codebook.db.resource_hash("med1"),
                "status": "active",
            },
            dict(df.iloc[0]),
        )

    @mock.patch("cumulus_etl.fhir.download_reference")
    async def test_external_medications_with_error(self, mock_download):
        """Verify that we record/save download errors"""
        self.make_json("MedicationRequest.1", "A", medicationReference={"reference": "Medication/123"})
        self.make_json("MedicationRequest.2", "B", medicationReference={"reference": "Medication/456"})
        self.make_json("MedicationRequest.3", "C", medicationReference={"reference": "Medication/789"})
        mock_download.side_effect = [  # Fail on first and third
            ValueError("bad hostname"),
            {"resourceType": "Medication", "id": "medB"},
            ValueError("bad haircut"),
        ]

        await basic_tasks.MedicationRequestTask(self.job_config, self.scrubber).run()

        # Confirm we still wrote out all three request resources
        self.assertEqual(1, self.format.write_records.call_count)
        df = self.format.write_records.call_args[0][0]
        self.assertEqual(3, len(df.id))

        # Confirm we still wrote out the medication for B
        self.assertEqual(1, self.format2.write_records.call_count)
        df = self.format2.write_records.call_args[0][0]
        self.assertEqual([self.codebook.db.resource_hash("medB")], list(df.id))

        # And we saved the error?
        med_error_dir = f"{self.errors_dir}/medicationrequest"
        self.assertEqual(["medication-fetch-errors.ndjson"], list(sorted(os.listdir(med_error_dir))))
        self.assertEqual(
            ["A", "C"],  # pre-scrubbed versions of the resources are stored, for easier debugging
            [x["id"] for x in common.read_ndjson(f"{med_error_dir}/medication-fetch-errors.ndjson")],
        )

    @mock.patch("cumulus_etl.fhir.download_reference")
    async def test_external_medications_skips_duplicates(self, mock_download):
        """Verify that we skip medications that are repeated"""
        self.make_json("MedicationRequest.1", "A", medicationReference={"reference": "Medication/dup"})
        self.make_json("MedicationRequest.2", "B", medicationReference={"reference": "Medication/dup"})
        self.make_json("MedicationRequest.3", "C", medicationReference={"reference": "Medication/new"})
        self.job_config.batch_size = 1  # to confirm we detect duplicates even across batches
        mock_download.side_effect = [
            {"resourceType": "Medication", "id": "dup"},
            {"resourceType": "Medication", "id": "new"},
        ]

        await basic_tasks.MedicationRequestTask(self.job_config, self.scrubber).run()

        # Confirm we only called the download method twice
        self.assertEqual(
            [
                mock.call(self.job_config.client, "Medication/dup"),
                mock.call(self.job_config.client, "Medication/new"),
            ],
            mock_download.call_args_list,
        )

        # Confirm we wrote just the downloaded resources, and didn't repeat the dup at all
        self.assertEqual(2, self.format2.write_records.call_count)
        df1 = self.format2.write_records.call_args_list[0][0][0]
        self.assertEqual([self.codebook.db.resource_hash("dup")], list(df1.id))
        df2 = self.format2.write_records.call_args_list[1][0][0]
        self.assertEqual([self.codebook.db.resource_hash("new")], list(df2.id))

    @mock.patch("cumulus_etl.fhir.download_reference")
    async def test_external_medications_skips_unknown_modifiers(self, mock_download):
        """Verify that we skip medications with unknown modifier extensions (unlikely, but still)"""
        self.make_json("MedicationRequest.1", "A", medicationReference={"reference": "Medication/odd"})
        self.make_json("MedicationRequest.2", "B", medicationReference={"reference": "Medication/good"})
        mock_download.side_effect = [
            {
                "resourceType": "Medication",
                "id": "odd",
                "modifierExtension": [{"url": "unrecognized"}],
            },
            {
                "resourceType": "Medication",
                "id": "good",
            },
        ]

        await basic_tasks.MedicationRequestTask(self.job_config, self.scrubber).run()

        self.assertEqual(1, self.format2.write_records.call_count)
        df = self.format2.write_records.call_args[0][0]
        self.assertEqual([self.codebook.db.resource_hash("good")], list(df.id))  # no "odd" written
