"""Tests for etl/tasks/"""

import os
from unittest import mock

import ddt

from cumulus_etl import common, errors
from cumulus_etl.etl.tasks import basic_tasks, task_factory
from tests.etl import TaskTestCase


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
        batch = self.format.write_records.call_args[0][0]
        self.assertEqual([self.codebook.db.patient("0")], [row["id"] for row in batch.rows])

    def test_unknown_task(self):
        with self.assertRaises(SystemExit) as cm:
            task_factory.get_selected_tasks(names=["blarg"])
        self.assertEqual(errors.TASK_UNKNOWN, cm.exception.code)

    def test_over_filtered(self):
        """Verify that we catch when the user filters out all tasks for themselves"""
        with self.assertRaises(SystemExit) as cm:
            task_factory.get_selected_tasks(filter_tags=["cpu", "gpu"])
        self.assertEqual(errors.TASK_SET_EMPTY, cm.exception.code)

    def test_filtered_but_named_task(self):
        with self.assertRaises(SystemExit) as cm:
            task_factory.get_selected_tasks(names=["condition"], filter_tags=["gpu"])
        self.assertEqual(errors.TASK_FILTERED_OUT, cm.exception.code)

    @ddt.data(
        (None, "default"),
        ([], "default"),
        (filter(None, []), "default"),  # iterable, not list
        (["observation", "condition", "procedure"], ["condition", "observation", "procedure"]),  # re-ordered
        (["condition", "patient", "encounter"], ["encounter", "patient", "condition"]),  # encounter and patient first
    )
    @ddt.unpack
    def test_task_selection_ordering(self, user_tasks, expected_tasks):
        """Verify we define the order, not the user, and that encounter & patient are early"""
        names = [t.name for t in task_factory.get_selected_tasks(names=user_tasks)]
        if expected_tasks == "default":
            expected_tasks = [t.name for t in task_factory.get_default_tasks()]
        self.assertEqual(expected_tasks, names)

    async def test_drop_duplicates(self):
        """Verify that we run() will drop duplicate rows inside an input batch."""
        # Two "A" ids and one "B" id
        self.make_json("Patient.01", "A")
        self.make_json("Patient.02", "A")
        self.make_json("Patient.1", "B")

        await basic_tasks.PatientTask(self.job_config, self.scrubber).run()

        # Confirm that only one version of patient A got stored
        self.assertEqual(1, self.format.write_records.call_count)
        batch = self.format.write_records.call_args[0][0]
        self.assertEqual(2, len(batch.rows))
        self.assertEqual(
            {self.codebook.db.patient("A"), self.codebook.db.patient("B")}, {row["id"] for row in batch.rows}
        )

    async def test_batch_write_errors_saved(self):
        self.make_json("Patient.1", "A")
        self.make_json("Patient.2", "B")
        self.make_json("Patient.3", "C")
        self.job_config.batch_size = 1
        self.format = mock.MagicMock(dbname="patient")
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

    async def test_batch_is_given_schema(self):
        """Verify that we calculate a schema for a batch"""
        self.make_json("Patient.1", "A")
        await basic_tasks.PatientTask(self.job_config, self.scrubber).run()

        # Spot check that the schema (from cumulus-fhir-support) exists / looks right
        schema = self.format.write_records.call_args[0][0].schema
        self.assertIn("address", schema.names)
        self.assertIn("id", schema.names)


@ddt.ddt
class TestMedicationRequestTask(TaskTestCase):
    """Test case for MedicationRequestTask, which has some extra logic than normal FHIR resources"""

    async def test_inline_codes(self):
        """Verify that we handle basic normal inline codes (no external fetching) as a baseline"""
        self.make_json("MedicationRequest.1", "InlineCode", medicationCodeableConcept={"text": "Old but checks out"})
        self.make_json("MedicationRequest.2", "NoCode")

        await basic_tasks.MedicationRequestTask(self.job_config, self.scrubber).run()

        self.assertEqual(1, self.format.write_records.call_count)
        batch = self.format.write_records.call_args[0][0]
        self.assertEqual(
            {
                self.codebook.db.resource_hash("InlineCode"),
                self.codebook.db.resource_hash("NoCode"),
            },
            {row["id"] for row in batch.rows},
        )

        # Confirm we wrote an empty dataframe to the medication table
        self.assertEqual(1, self.format2.write_records.call_count)
        batch = self.format2.write_records.call_args[0][0]
        self.assertEqual([], batch.rows)

    async def test_contained_medications(self):
        """Verify that we pass it through and don't blow up"""
        self.make_json("MedicationRequest.1", "A", medicationReference={"reference": "#123"})

        await basic_tasks.MedicationRequestTask(self.job_config, self.scrubber).run()

        # Confirm we wrote the basic MedicationRequest
        self.assertEqual(1, self.format.write_records.call_count)
        batch = self.format.write_records.call_args[0][0]
        self.assertEqual(f'#{self.codebook.db.resource_hash("123")}', batch.rows[0]["medicationReference"]["reference"])

        # Confirm we wrote an empty dataframe to the medication table
        self.assertEqual(1, self.format2.write_records.call_count)
        batch = self.format2.write_records.call_args[0][0]
        self.assertEqual(0, len(batch.rows))

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
        batch = self.format.write_records.call_args[0][0]
        self.assertEqual([self.codebook.db.resource_hash("A")], [row["id"] for row in batch.rows])
        self.assertEqual(
            f'Medication/{self.codebook.db.resource_hash("123")}',
            batch.rows[0]["medicationReference"]["reference"],
        )

        # AND that we wrote the downloaded resource!
        self.assertEqual(1, self.format2.write_records.call_count)
        batch = self.format2.write_records.call_args[0][0]
        self.assertEqual(
            [self.codebook.db.resource_hash("med1")], [row["id"] for row in batch.rows]
        )  # meds should be scrubbed too

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
        batch = self.format2.write_records.call_args[0][0]
        self.assertEqual(
            {
                "resourceType": "Medication",
                "id": self.codebook.db.resource_hash("med1"),
                "status": "active",
            },
            common.sparse_dict(batch.rows[0]),
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
        batch = self.format.write_records.call_args[0][0]
        self.assertEqual(3, len(batch.rows))

        # Confirm we still wrote out the medication for B
        self.assertEqual(1, self.format2.write_records.call_count)
        batch = self.format2.write_records.call_args[0][0]
        self.assertEqual([self.codebook.db.resource_hash("medB")], [row["id"] for row in batch.rows])

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
        batch = self.format2.write_records.call_args_list[0][0][0]
        self.assertEqual([self.codebook.db.resource_hash("dup")], [row["id"] for row in batch.rows])
        batch = self.format2.write_records.call_args_list[1][0][0]
        self.assertEqual([self.codebook.db.resource_hash("new")], [row["id"] for row in batch.rows])

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
        batch = self.format2.write_records.call_args[0][0]
        self.assertEqual([self.codebook.db.resource_hash("good")], [row["id"] for row in batch.rows])  # no "odd"
