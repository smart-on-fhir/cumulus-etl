"""Tests for etl/tasks/"""

import os
from unittest import mock

import cumulus_fhir_support
import ddt

from cumulus_etl import common, errors
from cumulus_etl.etl import tasks
from cumulus_etl.etl.tasks import basic_tasks, task_factory
from tests.etl import TaskTestCase


@ddt.ddt
class TestTasks(TaskTestCase):
    """Test case for basic task methods"""

    def test_read_ndjson(self):
        """Verify we can read a dir of ndjson"""
        self.make_json("Condition", "11")
        self.make_json("Condition", "12")
        self.make_json("Condition", "13")
        self.make_json("Patient", "14")

        resources = basic_tasks.ConditionTask(self.job_config, self.scrubber).read_ndjson()
        self.assertEqual({"11", "12", "13"}, {r["id"] for r in resources})

        resources = basic_tasks.PatientTask(self.job_config, self.scrubber).read_ndjson()
        self.assertEqual({"14"}, {r["id"] for r in resources})

        resources = basic_tasks.EncounterTask(self.job_config, self.scrubber).read_ndjson()
        self.assertEqual([], list(resources))

    @ddt.data(
        {"description": "not a FHIR Condition"},  # dict, but not a FHIR object
        {"resourceType": "Patient", "id": "patient"},  # wrong FHIR type
        "hello world",  # not even a dict
    )
    async def test_read_bogus_ndjson(self, bogus_row):
        """
        Verify we complain about invalid JSON found.

        We've seen this situation when folks try to combine NDJSON results themselves.
        (They got OperationOutcomes mixed into Conditions.)
        """
        with common.NdjsonWriter(f"{self.input_dir}/conditions.ndjson") as writer:
            # First line looks normal, so our sniffer will think this is a Condition file
            writer.write({"resourceType": "Condition", "id": "normal"})
            writer.write(bogus_row)

        with self.assertLogs(level="WARNING") as logs:
            await basic_tasks.ConditionTask(self.job_config, self.scrubber).run()

        self.assertEqual(
            logs.output, [f"WARNING:root:Encountered invalid or unexpected FHIR: `{bogus_row}`"]
        )

    async def test_unknown_modifier_extensions_skipped_for_patients(self):
        """Verify we ignore unknown modifier extensions during a normal task (like patients)"""
        self.make_json("Patient", "0")
        self.make_json("Patient", "1", modifierExtension=[{"url": "unrecognized"}])

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
        # re-ordered
        (["observation", "condition", "procedure"], ["condition", "observation", "procedure"]),
        # encounter and patient first
        (["condition", "patient", "encounter"], ["encounter", "patient", "condition"]),
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
        self.make_json("Patient", "A", birthDate="2020")
        self.make_json("Patient", "A", birthDate="2021")
        self.make_json("Patient", "B")

        await basic_tasks.PatientTask(self.job_config, self.scrubber).run()

        # Confirm that only the later version of patient A got stored
        self.assertEqual(1, self.format.write_records.call_count)
        batch = self.format.write_records.call_args[0][0]
        self.assertEqual(
            batch.rows,
            [  # Output ordering is guaranteed to be stable
                {
                    "resourceType": "Patient",
                    "id": self.codebook.db.patient("A"),
                    "birthDate": "2021",  # the row that came later won
                },
                {
                    "resourceType": "Patient",
                    "id": self.codebook.db.patient("B"),
                },
            ],
        )

    async def test_batch_write_errors_saved(self):
        self.make_json("Patient", "A")
        self.make_json("Patient", "B")
        self.make_json("Patient", "C")
        self.job_config.batch_size = 1
        self.format = mock.MagicMock(dbname="patient")
        self.format.write_records.side_effect = [False, True, False]  # First and third will fail

        await basic_tasks.PatientTask(self.job_config, self.scrubber).run()

        self.assertEqual(
            ["write-error.000.ndjson", "write-error.002.ndjson"],
            list(sorted(os.listdir(f"{self.errors_dir}/patient"))),
        )
        self.assertEqual(
            {
                "resourceType": "Patient",
                "id": "30d95f17d9f51f3a151c51bf0a7fcb1717363f3a87d2dbace7d594ee68d3a82f",
            },
            common.read_json(f"{self.errors_dir}/patient/write-error.000.ndjson"),
        )
        self.assertEqual(
            {
                "resourceType": "Patient",
                "id": "ed9ab553005a7c9bdb26ecf9f612ea996ad99b1a96a34bf88c260f1c901d8289",
            },
            common.read_json(f"{self.errors_dir}/patient/write-error.002.ndjson"),
        )

    async def test_batch_is_given_schema(self):
        """Verify that we calculate a schema for a batch"""
        self.make_json("Patient", "A")
        await basic_tasks.PatientTask(self.job_config, self.scrubber).run()

        # Spot check that the schema (from cumulus-fhir-support) exists / looks right
        schema = self.format.write_records.call_args[0][0].schema
        self.assertIn("address", schema.names)
        self.assertIn("id", schema.names)

    async def test_get_schema(self):
        """Verify that Task.get_schema() works for resources and non-resources"""
        schema = tasks.EtlTask.get_schema("Patient", [])
        self.assertIn("gender", schema.names)
        schema = tasks.EtlTask.get_schema(None, [])
        self.assertIsNone(schema)

    async def test_prepare_can_skip_task(self):
        """Verify that if prepare_task returns false, we skip the task"""
        self.make_json("Patient", "A")
        with mock.patch(
            "cumulus_etl.etl.tasks.basic_tasks.PatientTask.prepare_task", return_value=False
        ):
            summaries = await basic_tasks.PatientTask(self.job_config, self.scrubber).run()
        self.assertEqual(len(summaries), 1)
        self.assertEqual(summaries[0].attempt, 0)
        self.assertIsNone(self.format)

    async def test_deleted_ids_no_op(self):
        """Verify that we don't try to delete IDs if none are given"""
        # Just a simple test to confirm we don't even ask the formatter to consider
        # deleting any IDs if we weren't given any.
        await basic_tasks.PatientTask(self.job_config, self.scrubber).run()
        self.assertEqual(self.format.delete_records.call_count, 0)

    async def test_deleted_ids(self):
        """Verify that we send deleted IDs down to the formatter"""
        self.job_config.deleted_ids = {"Patient": {"p1", "p2"}}
        await basic_tasks.PatientTask(self.job_config, self.scrubber).run()

        self.assertEqual(self.format.delete_records.call_count, 1)
        ids = self.format.delete_records.call_args[0][0]
        self.assertEqual(
            ids,
            {
                self.codebook.db.resource_hash("p1"),
                self.codebook.db.resource_hash("p2"),
            },
        )


@ddt.ddt
class TestTaskCompletion(TaskTestCase):
    """Tests for etl__completion* handling"""

    async def test_encounter_completion(self):
        """Verify that we write out completion data correctly"""
        self.make_json("Encounter", "FirstBatch.A")
        self.make_json("Encounter", "FirstBatch.B")
        self.make_json("Encounter", "SecondBatch.C")
        self.job_config.batch_size = 4  # two encounters at a time (each encounter makes 2 rows)

        await basic_tasks.EncounterTask(self.job_config, self.scrubber).run()

        comp_enc_format = self.format  # etl__completion_encounters
        enc_format = self.format2  # encounter
        comp_format = self.format3  # etl__completion

        self.assertEqual("etl__completion_encounters", comp_enc_format.dbname)
        self.assertEqual({"encounter_id", "group_name"}, comp_enc_format.uniqueness_fields)
        self.assertFalse(comp_enc_format.update_existing)

        self.assertEqual("etl__completion", comp_format.dbname)
        self.assertEqual(
            {"table_name", "group_name", "export_time", "etl_time"}, comp_format.uniqueness_fields
        )
        self.assertTrue(comp_format.update_existing)

        self.assertEqual(2, comp_enc_format.write_records.call_count)
        self.assertEqual(2, enc_format.write_records.call_count)
        self.assertEqual(1, comp_format.write_records.call_count)

        comp_enc_batches = [call[0][0] for call in comp_enc_format.write_records.call_args_list]
        self.assertEqual(
            [
                {
                    "encounter_id": self.codebook.db.encounter("FirstBatch.A"),
                    "group_name": "test-group",
                    "export_time": "2012-10-10T05:30:12+00:00",
                },
                {
                    "encounter_id": self.codebook.db.encounter("FirstBatch.B"),
                    "group_name": "test-group",
                    "export_time": "2012-10-10T05:30:12+00:00",
                },
            ],
            comp_enc_batches[0].rows,
        )
        self.assertEqual(
            [
                {
                    "encounter_id": self.codebook.db.encounter("SecondBatch.C"),
                    "group_name": "test-group",
                    "export_time": "2012-10-10T05:30:12+00:00",
                }
            ],
            comp_enc_batches[1].rows,
        )

        comp_batch = comp_format.write_records.call_args[0][0]
        self.assertEqual(
            [
                {
                    "table_name": "encounter",
                    "group_name": "test-group",
                    "export_time": "2012-10-10T05:30:12+00:00",
                    "export_url": self.export_url,
                    "etl_version": "1.0.0+test",
                    "etl_time": "2021-09-14T21:23:45+00:00",
                }
            ],
            comp_batch.rows,
        )

    async def test_medication_completion(self):
        """
        Verify that we write out Medication completion too.

        We just want to verify that we handle multi-output tasks.
        """
        self.make_json("MedicationRequest", "A")

        await basic_tasks.MedicationRequestTask(self.job_config, self.scrubber).run()

        med_req_format = self.format  # MedicationRequest
        med_format = self.format2  # Medication (second because no content, touched in finalize)
        comp_format = self.format3  # etl__completion

        self.assertEqual("medication", med_format.dbname)
        self.assertEqual("medicationrequest", med_req_format.dbname)
        self.assertEqual("etl__completion", comp_format.dbname)

        self.assertEqual(1, med_format.write_records.call_count)
        self.assertEqual(1, med_req_format.write_records.call_count)
        self.assertEqual(1, comp_format.write_records.call_count)

        comp_batch = comp_format.write_records.call_args[0][0]
        self.assertEqual(
            [
                {
                    "table_name": "medication",
                    "group_name": "test-group",
                    "export_time": "2012-10-10T05:30:12+00:00",
                    "export_url": self.export_url,
                    "etl_version": "1.0.0+test",
                    "etl_time": "2021-09-14T21:23:45+00:00",
                },
                {
                    "table_name": "medicationrequest",
                    "group_name": "test-group",
                    "export_time": "2012-10-10T05:30:12+00:00",
                    "export_url": self.export_url,
                    "etl_version": "1.0.0+test",
                    "etl_time": "2021-09-14T21:23:45+00:00",
                },
            ],
            comp_batch.rows,
        )

    async def test_allow_empty_group(self):
        """Empty groups are (rarely) used to mark a server-wide global export"""
        self.make_json("Device", "A")
        self.job_config.export_group_name = ""

        await basic_tasks.DeviceTask(self.job_config, self.scrubber).run()

        comp_format = self.format2  # etl__completion

        self.assertEqual(1, comp_format.write_records.call_count)
        self.assertEqual(
            [
                {
                    "table_name": "device",
                    "group_name": "",
                    "export_time": "2012-10-10T05:30:12+00:00",
                    "export_url": self.export_url,
                    "etl_version": "1.0.0+test",
                    "etl_time": "2021-09-14T21:23:45+00:00",
                }
            ],
            comp_format.write_records.call_args[0][0].rows,
        )

    async def test_error_during_write(self):
        """We should flag the task as failed if we can't write completion"""

        # Change the way we mock formatters to insert an error
        def make_formatter(dbname: str, **kwargs):
            mock_formatter = mock.MagicMock(dbname=dbname, **kwargs)
            if dbname == "etl__completion":
                mock_formatter.write_records.return_value = False
            return mock_formatter

        self.job_config.create_formatter = mock.MagicMock(side_effect=make_formatter)

        self.make_json("Device", "A")
        summaries = await basic_tasks.DeviceTask(self.job_config, self.scrubber).run()
        self.assertTrue(summaries[0].had_errors)


@ddt.ddt
class TestMedicationRequestTask(TaskTestCase):
    """Test case for MedicationRequestTask, which has some extra logic than normal FHIR resources"""

    async def test_inline_codes(self):
        """Verify that we handle basic normal inline codes (no external fetching) as a baseline"""
        self.make_json(
            "MedicationRequest",
            "InlineCode",
            medicationCodeableConcept={"text": "Old but checks out"},
        )
        self.make_json("MedicationRequest", "NoCode")

        await basic_tasks.MedicationRequestTask(self.job_config, self.scrubber).run()

        med_req_format = self.format
        med_format = self.format2  # second because it's empty

        self.assertEqual(1, med_req_format.write_records.call_count)
        batch = med_req_format.write_records.call_args[0][0]
        self.assertEqual(
            {
                self.codebook.db.resource_hash("InlineCode"),
                self.codebook.db.resource_hash("NoCode"),
            },
            {row["id"] for row in batch.rows},
        )

        # Confirm we wrote an empty dataframe to the medication table
        self.assertEqual(1, med_format.write_records.call_count)
        batch = med_format.write_records.call_args[0][0]
        self.assertEqual([], batch.rows)

    async def test_contained_medications(self):
        """Verify that we pass it through and don't blow up"""
        self.make_json("MedicationRequest", "A", medicationReference={"reference": "#123"})

        await basic_tasks.MedicationRequestTask(self.job_config, self.scrubber).run()

        med_req_format = self.format
        med_format = self.format2  # second because it's empty

        # Confirm we wrote the basic MedicationRequest
        self.assertEqual(1, med_req_format.write_records.call_count)
        batch = med_req_format.write_records.call_args[0][0]
        self.assertEqual(
            f"#{self.codebook.db.resource_hash('123')}",
            batch.rows[0]["medicationReference"]["reference"],
        )

        # Confirm we wrote an empty dataframe to the medication table
        self.assertEqual(1, med_format.write_records.call_count)
        batch = med_format.write_records.call_args[0][0]
        self.assertEqual(0, len(batch.rows))

    @mock.patch("cumulus_etl.fhir.download_reference")
    async def test_external_medications(self, mock_download):
        """Verify that we download referenced medications"""
        self.make_json(
            "MedicationRequest", "A", medicationReference={"reference": "Medication/123"}
        )
        mock_download.return_value = {"resourceType": "Medication", "id": "med1"}

        await basic_tasks.MedicationRequestTask(self.job_config, self.scrubber).run()

        med_format = self.format
        med_req_format = self.format2

        # Confirm we made all table formatters correctly
        self.assertEqual(3, self.create_formatter_mock.call_count)
        self.assertEqual(
            [
                mock.call(
                    "medication",
                    group_field=None,
                    uniqueness_fields=None,
                    update_existing=True,
                ),
                mock.call(
                    "medicationrequest",
                    group_field=None,
                    uniqueness_fields=None,
                    update_existing=True,
                ),
                mock.call(
                    dbname="etl__completion",
                    uniqueness_fields={"group_name", "table_name", "export_time", "etl_time"},
                ),
            ],
            self.create_formatter_mock.call_args_list,
        )

        # Confirm we wrote the basic MedicationRequest
        self.assertEqual(1, med_req_format.write_records.call_count)
        batch = med_req_format.write_records.call_args[0][0]
        self.assertEqual([self.codebook.db.resource_hash("A")], [row["id"] for row in batch.rows])
        self.assertEqual(
            f"Medication/{self.codebook.db.resource_hash('123')}",
            batch.rows[0]["medicationReference"]["reference"],
        )

        # AND that we wrote the downloaded resource!
        self.assertEqual(1, med_format.write_records.call_count)
        batch = med_format.write_records.call_args[0][0]
        self.assertEqual(
            [self.codebook.db.resource_hash("med1")], [row["id"] for row in batch.rows]
        )  # meds should be scrubbed too

    @mock.patch("cumulus_etl.fhir.download_reference")
    async def test_external_medication_scrubbed(self, mock_download):
        """Verify that we scrub referenced medications as we download them"""
        self.make_json(
            "MedicationRequest", "A", medicationReference={"reference": "Medication/123"}
        )
        mock_download.return_value = {
            "resourceType": "Medication",
            "id": "med1",
            "extension": [{"url": "cool-extension"}],
            "identifier": [{"value": "hospital-identifier"}],
            "text": {"div": "<div>html</div>"},
            "status": "active",
        }

        await basic_tasks.MedicationRequestTask(self.job_config, self.scrubber).run()

        med_format = self.format

        # Check result
        self.assertEqual(1, med_format.write_records.call_count)
        batch = med_format.write_records.call_args[0][0]
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
        self.make_json(
            "MedicationRequest", "A", medicationReference={"reference": "Medication/123"}
        )
        self.make_json(
            "MedicationRequest", "B", medicationReference={"reference": "Medication/456"}
        )
        self.make_json(
            "MedicationRequest", "C", medicationReference={"reference": "Medication/789"}
        )
        mock_download.side_effect = [  # Fail on first and third
            ValueError("bad hostname"),
            {"resourceType": "Medication", "id": "medB"},
            ValueError("bad haircut"),
        ]

        with self.assertLogs(level="WARNING") as logs:
            await basic_tasks.MedicationRequestTask(self.job_config, self.scrubber).run()

        med_format = self.format
        med_req_format = self.format2

        # Confirm we still wrote out all three request resources
        self.assertEqual(1, med_req_format.write_records.call_count)
        batch = med_req_format.write_records.call_args[0][0]
        self.assertEqual(3, len(batch.rows))

        # Confirm we only logged about the first problem (to avoid spamming console)
        self.assertEqual(
            logs.output, ["WARNING:root:Could not download Medication reference: bad hostname"]
        )

        # Confirm we still wrote out the medication for B
        self.assertEqual(1, med_format.write_records.call_count)
        batch = med_format.write_records.call_args[0][0]
        self.assertEqual(
            [self.codebook.db.resource_hash("medB")], [row["id"] for row in batch.rows]
        )

        # And we saved the error?
        med_error_dir = f"{self.errors_dir}/medicationrequest"
        self.assertEqual(
            ["medication-fetch-errors.ndjson"], list(sorted(os.listdir(med_error_dir)))
        )
        self.assertEqual(
            ["A", "C"],  # pre-scrubbed versions of the resources are stored, for easier debugging
            [
                x["id"]
                for x in cumulus_fhir_support.read_multiline_json(
                    f"{med_error_dir}/medication-fetch-errors.ndjson"
                )
            ],
        )

    @mock.patch("cumulus_etl.fhir.download_reference")
    async def test_external_medications_skips_duplicates(self, mock_download):
        """Verify that we skip medications that are repeated"""
        self.make_json(
            "MedicationRequest", "A", medicationReference={"reference": "Medication/dup"}
        )
        self.make_json(
            "MedicationRequest", "B", medicationReference={"reference": "Medication/dup"}
        )
        self.make_json(
            "MedicationRequest", "C", medicationReference={"reference": "Medication/new"}
        )
        self.job_config.batch_size = 1  # to confirm we detect duplicates even across batches
        mock_download.side_effect = [
            {"resourceType": "Medication", "id": "dup"},
            {"resourceType": "Medication", "id": "new"},
        ]

        await basic_tasks.MedicationRequestTask(self.job_config, self.scrubber).run()

        med_format = self.format

        # Confirm we only called the download method twice
        self.assertEqual(
            [
                mock.call(self.job_config.client, "Medication/dup"),
                mock.call(self.job_config.client, "Medication/new"),
            ],
            mock_download.call_args_list,
        )

        # Confirm we wrote just the downloaded resources, and didn't repeat the dup at all
        self.assertEqual(2, med_format.write_records.call_count)
        batch = med_format.write_records.call_args_list[0][0][0]
        self.assertEqual([self.codebook.db.resource_hash("dup")], [row["id"] for row in batch.rows])
        batch = med_format.write_records.call_args_list[1][0][0]
        self.assertEqual([self.codebook.db.resource_hash("new")], [row["id"] for row in batch.rows])

    @mock.patch("cumulus_etl.fhir.download_reference")
    async def test_external_medications_skips_unknown_modifiers(self, mock_download):
        """Verify that we skip medications with unknown modifier extensions (unlikely, but still)"""
        self.make_json(
            "MedicationRequest", "A", medicationReference={"reference": "Medication/odd"}
        )
        self.make_json(
            "MedicationRequest", "B", medicationReference={"reference": "Medication/good"}
        )
        self.make_json(
            "MedicationRequest",
            "Skipped",  # this will be ignored due to uknown modifier in the MedReq itself
            modifierExtension=[{"url": "unrecognized"}],
        )
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

        med_format = self.format  # Medication
        medreq_format = self.format2  # MedicationRequest

        self.assertEqual(1, medreq_format.write_records.call_count)
        batch = medreq_format.write_records.call_args[0][0]
        self.assertEqual(  # no "Skipped"
            {self.codebook.db.resource_hash("A"), self.codebook.db.resource_hash("B")},
            {row["id"] for row in batch.rows},
        )

        self.assertEqual(1, med_format.write_records.call_count)
        batch = med_format.write_records.call_args[0][0]
        self.assertEqual(  # no "odd"
            [self.codebook.db.resource_hash("good")], [row["id"] for row in batch.rows]
        )

    async def test_adjacent_medications(self):
        """Verify that we grab Medication resources that are in same folder"""
        self.make_json("Medication", "A")
        self.make_json("Medication", "B")  # a non-referenced Med - will still be pushed up
        self.make_json("MedicationRequest", "A", medicationReference={"reference": "Medication/A"})

        # Confirm we didn't warn about not being able to download resources
        with self.assertNoLogs(level="WARNING"):
            await basic_tasks.MedicationRequestTask(self.job_config, self.scrubber).run()

        med_format = self.format
        med_req_format = self.format2

        # Confirm we wrote out the Medications
        self.assertEqual(1, med_format.write_records.call_count)
        batch = med_format.write_records.call_args[0][0]
        self.assertEqual(
            {r["id"] for r in batch.rows},
            {self.codebook.db.resource_hash("A"), self.codebook.db.resource_hash("B")},
        )

        # Confirm we wrote the MedicationRequest (and never asked to download it, or we would have
        # hit an error when making the network request)
        self.assertEqual(1, med_req_format.write_records.call_count)
        batch = med_req_format.write_records.call_args[0][0]
        self.assertEqual(batch.rows[0]["id"], self.codebook.db.resource_hash("A"))
