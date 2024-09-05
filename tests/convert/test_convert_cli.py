"""Tests for etl/convert/cli.py"""

import os
import shutil
import tempfile
from unittest import mock

import ddt

from cumulus_etl import cli, common, errors
from tests import s3mock, utils


class ConvertTestsBase(utils.AsyncTestCase):
    """Base class for convert tests"""

    def setUp(self):
        super().setUp()

        self.tmpdir = tempfile.mkdtemp()
        # Comment out this next line when debugging, to persist directory
        self.addCleanup(shutil.rmtree, self.tmpdir)

        self.original_path = os.path.join(self.tmpdir, "original")
        self.target_path = os.path.join(self.tmpdir, "target")

    async def run_convert(
        self, input_path: str | None = None, output_path: str | None = None
    ) -> None:
        args = [
            "convert",
            input_path or self.original_path,
            output_path or self.target_path,
        ]
        await cli.main(args)


@ddt.ddt
class TestConvert(ConvertTestsBase):
    """Tests for high-level convert support."""

    def prepare_original_dir(self) -> str:
        """Returns the job timestamp used, for easier inspection"""
        # Fill in original dir, including a non-default output folder
        shutil.copytree(f"{self.datadir}/simple/output", self.original_path)
        shutil.copytree(
            f"{self.datadir}/covid/term-exists/covid_symptom__nlp_results_term_exists",
            f"{self.original_path}/covid_symptom__nlp_results_term_exists",
        )
        shutil.copyfile(
            f"{self.datadir}/covid/term-exists/etl__completion/etl__completion.000.ndjson",
            f"{self.original_path}/etl__completion/etl__completion.covid.ndjson",
        )
        # just to confirm we only copy what we understand, add an ignored folder
        os.makedirs(f"{self.original_path}/ignored")

        job_timestamp = "2023-02-28__19.53.08"
        config_dir = f"{self.original_path}/JobConfig/{job_timestamp}"
        os.makedirs(config_dir)
        common.write_json(f"{config_dir}/job_config.json", {"test": True})

        return job_timestamp

    async def test_input_dir_must_exist(self):
        """Verify that the input dir must already exist"""
        with self.assertRaises(SystemExit) as cm:
            await self.run_convert()
        self.assertEqual(errors.ARGS_INVALID, cm.exception.code)

    async def test_input_dir_must_look_real(self):
        """Verify that the input dir looks like an output dir"""
        os.makedirs(f"{self.original_path}/patient")
        with self.assertRaises(SystemExit) as cm:
            await self.run_convert()
        self.assertEqual(errors.ARGS_INVALID, cm.exception.code)

        # Confirm that creating JobConfig will fix the error
        os.makedirs(f"{self.original_path}/JobConfig")
        await self.run_convert()

    async def test_happy_path(self):
        """Verify that basic conversions work, first on empty target then updating that now-occupied target"""
        # Do first conversion
        job_timestamp = self.prepare_original_dir()
        await self.run_convert()

        # Test first conversion results
        expected_tables = set(os.listdir(self.original_path)) - {"ignored"}
        self.assertEqual(expected_tables, set(os.listdir(self.target_path)))
        self.assertEqual(
            {"test": True},
            common.read_json(f"{self.target_path}/JobConfig/{job_timestamp}/job_config.json"),
        )
        patients = utils.read_delta_lake(f"{self.target_path}/patient")  # spot check some patients
        self.assertEqual(2, len(patients))
        self.assertEqual("1de9ea66-70d3-da1f-c735-df5ef7697fb9", patients[0]["id"])
        self.assertEqual("1982", patients[0]["birthDate"])
        self.assertEqual("1983", patients[1]["birthDate"])
        conditions = utils.read_delta_lake(f"{self.target_path}/condition")  # and conditions
        self.assertEqual(2, len(conditions))
        self.assertEqual("2010-03-02", conditions[0]["recordedDate"])
        # and a non-default study table
        symptoms = utils.read_delta_lake(
            f"{self.target_path}/covid_symptom__nlp_results_term_exists"
        )
        self.assertEqual(2, len(symptoms))
        self.assertEqual("for", symptoms[0]["match"]["text"])
        completion = utils.read_delta_lake(f"{self.target_path}/etl__completion")  # and completion
        self.assertEqual(14, len(completion))
        self.assertEqual("allergyintolerance", completion[0]["table_name"])
        comp_enc = utils.read_delta_lake(f"{self.target_path}/etl__completion_encounters")
        self.assertEqual(2, len(comp_enc))
        self.assertEqual("08f0ebd4-950c-ddd9-ce97-b5bdf073eed1", comp_enc[0]["encounter_id"])
        self.assertEqual("2020-10-13T12:00:20-05:00", comp_enc[0]["export_time"])

        # Now make a second small, partial output folder to layer into the existing Delta Lake
        delta_timestamp = "2023-02-29__19.53.08"
        delta_path = os.path.join(self.tmpdir, "delta")
        os.makedirs(f"{delta_path}/patient")
        with common.NdjsonWriter(f"{delta_path}/patient/new.ndjson") as writer:
            writer.write(
                {
                    "resourceType": "Patient",
                    "id": "1de9ea66-70d3-da1f-c735-df5ef7697fb9",
                    "birthDate": "1800",
                }
            )
            writer.write({"resourceType": "Patient", "id": "z-gen", "birthDate": "2005"})
        os.makedirs(f"{delta_path}/etl__completion_encounters")
        with common.NdjsonWriter(f"{delta_path}/etl__completion_encounters/new.ndjson") as writer:
            # Newer timestamp for the existing row
            writer.write(
                {
                    "encounter_id": "08f0ebd4-950c-ddd9-ce97-b5bdf073eed1",
                    "group_name": "test-group",
                    "export_time": "2021-10-13T17:00:20+00:00",
                }
            )
            # Totally new encounter
            writer.write(
                {
                    "encounter_id": "NEW",
                    "group_name": "NEW",
                    "export_time": "2021-12-12T17:00:20+00:00",
                }
            )
        delta_config_dir = f"{delta_path}/JobConfig/{delta_timestamp}"
        os.makedirs(delta_config_dir)
        common.write_json(f"{delta_config_dir}/job_config.json", {"delta": "yup"})
        await self.run_convert(input_path=delta_path)

        # How did that change the delta lake dir? Hopefully we only interwove the new data
        self.assertEqual(  # confirm this is still here
            {"test": True},
            common.read_json(f"{self.target_path}/JobConfig/{job_timestamp}/job_config.json"),
        )
        self.assertEqual(
            {"delta": "yup"},
            common.read_json(f"{self.target_path}/JobConfig/{delta_timestamp}/job_config.json"),
        )
        patients = utils.read_delta_lake(f"{self.target_path}/patient")  # re-check the patients
        self.assertEqual(3, len(patients))
        # these rows are sorted by id, so these are reliable indexes
        self.assertEqual("1800", patients[0]["birthDate"])
        self.assertEqual("1983", patients[1]["birthDate"])
        self.assertEqual("2005", patients[2]["birthDate"])
        # and conditions shouldn't change at all
        conditions = utils.read_delta_lake(f"{self.target_path}/condition")
        self.assertEqual(2, len(conditions))
        self.assertEqual("2010-03-02", conditions[0]["recordedDate"])
        # but *some* enc mappings did
        comp_enc = utils.read_delta_lake(f"{self.target_path}/etl__completion_encounters")
        self.assertEqual(3, len(comp_enc))
        self.assertEqual("08f0ebd4-950c-ddd9-ce97-b5bdf073eed1", comp_enc[0]["encounter_id"])
        # confirm export_time *didn't* get updated
        self.assertEqual("2020-10-13T12:00:20-05:00", comp_enc[0]["export_time"])
        self.assertEqual("NEW", comp_enc[1]["encounter_id"])
        # but the new row did get inserted
        self.assertEqual("2021-12-12T17:00:20+00:00", comp_enc[1]["export_time"])

    @mock.patch("cumulus_etl.formats.Format.write_records")
    async def test_batch_metadata(self, mock_write):
        """Verify that we pass along per-batch metadata like groups"""
        # Set up input path
        shutil.copytree(  # First, one table that has no metadata
            f"{self.datadir}/simple/output/patient",
            f"{self.original_path}/patient",
        )
        shutil.copytree(  # Then, one that does
            f"{self.datadir}/covid/output/covid_symptom__nlp_results",
            f"{self.original_path}/covid_symptom__nlp_results",
        )
        # And make a second batch, to confirm we read each meta file
        common.write_json(
            f"{self.original_path}/covid_symptom__nlp_results/covid_symptom__nlp_results.001.meta",
            # Reference a group that doesn't exist to prove we are reading this file and not just pooling group_fields
            # that we see in the data.
            {"groups": ["nonexistent"]},
        )
        common.write_json(
            f"{self.original_path}/covid_symptom__nlp_results/covid_symptom__nlp_results.001.ndjson",
            {"id": "D1.0", "docref_id": "D1"},
        )
        os.makedirs(f"{self.original_path}/JobConfig")

        # Run conversion
        await self.run_convert()

        # Test results
        self.assertEqual(3, mock_write.call_count)
        self.assertEqual(set(), mock_write.call_args_list[0][0][0].groups)  # patients
        self.assertEqual(
            {
                "c31a3dbf188ed241b2c06b2475cd56159017fa1df1ea882d3fc4beab860fc24d",
                "eb30741bbb9395fc3da72d02fd29b96e2e4c0c2592c3ae997d80bf522c80070e",
            },
            mock_write.call_args_list[1][0][0].groups,  # first (actual) covid batch
        )
        # second (faked) covid batch
        self.assertEqual({"nonexistent"}, mock_write.call_args_list[2][0][0].groups)

    @mock.patch("cumulus_etl.formats.Format.write_records")
    @mock.patch("cumulus_etl.formats.deltalake.DeltaLakeFormat.delete_records")
    async def test_deleted_ids(self, mock_delete, mock_write):
        """Verify that we pass along deleted IDs in the table metadata"""
        # Set up input path
        shutil.copytree(  # First, one table that has no metadata
            f"{self.datadir}/simple/output/patient",
            f"{self.original_path}/patient",
        )
        shutil.copytree(  # Then, one that will
            f"{self.datadir}/simple/output/condition",
            f"{self.original_path}/condition",
        )
        common.write_json(f"{self.original_path}/condition/condition.meta", {"deleted": ["a", "b"]})
        os.makedirs(f"{self.original_path}/JobConfig")

        # Run conversion
        await self.run_convert()

        # Verify results
        self.assertEqual(mock_write.call_count, 2)
        self.assertEqual(mock_delete.call_count, 1)
        self.assertEqual(mock_delete.call_args, mock.call({"a", "b"}))


class TestConvertOnS3(s3mock.S3Mixin, ConvertTestsBase):
    @mock.patch("cumulus_etl.formats.Format.write_records")
    async def test_convert_from_s3(self, mock_write):
        """Quick test that we can read from an arbitrary input dir using fsspec"""
        # Set up input
        common.write_json(
            f"{self.bucket_url}/JobConfig/2024-08-09__16.32.51/job_config.json",
            {"comment": "unittest"},
        )
        common.write_json(
            f"{self.bucket_url}/condition/condition.000.ndjson",
            {"id": "con1"},
        )

        # Run conversion
        await self.run_convert(input_path=self.bucket_url)

        # Verify results
        self.assertEqual(mock_write.call_count, 1)
        self.assertEqual([{"id": "con1"}], mock_write.call_args[0][0].rows)
        print(os.listdir(f"{self.target_path}"))
        self.assertEqual(
            common.read_json(f"{self.target_path}/JobConfig/2024-08-09__16.32.51/job_config.json"),
            {"comment": "unittest"},
        )
