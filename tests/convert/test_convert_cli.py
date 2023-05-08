"""Tests for etl/convert/cli.py"""

import os
import shutil
import tempfile

import ddt

from cumulus_etl import cli, common, errors
from cumulus_etl.etl import tasks

from tests import utils


@ddt.ddt
class TestConvert(utils.AsyncTestCase):
    """Tests for high-level convert support."""

    def setUp(self):
        super().setUp()

        self.tmpdir = tempfile.mkdtemp()
        # Comment out this next line when debugging, to persist directory
        self.addCleanup(shutil.rmtree, self.tmpdir)

        self.original_path = os.path.join(self.tmpdir, "original")
        self.target_path = os.path.join(self.tmpdir, "target")

    def prepare_original_dir(self) -> str:
        """Returns the job timestamp used, for easier inspection"""
        # Fill in original dir
        shutil.copytree(f"{self.datadir}/simple/output", self.original_path)
        os.makedirs(f"{self.original_path}/ignored")  # just to confirm we only copy what we understand

        job_timestamp = "2023-02-28__19.53.08"
        config_dir = f"{self.original_path}/JobConfig/{job_timestamp}"
        os.makedirs(config_dir)
        common.write_json(f"{config_dir}/job_config.json", {"test": True})

        return job_timestamp

    async def run_convert(self, input_path: str = None, output_path: str = None) -> None:
        args = [
            "convert",
            input_path or self.original_path,
            output_path or self.target_path,
        ]
        await cli.main(args)

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
        self.assertEqual(
            {t.name for t in tasks.EtlTask.get_all_tasks()} | {"JobConfig"}, set(os.listdir(self.target_path))
        )
        self.assertEqual(
            {"test": True}, common.read_json(f"{self.target_path}/JobConfig/{job_timestamp}/job_config.json")
        )
        patients = utils.read_delta_lake(f"{self.target_path}/patient")  # spot check some patients
        self.assertEqual(2, len(patients))
        self.assertEqual("1de9ea66-70d3-da1f-c735-df5ef7697fb9", patients[0]["id"])
        self.assertEqual(1982, patients[0]["birthDate"])
        self.assertEqual(1983, patients[1]["birthDate"])
        conditions = utils.read_delta_lake(f"{self.target_path}/condition")  # and conditions
        self.assertEqual(2, len(conditions))
        self.assertEqual("2010-03-02", conditions[0]["recordedDate"])

        # Now make a second small, partial output folder to layer into the existing Delta Lake
        delta_timestamp = "2023-02-29__19.53.08"
        delta_path = os.path.join(self.tmpdir, "delta")
        os.makedirs(f"{delta_path}/patient")
        with common.NdjsonWriter(f"{delta_path}/patient/new.ndjson") as writer:
            writer.write({"resourceType": "Patient", "id": "1de9ea66-70d3-da1f-c735-df5ef7697fb9", "birthDate": 1800})
            writer.write({"resourceType": "Patient", "id": "z-gen", "birthDate": 2005})
        delta_config_dir = f"{delta_path}/JobConfig/{delta_timestamp}"
        os.makedirs(delta_config_dir)
        common.write_json(f"{delta_config_dir}/job_config.json", {"delta": "yup"})
        await self.run_convert(input_path=delta_path)

        # How did that change the delta lake dir? Hopefully we only interwove the new data
        self.assertEqual(  # confirm this is still here
            {"test": True}, common.read_json(f"{self.target_path}/JobConfig/{job_timestamp}/job_config.json")
        )
        self.assertEqual({"delta": "yup"}, common.read_json(f"{delta_config_dir}/job_config.json"))
        patients = utils.read_delta_lake(f"{self.target_path}/patient")  # re-check the patients
        self.assertEqual(3, len(patients))
        self.assertEqual(1800, patients[0]["birthDate"])  # these rows are sorted by id, so these are reliable indexes
        self.assertEqual(1983, patients[1]["birthDate"])
        self.assertEqual(2005, patients[2]["birthDate"])
        conditions = utils.read_delta_lake(f"{self.target_path}/condition")  # and conditions shouldn't change at all
        self.assertEqual(2, len(conditions))
        self.assertEqual("2010-03-02", conditions[0]["recordedDate"])
