"""Tests for etl/init/cli.py"""

import os

import ddt

from cumulus_etl import cli, common
from tests import utils


@ddt.ddt
class TestInit(utils.AsyncTestCase):
    """Tests for high-level init support."""

    def setUp(self):
        super().setUp()
        self.output_path = self.make_tempdir()

    async def run_init(self, output_path: str | None = None) -> None:
        args = [
            "init",
            output_path or self.output_path,
            "--output-format=ndjson",
        ]
        await cli.main(args)

    async def test_happy_path(self):
        """Verify that we can do a simple init"""
        await self.run_init()

        # Do some spot checks
        dirs = set(os.listdir(self.output_path))
        self.assertIn("device", dirs)
        self.assertIn("patient", dirs)
        self.assertIn("medicationrequest", dirs)
        self.assertIn("medication", dirs)  # secondary table
        self.assertIn("JobConfig", dirs)  # so that the dir is flagged as an ETL dir by 'convert'

        # Are folder contents what we expect?
        self.assertEqual(["patient.000.ndjson"], os.listdir(f"{self.output_path}/patient"))
        self.assertEqual("", common.read_text(f"{self.output_path}/patient/patient.000.ndjson"))
