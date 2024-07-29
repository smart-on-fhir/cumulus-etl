"""Tests for etl/cli.py using i2b2 inputs"""

import filecmp
import os
import tempfile

from tests.etl import BaseEtlSimple


class TestI2b2Etl(BaseEtlSimple):
    """
    Base test case for basic runs of etl methods against i2b2 data
    """

    DATA_ROOT = "i2b2"

    async def test_full_etl(self):
        await self.run_etl(
            input_format="i2b2",
            philter=False,
            tasks=[
                # Just check the tasks that we actually have i2b2 support for
                "condition",
                "documentreference",
                "encounter",
                "medicationrequest",
                "observation",
                "patient",
            ],
        )
        self.assert_output_equal()

    async def test_export(self):
        with tempfile.TemporaryDirectory() as export_path:
            # Only run patient task to make the test faster and confirm we don't export unnecessary files
            await self.run_etl(
                input_format="i2b2", export_to=export_path, tasks=["patient"], philter=False
            )

            expected_export_path = os.path.join(self.datadir, self.DATA_ROOT, "export")
            dircmp = filecmp.dircmp(export_path, expected_export_path)
            self.assert_file_tree_equal(dircmp)
