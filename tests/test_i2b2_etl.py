"""Tests for etl/cli.py using i2b2 inputs"""

import filecmp
import os
import shutil
import tempfile

import pytest

from cumulus_etl import cli, deid

from tests.ctakesmock import CtakesMixin
from tests.utils import AsyncTestCase, TreeCompareMixin


@pytest.mark.skipif(not shutil.which(deid.MSTOOL_CMD), reason="MS tool not installed")
class TestI2b2Etl(CtakesMixin, TreeCompareMixin, AsyncTestCase):
    """
    Base test case for basic runs of etl methods against i2b2 data
    """

    def setUp(self):
        super().setUp()

        i2b2_dir = os.path.join(self.datadir, "i2b2")
        self.input_path = os.path.join(i2b2_dir, "input")
        self.expected_output_path = os.path.join(i2b2_dir, "output")
        self.expected_export_path = os.path.join(i2b2_dir, "export")

        tmpdir = tempfile.mkdtemp()
        # Comment out this next line when debugging, to persist directory
        self.addCleanup(shutil.rmtree, tmpdir)

        self.output_path = os.path.join(tmpdir, "output")
        self.phi_path = os.path.join(tmpdir, "phi")

        # Copy the codebook over, to guarantee the same ID mappings run-to-run
        os.makedirs(self.phi_path)
        shutil.copy(os.path.join(i2b2_dir, "codebook.json"), self.phi_path)

    async def test_full_etl(self):
        await cli.main(
            [
                self.input_path,
                self.output_path,
                self.phi_path,
                "--skip-init-checks",
                "--input-format=i2b2",
                "--output-format=ndjson",
                f"--ctakes-overrides={self.ctakes_overrides.name}",
            ]
        )
        self.assert_etl_output_equal(self.expected_output_path, self.output_path)

    async def test_export(self):
        with tempfile.TemporaryDirectory() as export_path:
            await cli.main(
                [
                    self.input_path,
                    self.output_path,
                    self.phi_path,
                    "--skip-init-checks",
                    "--input-format=i2b2",
                    "--output-format=ndjson",
                    f"--export-to={export_path}",
                    "--task=patient",  # just to make the test faster and confirm we don't export unnecessary files
                ]
            )

            dircmp = filecmp.dircmp(export_path, self.expected_export_path)
            self.assert_file_tree_equal(dircmp)
