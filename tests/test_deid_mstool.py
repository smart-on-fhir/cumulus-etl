"""Tests for the mstool module"""

import filecmp
import os
import shutil
import tempfile

import pytest

from cumulus import common
from cumulus.deid.mstool import MSTOOL_CMD, run_mstool
from tests.utils import AsyncTestCase, TreeCompareMixin


@pytest.mark.skipif(not shutil.which(MSTOOL_CMD), reason="MS tool not installed")
class TestMicrosoftTool(TreeCompareMixin, AsyncTestCase):
    """Test case for the MS tool code (mostly testing our config file, really)"""

    def setUp(self):
        super().setUp()
        self.data_path = os.path.join(os.path.dirname(__file__), "data", "mstool")

    def combine_json(self, input_dir: str, output_dir: str) -> None:
        """
        Takes all the json files in the input folder and combines them into an ndjson file in the output folder.

        For example, with the following input folder:
          Encounter_1.json
          Patient_1.json
          Patient_2.json

        You will get the following output folder:
          Encounter.ndjson (one entry)
          Patient.ndjson (two entries)

        This is largely just so that our source files can be human-readable json, but tested in an ndjson context.
        """
        resource_buckets = {}
        for source_file in os.listdir(input_dir):
            resource = source_file.split(".")[0]
            resource_buckets.setdefault(resource, []).append(source_file)

        for resource, unsorted_files in resource_buckets.items():
            os.makedirs(output_dir, exist_ok=True)
            with common.NdjsonWriter(f"{output_dir}/{resource}.ndjson") as output_file:
                for filename in sorted(unsorted_files):
                    parsed_json = common.read_json(f"{input_dir}/{filename}")
                    output_file.write(parsed_json)

    async def test_expected_transform(self):
        """Confirms that our sample input data results in the correct output"""
        input_path = f"{self.data_path}/input"
        output_path = f"{self.data_path}/output"

        with tempfile.TemporaryDirectory() as tmpdir:
            self.combine_json(input_path, f"{tmpdir}/input")
            self.combine_json(output_path, f"{tmpdir}/expected")
            await run_mstool(f"{tmpdir}/input", f"{tmpdir}/output")
            dircmp = filecmp.dircmp(f"{tmpdir}/expected", f"{tmpdir}/output", ignore=[])
            self.assert_file_tree_equal(dircmp)

    async def test_invalid_syntax(self):
        """Confirms that unparsable files throw an error"""
        with tempfile.TemporaryDirectory() as input_dir:
            with tempfile.TemporaryDirectory() as output_dir:
                common.write_text(os.path.join(input_dir, "Condition.ndjson"), "foobar")
                with self.assertRaises(SystemExit):
                    await run_mstool(input_dir, output_dir)

    async def test_bad_fhir(self):
        """Confirms that parsable files with bad FHIR throw an error"""
        with tempfile.TemporaryDirectory() as input_dir:
            with tempfile.TemporaryDirectory() as output_dir:
                common.write_json(os.path.join(input_dir, "Condition.ndjson"), {})
                with self.assertRaises(SystemExit):
                    await run_mstool(input_dir, output_dir)
