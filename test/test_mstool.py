"""Tests for the mstool module"""

import filecmp
import os
import shutil
import tempfile
import unittest

import pytest

from cumulus import common
from cumulus.deid.mstool import MSTOOL_CMD, run_mstool
from test.utils import TreeCompareMixin


@pytest.mark.skipif(not shutil.which(MSTOOL_CMD), reason='MS tool not installed')
class TestMicrosoftTool(TreeCompareMixin, unittest.TestCase):
    """Test case for the MS tool code (mostly testing our config file, really)"""

    def setUp(self):
        super().setUp()
        self.data_path = os.path.join(os.path.dirname(__file__), 'data', 'mstool')

    def test_expected_transform(self):
        """Confirms that our sample input data results in the correct output"""
        input_path = os.path.join(self.data_path, 'input')

        with tempfile.TemporaryDirectory() as tmpdir:
            run_mstool(input_path, tmpdir)
            expected_path = os.path.join(self.data_path, 'output')
            dircmp = filecmp.dircmp(expected_path, tmpdir, ignore=[])
            self.assert_file_tree_equal(dircmp)

    def test_invalid_syntax(self):
        """Confirms that unparsable files throw an error"""
        with tempfile.TemporaryDirectory() as input_dir:
            with tempfile.TemporaryDirectory() as output_dir:
                common.write_text(os.path.join(input_dir, 'Condition.ndjson'), 'foobar')
                with self.assertRaises(SystemExit):
                    run_mstool(input_dir, output_dir)

    def test_bad_fhir(self):
        """Confirms that parsable files with bad FHIR throw an error"""
        with tempfile.TemporaryDirectory() as input_dir:
            with tempfile.TemporaryDirectory() as output_dir:
                common.write_json(os.path.join(input_dir, 'Condition.ndjson'), {})
                with self.assertRaises(SystemExit):
                    run_mstool(input_dir, output_dir)
