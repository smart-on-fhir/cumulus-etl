"""Tests for etl.py"""

import filecmp
import glob
import os
import random
import tempfile
import unittest
import uuid
from unittest import mock

from cumulus.i2b2 import etl
from cumulus.i2b2.config import JobConfig


class TestI2b2EtlSimple(unittest.TestCase):
    """Test case for basic runs of etl methods"""

    def setUp(self):
        script_dir = os.path.dirname(__file__)
        input_path = os.path.join(script_dir, 'data/i2b2/simple')
        self.expected_path = os.path.join(script_dir, 'data/i2b2/simple-output')

        tmpdir = tempfile.TemporaryDirectory()  # pylint: disable=consider-using-with
        self.addCleanup(tmpdir.cleanup)
        self.output_path = tmpdir.name

        self.config = JobConfig(input_path, self.output_path)

        filecmp.clear_cache()

        # Enforce reproducible UUIDs by mocking out uuid4(). Setting a global
        # random seed does not work in this case - we need to mock it out.
        rd = random.Random()
        rd.seed(1234)
        uuid4_mock = mock.patch('cumulus.common.uuid.uuid4',
                                new=lambda: uuid.UUID(int=rd.getrandbits(128)))
        self.addCleanup(uuid4_mock.stop)
        uuid4_mock.start()

    def assert_file_tree_equal(self, dircmp):
        """
        Compare a tree of file content.

        filecmp.dircmp by itself likes to only do shallow comparisons that
        notice changes like timestamps. But we want the contents themselves.
        """
        self.assertEqual([], dircmp.left_only, dircmp.left)
        self.assertEqual([], dircmp.right_only, dircmp.right)

        for filename in dircmp.common_files:
            left_path = os.path.join(dircmp.left, filename)
            right_path = os.path.join(dircmp.right, filename)
            with open(left_path, 'r', encoding='utf8') as f:
                left_contents = f.read()
            with open(right_path, 'r', encoding='utf8') as f:
                right_contents = f.read()
            self.assertEqual(left_contents, right_contents, filename)

        for subdircmp in dircmp.subdirs.values():
            self.assert_file_tree_equal(subdircmp)

    def assert_output_equal(self):
        """Compares the etl output with the expected json structure"""
        # Toplevel directory has a spare JobConfig folder. Ignore it.
        ignore_files = glob.iglob(os.path.join(self.output_path, 'JobConfig_*'))
        ignore_files = [os.path.basename(f) for f in ignore_files]
        dircmp = filecmp.dircmp(self.expected_path, self.output_path,
                                ignore=ignore_files)
        self.assert_file_tree_equal(dircmp)

    def test_etl_job(self):
        summary_list = etl.etl_job(self.config)

        # Confirm there were no failures
        for summary in summary_list:
            self.assertEqual([], summary.failed)

        self.assert_output_equal()
