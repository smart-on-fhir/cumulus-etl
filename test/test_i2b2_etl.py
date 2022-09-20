"""Tests for etl.py"""

import filecmp
import os
import random
import shutil
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
        self.input_path = os.path.join(script_dir, 'data/i2b2/simple')
        self.expected_path = os.path.join(script_dir, 'data/i2b2/simple-output')

        tmpdir = tempfile.mkdtemp()
        # Comment out this next line when debugging, to persist directory
        self.addCleanup(shutil.rmtree, tmpdir)

        self.output_path = os.path.join(tmpdir, 'output')
        os.mkdir(self.output_path)
        print(f'Output path: {self.output_path}')

        self.cache_path = os.path.join(tmpdir, 'cache')
        os.mkdir(self.cache_path)
        print(f'Cache path: {self.cache_path}')

        self.config = JobConfig(self.input_path, self.output_path,
                                self.cache_path)

        filecmp.clear_cache()

        self.enforce_consistent_uuids()

    def enforce_consistent_uuids(self):
        """Make sure that UUIDs will be the same from run to run"""
        # First, copy codebook over. This will help ensure that the order of
        # calls doesn't matter as much. If *every* UUID were recorded in the
        # codebook, this is all we'd need to do.
        shutil.copy(os.path.join(self.input_path, 'codebook.json'),
                    self.cache_path)

        # Enforce reproducible UUIDs by mocking out uuid4(). Setting a global
        # random seed does not work in this case - we need to mock it out.
        # This helps with UUIDs that don't get recorded in the codebook, like
        # observations. But note that it's sensitive to code changes that cause
        # a different timing of the calls to uuid4().
        rd = random.Random()
        rd.seed(12345)
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
        dircmp = filecmp.dircmp(self.expected_path, self.output_path, ignore=[])
        self.assert_file_tree_equal(dircmp)

    def test_etl_job(self):
        summary_list = etl.etl_job(self.config)

        # Confirm there were no failures
        for summary in summary_list:
            self.assertEqual([], summary.failed)

        self.assert_output_equal()
