"""Tests for etl.py"""

import filecmp
import json
import os
import random
import shutil
import tempfile
import unittest
import uuid
from unittest import mock

from cumulus.i2b2 import etl


class TestI2b2EtlSimple(unittest.TestCase):
    """Test case for basic runs of etl methods"""

    def setUp(self):
        script_dir = os.path.dirname(__file__)
        self.data_dir = os.path.join(script_dir, 'data/i2b2/simple')
        self.input_path = os.path.join(self.data_dir, 'input')

        tmpdir = tempfile.mkdtemp()
        # Comment out this next line when debugging, to persist directory
        self.addCleanup(shutil.rmtree, tmpdir)

        self.output_path = os.path.join(tmpdir, 'output')
        os.mkdir(self.output_path)

        self.cache_path = os.path.join(tmpdir, 'cache')
        os.mkdir(self.cache_path)

        self.args = [self.input_path, self.output_path, self.cache_path]

        filecmp.clear_cache()

        self.enforce_consistent_uuids()

    def enforce_consistent_uuids(self):
        """Make sure that UUIDs will be the same from run to run"""
        # First, copy codebook over. This will help ensure that the order of
        # calls doesn't matter as much. If *every* UUID were recorded in the
        # codebook, this is all we'd need to do.
        shutil.copy(os.path.join(self.data_dir, 'codebook.json'),
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

            with open(left_path, 'rb') as f:
                left_contents = f.read()
            with open(right_path, 'rb') as f:
                right_contents = f.read()

            # Try to avoid comparing json files byte-for-byte. We may reasonably
            # change formatting, or even want the test files in an
            # easier-to-read format than the actual output files. In theory all
            # json files are equal once parsed.
            if filename.endswith('.json'):
                left_json = json.loads(left_contents.decode('utf8'))
                right_json = json.loads(right_contents.decode('utf8'))
                self.assertEqual(left_json, right_json, filename)
            elif filename.endswith('.ndjson'):
                left_split = left_contents.decode('utf8').splitlines()
                right_split = right_contents.decode('utf8').splitlines()
                left_rows = list(map(json.loads, left_split))
                right_rows = list(map(json.loads, right_split))
                self.assertEqual(left_rows, right_rows, filename)
            else:
                self.assertEqual(left_contents, right_contents, filename)

        for subdircmp in dircmp.subdirs.values():
            self.assert_file_tree_equal(subdircmp)

    def assert_output_equal(self, folder: str):
        """Compares the etl output with the expected json structure"""
        expected_path = os.path.join(self.data_dir, folder)
        dircmp = filecmp.dircmp(expected_path, self.output_path, ignore=[])
        self.assert_file_tree_equal(dircmp)

    def test_etl_job_json(self):
        etl.main(self.args)  # json is default
        self.assert_output_equal('json-output')

    def test_etl_job_ndjson(self):
        etl.main(self.args + ['--format=ndjson'])
        self.assert_output_equal('ndjson-output')

    def test_etl_job_parquet(self):
        etl.main(self.args + ['--format=parquet'])

        # Merely test that the files got created. It's a binary format, so
        # diffs aren't helpful, and looks like it can differ from machine to
        # machine. So, let's do minimal checking here.

        self.assertEqual({
            'fhir_conditions.parquet',
            'fhir_documentreferences.parquet',
            'fhir_encounters.parquet',
            'fhir_labs.parquet',
            'fhir_patients.parquet',
        }, set(os.listdir(self.output_path)))
