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

import freezegun
import s3fs

from cumulus import etl

from .s3mock import S3Mixin


class TestI2b2EtlSimple(unittest.TestCase):
    """Base test case for basic runs of etl methods"""

    def setUp(self):
        super().setUp()

        script_dir = os.path.dirname(__file__)
        self.data_dir = os.path.join(script_dir, 'data/i2b2/simple')
        self.input_path = os.path.join(self.data_dir, 'input')

        tmpdir = tempfile.mkdtemp()
        # Comment out this next line when debugging, to persist directory
        self.addCleanup(shutil.rmtree, tmpdir)

        self.output_path = os.path.join(tmpdir, 'output')
        self.phi_path = os.path.join(tmpdir, 'phi')
        self.args = [self.input_path, self.output_path, self.phi_path]

        filecmp.clear_cache()

        self.enforce_consistent_uuids()

    def enforce_consistent_uuids(self):
        """Make sure that UUIDs will be the same from run to run"""
        # First, copy codebook over. This will help ensure that the order of
        # calls doesn't matter as much. If *every* UUID were recorded in the
        # codebook, this is all we'd need to do.
        os.makedirs(self.phi_path)
        shutil.copy(os.path.join(self.data_dir, 'codebook.json'),
                    self.phi_path)

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
        # We don't compare contents of the job config because it includes a lot of paths etc.
        # But we can at least confirm that it was created.
        self.assertTrue(os.path.exists(os.path.join(self.output_path, 'JobConfig')))

        expected_path = os.path.join(self.data_dir, folder)
        dircmp = filecmp.dircmp(expected_path, self.output_path, ignore=['JobConfig'])
        self.assert_file_tree_equal(dircmp)


@freezegun.freeze_time('Sep 15th, 2021 1:23:45', tz_offset=-4)
class TestI2b2EtlJobConfig(TestI2b2EtlSimple):
    """Test case for the job config logging data"""

    def setUp(self):
        super().setUp()
        self.job_config_path = os.path.join(self.output_path, 'JobConfig/2021-09-14__21.23.45')

    def read_config_file(self, name: str) -> dict:
        full_path = os.path.join(self.job_config_path, name)
        with open(full_path, 'r', encoding='utf8') as f:
            return json.load(f)

    def test_comment(self):
        """Verify that a comment makes it from command line to the log file"""
        etl.main(self.args + ['--comment=Run by foo on machine bar'])
        config = self.read_config_file('job_config.json')
        self.assertEqual(config['comment'], 'Run by foo on machine bar')


class TestI2b2EtlFormats(TestI2b2EtlSimple):
    """Test case for each of the formats we support"""

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

        all_files = [os.path.relpath(os.path.join(root, name), start=self.output_path)
                     for root, dirs, files in os.walk(self.output_path)
                     for name in files]

        # Filter out job config files, we don't care about those for now
        all_files = filter(lambda filename: 'JobConfig' not in filename, all_files)

        self.assertEqual(
            {
                'condition/fhir_conditions.parquet',
                'documentreference/fhir_documentreferences.parquet',
                'encounter/fhir_encounters.parquet',
                'observation/fhir_observations.parquet',
                'patient/fhir_patients.parquet',
            }, set(all_files))


class TestI2b2EtlOnS3(S3Mixin, TestI2b2EtlSimple):
    """Test case for our support of writing to S3"""

    def test_etl_job_s3(self):
        etl.main(['--format=ndjson', self.input_path, 's3://mockbucket/root',
                  self.phi_path])

        fs = s3fs.S3FileSystem()
        all_files = {x for x in fs.find('mockbucket/root') if '/JobConfig/' not in x}
        self.assertEqual({
            'mockbucket/root/condition/fhir_conditions.ndjson',
            'mockbucket/root/documentreference/fhir_documentreferences.ndjson',
            'mockbucket/root/encounter/fhir_encounters.ndjson',
            'mockbucket/root/observation/fhir_observations.ndjson',
            'mockbucket/root/patient/fhir_patients.ndjson',
        }, all_files)

        # Confirm we did not accidentally create an 's3:' directory locally
        # because we misinterpreted the s3 path as a local path
        self.assertFalse(os.path.exists('s3:'))