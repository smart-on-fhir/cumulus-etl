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

from cumulus import common, etl, i2b2

from .ctakesmock import CtakesMixin, fake_ctakes_extract
from .s3mock import S3Mixin


@freezegun.freeze_time('Sep 15th, 2021 1:23:45', tz_offset=-4)
class BaseI2b2EtlSimple(CtakesMixin, unittest.TestCase):
    """
    Base test case for basic runs of etl methods

    Don't put actual tests in here, but rather in subclasses below.
    """

    def setUp(self):
        super().setUp()

        # you'll always want this when debugging
        self.maxDiff = None  # pylint: disable=invalid-name

        script_dir = os.path.dirname(__file__)
        self.data_dir = os.path.join(script_dir, 'data/simple-i2b2')
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
        # This helps with UUIDs that don't get recorded in the codebook, like observations.
        # Note that we only mock out the fake_id calls in codebook.py -- this is
        # intentional, as while we do use fake_id() elsewhere when generating new FHIR
        # objects, those are arbitrary real IDs before de-identification. If those
        # make it through to the end of etl test output, we have a problem anyway
        # and the flakiness of those IDs is a feature not a bug.
        self.category_seeds = {}
        uuid4_mock = mock.patch('cumulus.codebook.common.fake_id', new=self.reliable_fake_id)
        self.addCleanup(uuid4_mock.stop)
        uuid4_mock.start()

    def reliable_fake_id(self, category: str) -> str:
        """
        A version of common.fake_id that uses a new seed per category

        This helps reduce churn as each category has its own random generator.
        Without this, adding a new call to common.fake_id() early in the pipeline
        would change the result of every single call to common.fake_id() afterward.

        This was causing a lot of test file churn, so now we silo each random
        generator in each category. There is still churn in each category, but :shrug:.
        """
        rd = self.category_seeds.setdefault(category, random.Random(category))
        return str(uuid.UUID(int=rd.getrandbits(128)))

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


class TestI2b2EtlJobConfig(BaseI2b2EtlSimple):
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


class TestI2b2EtlBatches(BaseI2b2EtlSimple):
    """Test case for etl batching"""

    def test_batched_output(self):
        etl.main(self.args + ['--format=ndjson', '--batch-size=1'])
        self.assert_output_equal('batched-ndjson-output')

    def test_batch_iterate(self):
        """Check a bunch of edge cases for the _batch_iterate helper"""
        # pylint: disable=protected-access

        self.assertEqual([], [list(x) for x in etl._batch_iterate([], 2)])

        self.assertEqual([
            [1, 2],
            [3, 4],
        ], [list(x) for x in etl._batch_iterate([1, 2, 3, 4], 2)])

        self.assertEqual([
            [1, 2],
            [3, 4],
            [5],
        ], [list(x) for x in etl._batch_iterate([1, 2, 3, 4, 5], 2)])

        self.assertEqual([
            [1, 2, 3],
            [4],
        ], [list(x) for x in etl._batch_iterate([1, 2, 3, 4], 3)])

        self.assertEqual([
            [1],
            [2],
            [3],
        ], [list(x) for x in etl._batch_iterate([1, 2, 3], 1)])

        with self.assertRaises(ValueError):
            list(etl._batch_iterate([1, 2, 3], 0))

        with self.assertRaises(ValueError):
            list(etl._batch_iterate([1, 2, 3], -1))


class TestI2b2EtlFormats(BaseI2b2EtlSimple):
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
                'condition/fhir_conditions.000.parquet',
                'documentreference/fhir_documentreferences.000.parquet',
                'encounter/fhir_encounters.000.parquet',
                'observation/fhir_observations.000.parquet',
                'patient/fhir_patients.000.parquet',
                'symptom/fhir_symptoms.000.parquet',
            }, set(all_files))


class TestI2b2EtlOnS3(S3Mixin, BaseI2b2EtlSimple):
    """Test case for our support of writing to S3"""

    def test_etl_job_s3(self):
        etl.main(['--format=ndjson', self.input_path, 's3://mockbucket/root',
                  self.phi_path])

        fs = s3fs.S3FileSystem()
        all_files = {x for x in fs.find('mockbucket/root') if '/JobConfig/' not in x}
        self.assertEqual({
            'mockbucket/root/condition/fhir_conditions.000.ndjson',
            'mockbucket/root/documentreference/fhir_documentreferences.000.ndjson',
            'mockbucket/root/encounter/fhir_encounters.000.ndjson',
            'mockbucket/root/observation/fhir_observations.000.ndjson',
            'mockbucket/root/patient/fhir_patients.000.ndjson',
            'mockbucket/root/symptom/fhir_symptoms.000.ndjson',
        }, all_files)

        # Confirm we did not accidentally create an 's3:' directory locally
        # because we misinterpreted the s3 path as a local path
        self.assertFalse(os.path.exists('s3:'))


class TestI2b2EtlCachedCtakes(BaseI2b2EtlSimple):
    """Test case for caching the cTAKES responses"""

    def path_for_checksum(self, checksum):
        return os.path.join(self.phi_path, 'ctakes-cache', 'version1', checksum[0:4], f'sha256-{checksum}.json')

    def test_stores_cached_json(self):
        etl.main(self.args + ['--format=parquet'])

        notes_csv_path = os.path.join(self.input_path, 'csv_note', 'note1.csv')
        facts = i2b2.extract.extract_csv_observation_facts(notes_csv_path)

        expected_checksums = {
            0: 'd4f19607abe69ff92f1c80da0f78da1adb3bd26ecde5946178dc5c2957bafd78',
            1: '2c75f7374e5706606532d8456d7f7d5be184ee3ea9322ca9309beeb1570ceb42',
        }

        for index, checksum in expected_checksums.items():
            self.assertEqual(
                fake_ctakes_extract(facts[index].observation_blob).as_json(),
                common.read_json(self.path_for_checksum(checksum))
            )

    def test_does_not_hit_server_if_cache_exists(self):
        expected_checksums = {
            'd4f19607abe69ff92f1c80da0f78da1adb3bd26ecde5946178dc5c2957bafd78',
            '2c75f7374e5706606532d8456d7f7d5be184ee3ea9322ca9309beeb1570ceb42',
        }

        for index, checksum in enumerate(expected_checksums):
            # Write out some fake results to the cache location
            common.write_json(self.path_for_checksum(checksum), {
                'SignSymptomMention': [
                    {
                        'begin': 123,
                        'end': 129,
                        'text': f'foobar{index}',
                        'polarity': 0,
                        'type': 'SignSymptomMention',
                        'conceptAttributes': [
                            {
                                'code': '91058',
                                'cui': 'C0304290',
                                'codingScheme': 'RXNORM',
                                'tui': 'T122'
                            },
                        ],
                    }
                ],
            })

        etl.main(self.args + ['--format=ndjson'])

        # We should never have called our mock cTAKES server
        self.assertEqual(0, self.nlp_mock.call_count)

        # And we should see our fake cached results in the output
        with open(os.path.join(self.output_path, 'symptom', 'fhir_symptoms.000.ndjson'), 'r', encoding='utf8') as f:
            lines = f.readlines()
        symptoms = [json.loads(line) for line in lines]
        self.assertEqual(2, len(symptoms))
        self.assertEqual({'foobar0', 'foobar1'}, {x['code']['text'] for x in symptoms})
        for symptom in symptoms:
            self.assertEqual({'91058', 'C0304290'}, {x['code'] for x in symptom['code']['coding']})
