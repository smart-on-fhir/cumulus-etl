"""Tests for etl.py"""

import filecmp
import json
import os
import random
import shutil
import tempfile
import unittest
import uuid
from typing import Optional
from unittest import mock

import freezegun
import pytest
import s3fs
from fhirclient.models.extension import Extension

from cumulus import common, config, context, deid, etl, store
from cumulus.loaders.i2b2 import extract

from tests.ctakesmock import CtakesMixin, fake_ctakes_extract
from tests.s3mock import S3Mixin
from tests.test_i2b2_transform import ExampleResources
from tests.utils import TreeCompareMixin


@pytest.mark.skipif(not shutil.which(deid.MSTOOL_CMD), reason='MS tool not installed')
@freezegun.freeze_time('Sep 15th, 2021 1:23:45', tz_offset=-4)
class BaseI2b2EtlSimple(CtakesMixin, TreeCompareMixin, unittest.TestCase):
    """
    Base test case for basic runs of etl methods

    Don't put actual tests in here, but rather in subclasses below.
    """

    def setUp(self):
        super().setUp()

        script_dir = os.path.dirname(__file__)
        self.data_dir = os.path.join(script_dir, 'data/simple')
        self.input_path = os.path.join(self.data_dir, 'i2b2-input')

        tmpdir = tempfile.mkdtemp()
        # Comment out this next line when debugging, to persist directory
        self.addCleanup(shutil.rmtree, tmpdir)

        self.output_path = os.path.join(tmpdir, 'output')
        self.phi_path = os.path.join(tmpdir, 'phi')

        filecmp.clear_cache()

        self.enforce_consistent_uuids()

    def run_etl(self, input_path=None, output_path=None, phi_path=None, input_format: Optional[str] = 'i2b2',
                output_format: Optional[str] = 'ndjson', comment=None, batch_size=None) -> None:
        args = [
            input_path or self.input_path,
            output_path or self.output_path,
            phi_path or self.phi_path,
            '--skip-init-checks',
        ]
        if input_format:
            args.append(f'--input-format={input_format}')
        if output_format:
            args.append(f'--output-format={output_format}')
        if comment:
            args.append(f'--comment={comment}')
        if batch_size:
            args.append(f'--batch-size={batch_size}')
        etl.main(args)

    def enforce_consistent_uuids(self):
        """Make sure that UUIDs will be the same from run to run"""
        # First, copy codebook over. This will help ensure that the order of
        # calls doesn't matter as much. If *every* UUID were recorded in the
        # codebook, this is all we'd need to do.
        os.makedirs(self.phi_path)
        shutil.copy(os.path.join(self.data_dir, 'codebook.json'),
                    self.phi_path)

        secrets_mock = mock.patch('cumulus.deid.codebook.secrets.token_bytes', new=lambda x: b'1234')
        self.addCleanup(secrets_mock.stop)
        secrets_mock.start()

        # Enforce reproducible UUIDs by mocking out uuid4(). Setting a global
        # random seed does not work in this case - we need to mock it out.
        # This helps with UUIDs that don't get recorded in the codebook, like observations.
        # Note that we only mock out the fake_id calls in codebook.py -- this is
        # intentional, as while we do use fake_id() elsewhere when generating new FHIR
        # objects, those are arbitrary real IDs before de-identification. If those
        # make it through to the end of etl test output, we have a problem anyway
        # and the flakiness of those IDs is a feature not a bug.
        self.category_seeds = {}
        uuid4_mock = mock.patch('cumulus.deid.codebook.common.fake_id', new=self.reliable_fake_id)
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

    def assert_output_equal(self, folder: str):
        """Compares the etl output with the expected json structure"""
        # We don't compare contents of the job config because it includes a lot of paths etc.
        # But we can at least confirm that it was created.
        self.assertTrue(os.path.exists(os.path.join(self.output_path, 'JobConfig')))

        expected_path = os.path.join(self.data_dir, folder)
        dircmp = filecmp.dircmp(expected_path, self.output_path, ignore=['JobConfig'])
        self.assert_file_tree_equal(dircmp)


class TestI2b2EtlJobFlow(BaseI2b2EtlSimple):
    """Test case for the sequence of data through the system"""

    def setUp(self):
        super().setUp()
        self.scrubber = deid.Scrubber()
        self.codebook = self.scrubber.codebook
        self.loader = mock.MagicMock()
        self.dir_input = mock.MagicMock()
        self.format = mock.MagicMock()
        phi_root = store.Root(self.phi_path)
        self.config = config.JobConfig(self.loader, self.dir_input, self.format, phi_root, batch_size=5)

    def test_unknown_modifier_extensions_skipped_for_patients(self):
        """Verify we ignore unknown modifier extensions during a normal etl job flow (like patients)"""
        patient0 = ExampleResources.patient()
        patient0.id = '0'
        patient1 = ExampleResources.patient()
        patient1.id = '1'
        patient1.modifierExtension = [Extension({'url': 'unrecognized'})]

        with mock.patch('cumulus.etl._read_ndjson') as mock_read:
            mock_read.return_value = [patient0, patient1]
            etl.etl_patient(self.config, self.scrubber)

        # Confirm that only patient 0 got stored
        self.assertEqual(1, self.format.store_patients.call_count)
        df = self.format.store_patients.call_args[0][1]
        self.assertEqual([self.codebook.db.patient('0')], list(df.id))

    def test_unknown_modifier_extensions_skipped_for_nlp_symptoms(self):
        """Verify we ignore unknown modifier extensions during a custom etl job flow (nlp symptoms)"""
        docref0 = ExampleResources.documentreference()
        docref0.id = '0'
        docref0.subject.reference = 'Patient/1234'
        docref1 = ExampleResources.documentreference()
        docref1.id = '1'
        docref1.subject.reference = 'Patient/5678'
        docref1.modifierExtension = [Extension({'url': 'unrecognized'})]

        with mock.patch('cumulus.etl._read_ndjson') as mock_read:
            mock_read.return_value = [docref0, docref1]
            etl.etl_notes_text2fhir_symptoms(self.config, self.scrubber)

        # Confirm that only symptoms from docref 0 got stored
        self.assertEqual(1, self.format.store_symptoms.call_count)
        df = self.format.store_symptoms.call_args[0][1]
        expected_subject = f"Patient/{self.codebook.db.patient('1234')}"
        self.assertEqual({expected_subject}, {s['reference'] for s in df.subject})

    def test_downloaded_phi_is_not_kept(self):
        """Verify we remove all downloaded PHI even if interrupted"""
        internal_phi_dir = None

        def fake_scrub(phi_dir: str):
            # Save this dir path
            nonlocal internal_phi_dir
            internal_phi_dir = phi_dir

            # Run a couple checks to ensure that we do indeed have PHI in this dir
            self.assertIn('Patient.ndjson', os.listdir(phi_dir))
            with common.open_file(os.path.join(phi_dir, 'Patient.ndjson'), 'r') as f:
                first = json.loads(f.readlines()[0])
                self.assertEqual('02139', first['address'][0]['postalCode'])

            # Then raise an exception to interrupt the ETL flow before we normally would be able to clean up
            raise KeyboardInterrupt

        with mock.patch('cumulus.etl.deid.Scrubber.scrub_bulk_data', new=fake_scrub):
            with self.assertRaises(KeyboardInterrupt):
                self.run_etl()

        self.assertIsNotNone(internal_phi_dir)
        self.assertFalse(os.path.exists(internal_phi_dir))


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
        self.run_etl(comment='Run by foo on machine bar')
        config_file = self.read_config_file('job_config.json')
        self.assertEqual(config_file['comment'], 'Run by foo on machine bar')


class TestI2b2EtlJobContext(BaseI2b2EtlSimple):
    """Test case for the job context data"""

    def setUp(self):
        super().setUp()
        self.context_path = os.path.join(self.phi_path, 'context.json')

    def test_context_updated_on_success(self):
        """Verify that we update the success timestamp etc. when the job succeeds"""
        self.run_etl()
        job_context = context.JobContext(self.context_path)
        self.assertEqual('2021-09-14T21:23:45+00:00', job_context.last_successful_datetime.isoformat())
        self.assertEqual(self.input_path, job_context.last_successful_input_dir)
        self.assertEqual(self.output_path, job_context.last_successful_output_dir)

    def test_context_not_updated_on_failure(self):
        """Verify that we don't update the success timestamp etc. when the job fails"""
        input_context = {
            'last_successful_datetime': '2000-01-01T10:10:10+00:00',
            'last_successful_input': '/input',
            'last_successful_output': '/output',
        }
        common.write_json(self.context_path, input_context)

        with mock.patch('cumulus.etl.etl_job', side_effect=ZeroDivisionError):
            with self.assertRaises(ZeroDivisionError):
                self.run_etl()

        # Confirm we didn't change anything
        self.assertEqual(input_context, common.read_json(self.context_path))


class TestI2b2EtlBatches(BaseI2b2EtlSimple):
    """Test case for etl batching"""

    def test_batched_output(self):
        self.run_etl(batch_size=1)
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
        self.run_etl(output_format='json')
        self.assert_output_equal('json-output')

    def test_etl_job_ndjson(self):
        self.run_etl(output_format='ndjson')
        self.assert_output_equal('ndjson-output')

    def test_etl_job_input_ndjson(self):
        self.input_path = os.path.join(self.data_dir, 'ndjson-input')
        self.run_etl(input_format=None)  # ndjson should be default input
        self.assert_output_equal('ndjson-output')

    def test_etl_job_parquet(self):
        self.run_etl(output_format=None)  # parquet should be default output

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
        fs = s3fs.S3FileSystem()
        fs.makedirs('s3://mockbucket/')

        self.run_etl(output_path='s3://mockbucket/root')

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

    def setUp(self):
        super().setUp()
        # sha256 checksums of the two test patient notes
        self.expected_checksums = [
            '5db841c4c46d8a25fbb1891fd1eb352170278fa2b931c1c5edebe09a06582fb5',
            '6466bb1868126fd2b5e357a556fceed075fab1e8d25d5f777abf33144d93c5cf',
        ]

    def path_for_checksum(self, checksum):
        return os.path.join(self.phi_path, 'ctakes-cache', 'version1', checksum[0:4], f'sha256-{checksum}.json')

    def test_stores_cached_json(self):
        self.run_etl(output_format='parquet')

        notes_csv_path = os.path.join(self.input_path, 'csv_note', 'note1.csv')
        facts = extract.extract_csv_observation_facts(notes_csv_path)

        for index, checksum in enumerate(self.expected_checksums):
            self.assertEqual(
                fake_ctakes_extract(facts[index].observation_blob).as_json(),
                common.read_json(self.path_for_checksum(checksum))
            )

    def test_does_not_hit_server_if_cache_exists(self):
        for index, checksum in enumerate(self.expected_checksums):
            # Write out some fake results to the cache location
            filename = self.path_for_checksum(checksum)
            os.makedirs(os.path.dirname(filename))
            common.write_json(filename, {
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

        self.run_etl()

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
