"""Tests for etl.py"""

import filecmp
import itertools
import json
import os
import shutil
import tempfile
import unittest
from typing import Optional
from unittest import mock

import freezegun
import pytest
import s3fs
from ctakesclient.typesystem import Polarity

from cumulus import common, config, context, deid, errors, etl, loaders, store
from cumulus.loaders.i2b2 import extract

from tests.ctakesmock import CtakesMixin, fake_ctakes_extract
from tests.s3mock import S3Mixin
from tests.utils import TreeCompareMixin


@pytest.mark.skipif(not shutil.which(deid.MSTOOL_CMD), reason="MS tool not installed")
@freezegun.freeze_time("Sep 15th, 2021 1:23:45", tz_offset=-4)
class BaseI2b2EtlSimple(CtakesMixin, TreeCompareMixin, unittest.TestCase):
    """
    Base test case for basic runs of etl methods

    Don't put actual tests in here, but rather in subclasses below.
    """

    def setUp(self):
        super().setUp()

        script_dir = os.path.dirname(__file__)
        self.data_dir = os.path.join(script_dir, "data/simple")
        self.input_path = os.path.join(self.data_dir, "i2b2-input")

        tmpdir = tempfile.mkdtemp()
        # Comment out this next line when debugging, to persist directory
        self.addCleanup(shutil.rmtree, tmpdir)

        self.output_path = os.path.join(tmpdir, "output")
        self.phi_path = os.path.join(tmpdir, "phi")

        filecmp.clear_cache()

        self.enforce_consistent_uuids()

    def run_etl(
        self,
        input_path=None,
        output_path=None,
        phi_path=None,
        input_format: Optional[str] = "i2b2",
        output_format: Optional[str] = "ndjson",
        comment=None,
        batch_size=None,
        tasks=None,
    ) -> None:
        args = [
            input_path or self.input_path,
            output_path or self.output_path,
            phi_path or self.phi_path,
            "--skip-init-checks",
        ]
        if input_format:
            args.append(f"--input-format={input_format}")
        if output_format:
            args.append(f"--output-format={output_format}")
        if comment:
            args.append(f"--comment={comment}")
        if batch_size:
            args.append(f"--batch-size={batch_size}")
        if tasks:
            args.append(f'--task={",".join(tasks)}')
        etl.main(args)

    def enforce_consistent_uuids(self):
        """Make sure that UUIDs will be the same from run to run"""
        # First, copy codebook over. This will help ensure that the order of
        # calls doesn't matter as much. If *every* UUID were recorded in the
        # codebook, this is all we'd need to do.
        os.makedirs(self.phi_path)
        shutil.copy(os.path.join(self.data_dir, "codebook.json"), self.phi_path)

    def assert_output_equal(self, folder: str):
        """Compares the etl output with the expected json structure"""
        # We don't compare contents of the job config because it includes a lot of paths etc.
        # But we can at least confirm that it was created.
        self.assertTrue(os.path.exists(os.path.join(self.output_path, "JobConfig")))

        expected_path = os.path.join(self.data_dir, folder)
        dircmp = filecmp.dircmp(expected_path, self.output_path, ignore=["JobConfig"])
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

    def test_batched_output(self):
        self.run_etl(batch_size=1)
        self.assert_output_equal("batched-ndjson-output")

    def test_downloaded_phi_is_not_kept(self):
        """Verify we remove all downloaded PHI even if interrupted"""
        internal_phi_dir = None

        def fake_scrub(phi_dir: str):
            # Save this dir path
            nonlocal internal_phi_dir
            internal_phi_dir = phi_dir

            # Run a couple checks to ensure that we do indeed have PHI in this dir
            self.assertIn("Patient.ndjson", os.listdir(phi_dir))
            with common.open_file(os.path.join(phi_dir, "Patient.ndjson"), "r") as f:
                first = json.loads(f.readlines()[0])
                self.assertEqual("02139", first["address"][0]["postalCode"])

            # Then raise an exception to interrupt the ETL flow before we normally would be able to clean up
            raise KeyboardInterrupt

        with mock.patch("cumulus.etl.deid.Scrubber.scrub_bulk_data", new=fake_scrub):
            with self.assertRaises(KeyboardInterrupt):
                self.run_etl()

        self.assertIsNotNone(internal_phi_dir)
        self.assertFalse(os.path.exists(internal_phi_dir))

    def test_unknown_task(self):
        with self.assertRaises(SystemExit) as cm:
            self.run_etl(tasks=["blarg"])
        self.assertEqual(errors.TASK_UNKNOWN, cm.exception.code)

    def test_failed_task(self):
        # Make it so any writes will fail
        with mock.patch("cumulus.formats.ndjson.NdjsonFormat.write_format", side_effect=Exception):
            with self.assertRaises(SystemExit) as cm:
                self.run_etl()
        self.assertEqual(errors.TASK_FAILED, cm.exception.code)

    def test_single_task(self):
        # Grab all observations before we mock anything
        observations = loaders.I2b2Loader(store.Root(self.input_path), 5).load_all(["Observation"])

        def fake_load_all(internal_self, resources):
            del internal_self
            # Confirm we only tried to load one resource
            self.assertEqual(["Observation"], resources)
            return observations

        with mock.patch.object(loaders.I2b2Loader, "load_all", new=fake_load_all):
            self.run_etl(tasks=["observation"])

        # Confirm we only wrote the one resource
        self.assertEqual({"observation", "JobConfig"}, set(os.listdir(self.output_path)))
        self.assertEqual(["observation.000.ndjson"], os.listdir(os.path.join(self.output_path, "observation")))

    def test_multiple_tasks(self):
        # Grab all observations before we mock anything
        loaded = loaders.I2b2Loader(store.Root(self.input_path), 5).load_all(["Observation", "Patient"])

        def fake_load_all(internal_self, resources):
            del internal_self
            # Confirm we only tried to load two resources
            self.assertEqual({"Observation", "Patient"}, set(resources))
            return loaded

        with mock.patch.object(loaders.I2b2Loader, "load_all", new=fake_load_all):
            self.run_etl(tasks=["observation", "patient"])

        # Confirm we only wrote the one resource
        self.assertEqual({"observation", "patient", "JobConfig"}, set(os.listdir(self.output_path)))
        self.assertEqual(["observation.000.ndjson"], os.listdir(os.path.join(self.output_path, "observation")))
        self.assertEqual(["patient.000.ndjson"], os.listdir(os.path.join(self.output_path, "patient")))

    @mock.patch("cumulus.deid.codebook.secrets.token_hex", new=lambda x: "1234")
    def test_codebook_is_saved_during(self):
        """Verify that we are saving the codebook as we go"""
        # Clear out the saved test codebook first
        codebook_path = os.path.join(self.phi_path, "codebook.json")
        os.remove(codebook_path)

        # Cause a system exit as soon as we try to write a file.
        # The goal is that the codebook is already in place by this time.
        with self.assertRaises(SystemExit):
            with mock.patch("cumulus.formats.ndjson.NdjsonFormat.write_format", side_effect=SystemExit):
                self.run_etl(tasks=["patient"])

        # Ensure we wrote a valid codebook out
        codebook = common.read_json(codebook_path)
        self.assertEqual("1234", codebook["id_salt"])
        mappings = common.read_json(os.path.join(self.phi_path, "codebook-cached-mappings.json"))
        self.assertDictEqual(
            {
                "323456": "58d7507019c4ebe8daaf70f796578d12284de4a0e0fd85b968cda8ef85dee949",
                "3234567": "21bf1d599a1d856eb911cdf681c05e848a6afc8a726038be6b1535286edd7444",
            },
            mappings["Patient"],
        )


class TestI2b2EtlJobConfig(BaseI2b2EtlSimple):
    """Test case for the job config logging data"""

    def setUp(self):
        super().setUp()
        self.job_config_path = os.path.join(self.output_path, "JobConfig/2021-09-14__21.23.45")

    def read_config_file(self, name: str) -> dict:
        full_path = os.path.join(self.job_config_path, name)
        with open(full_path, "r", encoding="utf8") as f:
            return json.load(f)

    def test_comment(self):
        """Verify that a comment makes it from command line to the log file"""
        self.run_etl(comment="Run by foo on machine bar")
        config_file = self.read_config_file("job_config.json")
        self.assertEqual(config_file["comment"], "Run by foo on machine bar")


class TestI2b2EtlJobContext(BaseI2b2EtlSimple):
    """Test case for the job context data"""

    def setUp(self):
        super().setUp()
        self.context_path = os.path.join(self.phi_path, "context.json")

    def test_context_updated_on_success(self):
        """Verify that we update the success timestamp etc. when the job succeeds"""
        self.run_etl()
        job_context = context.JobContext(self.context_path)
        self.assertEqual("2021-09-14T21:23:45+00:00", job_context.last_successful_datetime.isoformat())
        self.assertEqual(self.input_path, job_context.last_successful_input_dir)
        self.assertEqual(self.output_path, job_context.last_successful_output_dir)

    def test_context_not_updated_on_failure(self):
        """Verify that we don't update the success timestamp etc. when the job fails"""
        input_context = {
            "last_successful_datetime": "2000-01-01T10:10:10+00:00",
            "last_successful_input": "/input",
            "last_successful_output": "/output",
        }
        common.write_json(self.context_path, input_context)

        with mock.patch("cumulus.etl.etl_job", side_effect=ZeroDivisionError):
            with self.assertRaises(ZeroDivisionError):
                self.run_etl()

        # Confirm we didn't change anything
        self.assertEqual(input_context, common.read_json(self.context_path))


class TestI2b2EtlFormats(BaseI2b2EtlSimple):
    """Test case for each of the formats we support"""

    def test_etl_job_ndjson(self):
        self.run_etl(output_format="ndjson")
        self.assert_output_equal("ndjson-output")

    def test_etl_job_input_ndjson(self):
        self.input_path = os.path.join(self.data_dir, "ndjson-input")
        self.run_etl(input_format=None)  # ndjson should be default input
        self.assert_output_equal("ndjson-output")

    def test_etl_job_parquet(self):
        self.run_etl(output_format="parquet")

        # Merely test that the files got created. It's a binary format, so
        # diffs aren't helpful, and looks like it can differ from machine to
        # machine. So, let's do minimal checking here.

        all_files = [
            os.path.relpath(os.path.join(root, name), start=self.output_path)
            for root, dirs, files in os.walk(self.output_path)
            for name in files
        ]

        # Filter out job config files, we don't care about those for now
        all_files = filter(lambda filename: "JobConfig" not in filename, all_files)

        self.assertEqual(
            {
                "condition/condition.000.parquet",
                "documentreference/documentreference.000.parquet",
                "encounter/encounter.000.parquet",
                "observation/observation.000.parquet",
                "patient/patient.000.parquet",
                "covid_symptom__nlp_results/covid_symptom__nlp_results.000.parquet",
            },
            set(all_files),
        )

    def test_etl_job_deltalake(self):
        self.run_etl(output_format=None)  # deltalake should be default output format

        # Just test that the files got created, for a single table.

        condition_path = os.path.join(self.output_path, "condition")
        all_files = {
            os.path.relpath(os.path.join(root, name), start=condition_path)
            for root, dirs, files in os.walk(condition_path)
            for name in files
        }

        metadata_files = {x for x in all_files if x.startswith("_")}
        data_files = {x for x in all_files if x.startswith("part-")}
        data_crc_files = {x for x in all_files if x.startswith(".part-")}

        # Just confirm that we sliced them up correctly
        self.assertEqual(metadata_files | data_files | data_crc_files, all_files)

        # Check metadata files (these have consistent names)
        self.assertEqual(
            {
                "_delta_log/00000000000000000000.json",
                "_delta_log/.00000000000000000000.json.crc",
                "_symlink_format_manifest/manifest",
                "_symlink_format_manifest/.manifest.crc",
            },
            metadata_files,
        )

        self.assertEqual(1, len(data_files))
        self.assertRegex(data_files.pop(), r"part-00000-.*-c000.snappy.parquet")

        self.assertEqual(1, len(data_crc_files))
        self.assertRegex(data_crc_files.pop(), r".part-00000-.*-c000.snappy.parquet.crc")


class TestI2b2EtlOnS3(S3Mixin, BaseI2b2EtlSimple):
    """Test case for our support of writing to S3"""

    def test_etl_job_s3(self):
        fs = s3fs.S3FileSystem()
        fs.makedirs("s3://mockbucket/")

        self.run_etl(output_path="s3://mockbucket/root")

        all_files = {x for x in fs.find("mockbucket/root") if "/JobConfig/" not in x}
        self.assertEqual(
            {
                "mockbucket/root/condition/condition.000.ndjson",
                "mockbucket/root/documentreference/documentreference.000.ndjson",
                "mockbucket/root/encounter/encounter.000.ndjson",
                "mockbucket/root/observation/observation.000.ndjson",
                "mockbucket/root/patient/patient.000.ndjson",
                "mockbucket/root/covid_symptom__nlp_results/covid_symptom__nlp_results.000.ndjson",
            },
            all_files,
        )

        # Confirm we did not accidentally create an 's3:' directory locally
        # because we misinterpreted the s3 path as a local path
        self.assertFalse(os.path.exists("s3:"))


class TestI2b2EtlNlp(BaseI2b2EtlSimple):
    """Test case for the cTAKES/cNLP responses"""

    def setUp(self):
        super().setUp()
        # sha256 checksums of the two test patient notes
        self.expected_checksums = [
            "5db841c4c46d8a25fbb1891fd1eb352170278fa2b931c1c5edebe09a06582fb5",
            "6466bb1868126fd2b5e357a556fceed075fab1e8d25d5f777abf33144d93c5cf",
        ]

    def path_for_checksum(self, prefix, checksum):
        return os.path.join(self.phi_path, "ctakes-cache", prefix, checksum[0:4], f"sha256-{checksum}.json")

    def read_symptoms(self):
        """Loads the output symptoms ndjson from disk"""
        path = os.path.join(self.output_path, "covid_symptom__nlp_results", "covid_symptom__nlp_results.000.ndjson")
        with open(path, "r", encoding="utf8") as f:
            lines = f.readlines()
        return [json.loads(line) for line in lines]

    def test_stores_cached_json(self):
        self.run_etl(output_format="parquet", tasks=["covid_symptom__nlp_results"])

        notes_csv_path = os.path.join(self.input_path, "observation_fact_notes.csv")
        facts = list(extract.extract_csv_observation_facts(notes_csv_path, 5))

        for index, checksum in enumerate(self.expected_checksums):
            ner = fake_ctakes_extract(facts[index].observation_blob)
            self.assertEqual(ner.as_json(), common.read_json(self.path_for_checksum("covid_symptom_v1", checksum)))
            self.assertEqual([0, 0], common.read_json(self.path_for_checksum("covid_symptom_v1-cnlp_v2", checksum)))

    def test_does_not_hit_server_if_cache_exists(self):
        for index, checksum in enumerate(self.expected_checksums):
            # Write out some fake results to the cache location
            filename = self.path_for_checksum("covid_symptom_v1", checksum)
            os.makedirs(os.path.dirname(filename))
            common.write_json(
                filename,
                {
                    "SignSymptomMention": [
                        {
                            "begin": 123,
                            "end": 129,
                            "text": f"foobar{index}",
                            "polarity": 0,
                            "type": "SignSymptomMention",
                            "conceptAttributes": [
                                {"code": "68235000", "cui": "C0027424", "codingScheme": "SNOMEDCT_US", "tui": "T184"},
                            ],
                        }
                    ],
                },
            )

            cnlp_filename = self.path_for_checksum("covid_symptom_v1-cnlp_v2", checksum)
            os.makedirs(os.path.dirname(cnlp_filename))
            common.write_json(cnlp_filename, [0])

        self.run_etl(tasks=["covid_symptom__nlp_results"])

        # We should never have called our mock cTAKES server
        self.assertEqual(0, self.nlp_mock.call_count)
        self.assertEqual(0, self.cnlp_mock.call_count)

        # And we should see our fake cached results in the output
        symptoms = self.read_symptoms()
        self.assertEqual(2, len(symptoms))
        self.assertEqual({"foobar0", "foobar1"}, {x["match"]["text"] for x in symptoms})
        for symptom in symptoms:
            self.assertEqual(
                {("68235000", "C0027424")}, {(x["code"], x["cui"]) for x in symptom["match"]["conceptAttributes"]}
            )

    def test_cnlp_rejects(self):
        """Verify that if the cnlp server negates a match, it does not show up"""
        # First match is fever, second is nausea
        self.cnlp_mock.side_effect = lambda _, spans: [Polarity.neg, Polarity.pos]
        self.run_etl(tasks=["covid_symptom__nlp_results"])

        symptoms = self.read_symptoms()
        self.assertEqual(2, len(symptoms))
        # Confirm that the only symptom to survive was the second nausea one
        self.assertEqual(
            {("422587007", "C0027497")}, {(x["code"], x["cui"]) for x in symptoms[0]["match"]["conceptAttributes"]}
        )

    def test_non_covid_symptoms_skipped(self):
        """Verify that the 'itch' symptom in our mock response does not make it to the output table"""
        self.run_etl(tasks=["covid_symptom__nlp_results"])

        symptoms = self.read_symptoms()
        self.assertEqual({"for"}, {x["match"]["text"] for x in symptoms})  # the second word ("for") is the fever word
        attributes = itertools.chain.from_iterable(symptom["match"]["conceptAttributes"] for symptom in symptoms)
        cuis = {x["cui"] for x in attributes}
        self.assertEqual({"C0027497", "C0015967"}, cuis)  # notably, no C0033774 itch CUI
