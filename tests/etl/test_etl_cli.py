"""Tests for etl/cli.py"""

import contextlib
import datetime
import gzip
import io
import itertools
import json
import os
import shutil
import tempfile
from unittest import mock

import cumulus_fhir_support
import ddt
import respx
from ctakesclient.typesystem import Polarity

from cumulus_etl import common, errors, loaders, store
from cumulus_etl.etl import context
from tests.ctakesmock import fake_ctakes_extract
from tests.etl import BaseEtlSimple
from tests.s3mock import S3Mixin
from tests.utils import FROZEN_TIME_UTC, read_delta_lake


@ddt.ddt
class TestEtlJobFlow(BaseEtlSimple):
    """Test case for the sequence of data through the system"""

    async def test_batched_output(self):
        await self.run_etl(
            batch_size=1,
            tasks=[
                # Batching is not task-specific, so just test a smattering of tasks
                "condition",
                "documentreference",
                "encounter",
                "medicationrequest",
                "observation",
                "patient",
                "procedure",
                "servicerequest",
            ],
        )
        self.assert_output_equal("batched-output")

    @ddt.data(
        (["covid_symptom__nlp_results"], False),
        (["patient"], True),
        (["covid_symptom__nlp_results", "patient"], True),
    )
    @ddt.unpack
    async def test_ms_deid_skipped_if_not_needed(self, tasks: list[str], expected_ms_deid: bool):
        with self.assertRaises(SystemExit):
            with mock.patch("cumulus_etl.deid.Scrubber.scrub_bulk_data") as mock_deid:
                with mock.patch("cumulus_etl.etl.cli.etl_job", side_effect=SystemExit):
                    await self.run_etl(tasks=tasks)
        self.assertEqual(1 if expected_ms_deid else 0, mock_deid.call_count)

    async def test_downloaded_phi_is_not_kept(self):
        """Verify we remove all downloaded PHI even if interrupted"""
        internal_phi_dir = None

        def fake_scrub(phi_dir: str):
            # Save this dir path
            nonlocal internal_phi_dir
            internal_phi_dir = phi_dir

            # Run a couple checks to ensure that we do indeed have PHI in this dir
            self.assertIn("Patient.ndjson", os.listdir(phi_dir))
            patients = list(
                cumulus_fhir_support.read_multiline_json(os.path.join(phi_dir, "Patient.ndjson"))
            )
            first = patients[0]
            self.assertEqual("02139", first["address"][0]["postalCode"])

            # Then raise an exception to interrupt the ETL flow before we normally would be able to clean up
            raise KeyboardInterrupt

        with mock.patch("cumulus_etl.deid.Scrubber.scrub_bulk_data", new=fake_scrub):
            with self.assertRaises(KeyboardInterrupt):
                await self.run_etl()

        self.assertIsNotNone(internal_phi_dir)
        self.assertFalse(os.path.exists(internal_phi_dir))

    async def test_unknown_task(self):
        with self.assertRaises(SystemExit) as cm:
            await self.run_etl(tasks=["blarg"])
        self.assertEqual(errors.TASK_UNKNOWN, cm.exception.code)

    async def test_help_task(self):
        with self.assertRaises(SystemExit) as cm:
            await self.run_etl(tasks=["patient", "help"])
        self.assertEqual(errors.TASK_HELP, cm.exception.code)

    async def test_failed_task(self):
        # Make it so any writes will fail
        with mock.patch(
            "cumulus_etl.formats.ndjson.NdjsonFormat.write_format", side_effect=Exception
        ):
            with self.assertRaises(SystemExit) as cm:
                await self.run_etl()
        self.assertEqual(errors.TASK_FAILED, cm.exception.code)

    async def test_single_task(self):
        # Grab all observations before we mock anything
        observations = loaders.FhirNdjsonLoader(store.Root(self.input_path)).load_resources(
            {"Observation"}
        )

        def fake_load_resources(internal_self, resources):
            del internal_self
            # Confirm we only tried to load one resource
            self.assertEqual({"Observation"}, resources)
            return observations

        with mock.patch.object(loaders.FhirNdjsonLoader, "load_resources", new=fake_load_resources):
            await self.run_etl(tasks=["observation"])

        # Confirm we only wrote the one resource
        self.assertEqual(
            {"etl__completion", "observation", "JobConfig"}, set(os.listdir(self.output_path))
        )
        self.assertEqual(
            ["observation.000.ndjson"], os.listdir(os.path.join(self.output_path, "observation"))
        )

    async def test_multiple_tasks(self):
        # Grab all observations before we mock anything
        loaded = loaders.FhirNdjsonLoader(store.Root(self.input_path)).load_resources(
            {"Observation", "Patient"}
        )

        def fake_load_resources(internal_self, resources):
            del internal_self
            # Confirm we only tried to load two resources
            self.assertEqual({"Observation", "Patient"}, resources)
            return loaded

        with mock.patch.object(loaders.FhirNdjsonLoader, "load_resources", new=fake_load_resources):
            await self.run_etl(tasks=["observation", "patient"])

        # Confirm we only wrote the two resources
        self.assertEqual(
            {"etl__completion", "observation", "patient", "JobConfig"},
            set(os.listdir(self.output_path)),
        )
        self.assertEqual(
            ["observation.000.ndjson"], os.listdir(os.path.join(self.output_path, "observation"))
        )
        self.assertEqual(
            ["patient.000.ndjson"], os.listdir(os.path.join(self.output_path, "patient"))
        )

    async def test_codebook_is_saved_during(self):
        """Verify that we are saving the codebook as we go"""
        # Clear out the saved test codebook first
        codebook_path = os.path.join(self.phi_path, "codebook.json")
        os.remove(codebook_path)

        # Cause a system exit as soon as we try to write a file.
        # The goal is that the codebook is already in place by this time.
        with self.assertRaises(SystemExit):
            with mock.patch(
                "cumulus_etl.formats.ndjson.NdjsonFormat.write_format", side_effect=SystemExit
            ):
                await self.run_etl(tasks=["patient"])

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

    async def test_errors_to_must_be_empty(self):
        with self.assertRaises(SystemExit) as cm:
            await self.run_etl(errors_to=self.phi_path)  # it already has a codebook there
        self.assertEqual(errors.FOLDER_NOT_EMPTY, cm.exception.code)

    async def test_errors_to_passed_to_tasks(self):
        with self.assertRaises(SystemExit):
            with mock.patch("cumulus_etl.etl.cli.etl_job", side_effect=SystemExit) as mock_etl_job:
                await self.run_etl(errors_to=f"{self.tmpdir}/errors")
        self.assertEqual(mock_etl_job.call_args[0][0].dir_errors, f"{self.tmpdir}/errors")

    async def test_resume_url_passed_to_loader(self):
        with self.assertRaises(SystemExit):
            with mock.patch(
                "cumulus_etl.loaders.FhirNdjsonLoader", side_effect=SystemExit
            ) as mock_loader:
                await self.run_etl("--resume=https://blarg/", input_path="http://example.invalid/")
        self.assertEqual(mock_loader.call_args[1]["resume"], "https://blarg/")

    @respx.mock
    async def test_bulk_no_auth(self):
        """Verify that if no auth is provided, we'll error out well."""
        respx.get("https://localhost:12345/metadata").respond(401)
        respx.get("https://localhost:12345/$export?_type=Patient").respond(401)

        # Now run the ETL on that new input dir without any server auth config provided
        with self.assertRaises(SystemExit) as cm:
            await self.run_etl(input_path="https://localhost:12345/", tasks=["patient"])
        self.assertEqual(errors.BULK_EXPORT_FAILED, cm.exception.code)

    @ddt.data(
        "--export-to=output/dir",
        "--since=2024",
        "--until=2024",
        "--resume=http://example.com/",
    )
    async def test_bulk_no_url(self, bulk_arg):
        """Verify that if no FHIR URL is provided, but export args *are*, we'll error out."""
        with self.assertRaises(SystemExit) as cm:
            await self.run_etl(bulk_arg)
        self.assertEqual(errors.ARGS_CONFLICT, cm.exception.code)

    async def test_no_ms_tool(self):
        """Verify that we require the MS tool to be in PATH."""
        self.patch_dict(os.environ, {"PATH": "/nothing-here"})
        with self.assertRaises(SystemExit) as cm:
            await self.run_etl(skip_init_checks=False)
        self.assertEqual(cm.exception.code, errors.MSTOOL_MISSING)

    @mock.patch("cumulus_etl.etl.tasks.basic_tasks.ProcedureTask.init_check")
    async def test_task_init_checks(self, mock_check):
        """Verify that we check task requirements."""
        mock_check.side_effect = ZeroDivisionError
        with self.assertRaises(ZeroDivisionError):
            await self.run_etl(skip_init_checks=False)

    @ddt.data(
        # First line is CLI args
        # Second line is what loader will represent as the group/time
        # Third line is what we expect to use as the group/time
        (
            {"export_group": "CLI", "export_timestamp": "2020-01-02"},
            ("Loader", datetime.datetime(2010, 12, 12)),
            ("CLI", datetime.datetime(2020, 1, 2)),
        ),
        (
            {"export_group": None, "export_timestamp": None},
            ("Loader", datetime.datetime(2010, 12, 12)),
            ("Loader", datetime.datetime(2010, 12, 12)),
        ),
        (
            {"export_group": "CLI", "export_timestamp": None},
            (None, None),
            None,  # errors out
        ),
        (
            {"export_group": None, "export_timestamp": "2020-01-02"},
            (None, None),
            None,  # errors out
        ),
        (
            {"export_group": None, "export_timestamp": None},
            (None, None),
            None,  # errors out
        ),
    )
    @ddt.unpack
    async def test_completion_args(self, etl_args, loader_vals, expected_vals):
        """Verify that we parse completion args with the correct fallbacks and checks."""
        # Grab all observations before we mock anything
        observations = await loaders.FhirNdjsonLoader(store.Root(self.input_path)).load_resources(
            {"Observation"}
        )
        observations.group_name = loader_vals[0]
        observations.export_datetime = loader_vals[1]

        with (
            self.assertRaises(SystemExit) as cm,
            mock.patch("cumulus_etl.etl.cli.etl_job", side_effect=SystemExit) as mock_etl_job,
            mock.patch.object(
                loaders.FhirNdjsonLoader, "load_resources", return_value=observations
            ),
        ):
            await self.run_etl(tasks=["observation"], **etl_args)

        if expected_vals is None:
            self.assertEqual(errors.COMPLETION_ARG_MISSING, cm.exception.code)
        else:
            config = mock_etl_job.call_args[0][0]
            self.assertEqual(expected_vals[0], config.export_group_name)
            self.assertEqual(expected_vals[1], config.export_datetime)

    async def test_deleted_ids_passed_down(self):
        """Verify that we parse pass along any deleted ids to the JobConfig."""
        with tempfile.TemporaryDirectory() as tmpdir:
            results = loaders.LoaderResults(
                directory=common.RealDirectory(tmpdir), deleted_ids={"Observation": {"obs1"}}
            )

            with (
                self.assertRaises(SystemExit),
                mock.patch("cumulus_etl.etl.cli.etl_job", side_effect=SystemExit) as mock_etl_job,
                mock.patch.object(loaders.FhirNdjsonLoader, "load_resources", return_value=results),
            ):
                await self.run_etl(tasks=["observation"])

        self.assertEqual(mock_etl_job.call_count, 1)
        config = mock_etl_job.call_args[0][0]
        self.assertEqual({"Observation": {"obs1"}}, config.deleted_ids)

    @ddt.data(["patient"], None)
    async def test_missing_resources(self, tasks):
        with tempfile.TemporaryDirectory() as tmpdir:
            with self.assertRaises(SystemExit) as cm:
                await self.run_etl(tasks=tasks, input_path=tmpdir)
        self.assertEqual(errors.MISSING_REQUESTED_RESOURCES, cm.exception.code)

    async def test_allow_missing_resources(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            await self.run_etl("--allow-missing-resources", tasks=["patient"], input_path=tmpdir)

        self.assertEqual("", common.read_text(f"{self.output_path}/patient/patient.000.ndjson"))

    async def test_missing_resources_skips_tasks(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            common.write_json(f"{tmpdir}/p.ndjson", {"id": "A", "resourceType": "Patient"})
            await self.run_etl(input_path=tmpdir)

        self.assertEqual(
            {"etl__completion", "patient", "JobConfig"}, set(os.listdir(self.output_path))
        )

    async def test_unknown_extensions_get_printed(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            common.write_json(
                f"{tmpdir}/unknown-extension.ndjson",
                {"id": "A", "resourceType": "Patient", "extension": [{"url": "unknown"}]},
            )
            common.write_json(
                f"{tmpdir}/unknown-modifier-extension.ndjson",
                {"id": "B", "resourceType": "Patient", "modifierExtension": [{"url": "unknown"}]},
            )

            stdout = io.StringIO()
            with contextlib.redirect_stdout(stdout):
                await self.run_etl(input_path=tmpdir)

        self.assertIn("Unrecognized extensions dropped from resources:", stdout.getvalue())
        self.assertIn(
            "Resources skipped due to unrecognized modifier extensions:", stdout.getvalue()
        )

    async def test_compressed_input(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            with gzip.open(f"{tmpdir}/patients.ndjson.gz", "wt", encoding="utf8") as f:
                json.dump({"resourceType": "Patient", "id": "pat1"}, f)
            await self.run_etl(tasks=["patient"], input_path=tmpdir)

        self.assertEqual(
            common.read_json(f"{self.output_path}/patient/patient.000.ndjson"),
            {
                "resourceType": "Patient",
                "id": "50ffe70a1bdf3b6e73adac15e4ab7f9d7e247466d7a6c395c2ae9098741a62bd",
            },
        )


class TestEtlJobConfig(BaseEtlSimple):
    """Test case for the job config logging data"""

    def setUp(self):
        super().setUp()
        self.job_config_path = os.path.join(self.output_path, "JobConfig/2021-09-14__21.23.45")

    def read_config_file(self, name: str) -> dict:
        full_path = os.path.join(self.job_config_path, name)
        with open(full_path, encoding="utf8") as f:
            return json.load(f)

    async def test_serialization(self):
        """Verify that everything makes it from command line to the log file"""
        await self.run_etl(
            batch_size=100,
            comment="Run by foo on machine bar",
            tasks=["condition", "patient"],
        )
        config_file = self.read_config_file("job_config.json")
        self.assertEqual(
            {
                "dir_input": self.input_path,
                "dir_output": self.output_path,
                "dir_phi": self.phi_path,
                "path": f"{self.job_config_path}/job_config.json",
                "input_format": "ndjson",
                "output_format": "ndjson",
                "comment": "Run by foo on machine bar",
                "batch_size": 100,
                "tasks": "patient,condition",
                "export_group_name": "test-group",
                "export_timestamp": "2020-10-13T12:00:20-05:00",
                "export_url": "https://example.org/fhir/$export",
            },
            config_file,
        )


class TestEtlJobContext(BaseEtlSimple):
    """Test case for the job context data"""

    def setUp(self):
        super().setUp()
        self.context_path = os.path.join(self.phi_path, "context.json")

    async def test_context_updated_on_success(self):
        """Verify that we update the success timestamp etc. when the job succeeds"""
        await self.run_etl()
        job_context = context.JobContext(self.context_path)
        self.assertEqual(FROZEN_TIME_UTC, job_context.last_successful_datetime)
        self.assertEqual(self.input_path, job_context.last_successful_input_dir)
        self.assertEqual(self.output_path, job_context.last_successful_output_dir)

    async def test_context_not_updated_on_failure(self):
        """Verify that we don't update the success timestamp etc. when the job fails"""
        input_context = {
            "last_successful_datetime": "2000-01-01T10:10:10+00:00",
            "last_successful_input": "/input",
            "last_successful_output": "/output",
        }
        common.write_json(self.context_path, input_context)

        with mock.patch("cumulus_etl.etl.cli.etl_job", side_effect=ZeroDivisionError):
            with self.assertRaises(ZeroDivisionError):
                await self.run_etl()

        # Confirm we didn't change anything
        self.assertEqual(input_context, common.read_json(self.context_path))


class TestEtlFormats(BaseEtlSimple):
    """Test case for each of the formats we support"""

    async def test_etl_job_ndjson(self):
        await self.run_etl()
        self.assert_output_equal()

    async def test_etl_job_deltalake(self):
        # deltalake should be default output format, so pass in None to test that
        await self.run_etl(output_format=None, tasks=["condition"])

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
                "_delta_log/00000000000000000000.json",  # create
                "_delta_log/.00000000000000000000.json.crc",
                "_delta_log/00000000000000000000.crc",
                "_delta_log/.00000000000000000000.crc.crc",
                "_delta_log/00000000000000000001.json",  # merge
                "_delta_log/.00000000000000000001.json.crc",
                "_delta_log/00000000000000000001.crc",
                "_delta_log/.00000000000000000001.crc.crc",
                "_delta_log/00000000000000000002.json",  # optimize
                "_delta_log/.00000000000000000002.json.crc",
                "_delta_log/00000000000000000002.crc",
                "_delta_log/.00000000000000000002.crc.crc",
                "_delta_log/00000000000000000003.json",  # vacuum start
                "_delta_log/.00000000000000000003.json.crc",
                "_delta_log/00000000000000000003.crc",
                "_delta_log/.00000000000000000003.crc.crc",
                "_delta_log/00000000000000000004.json",  # vacuum end
                "_delta_log/.00000000000000000004.json.crc",
                "_delta_log/00000000000000000004.crc",
                "_delta_log/.00000000000000000004.crc.crc",
                "_delta_log/_last_vacuum_info",
                "_delta_log/._last_vacuum_info.crc",
                "_symlink_format_manifest/manifest",
                "_symlink_format_manifest/.manifest.crc",
            },
            metadata_files,
        )

        # Expect two data files - one will be original (now marked as deleted from optimize call)
        # and the other will be the new optimized file.
        self.assertEqual(2, len(data_files))
        self.assertRegex(data_files.pop(), r"part-00000-.*-c000.snappy.parquet")

        self.assertEqual(2, len(data_crc_files))
        self.assertRegex(data_crc_files.pop(), r".part-00000-.*-c000.snappy.parquet.crc")


class TestEtlOnS3(S3Mixin, BaseEtlSimple):
    """Test case for our support of writing to S3"""

    async def test_etl_job_s3(self):
        await self.run_etl(
            output_path="s3://mockbucket/root",
            tasks=[
                # Just a few to get a taste of how this behaves
                "condition",
                "medicationrequest",
                "patient",
            ],
        )

        all_files = {x for x in self.s3fs.find("mockbucket/root") if "/JobConfig/" not in x}
        self.assertEqual(
            {
                "mockbucket/root/condition/condition.000.ndjson",
                "mockbucket/root/etl__completion/etl__completion.000.ndjson",
                "mockbucket/root/etl__completion/etl__completion.001.ndjson",
                "mockbucket/root/etl__completion/etl__completion.002.ndjson",
                "mockbucket/root/medication/medication.000.ndjson",
                "mockbucket/root/medicationrequest/medicationrequest.000.ndjson",
                "mockbucket/root/patient/patient.000.ndjson",
            },
            all_files,
        )

        # Confirm we did not accidentally create an 's3:' directory locally
        # because we misinterpreted the s3 path as a local path
        self.assertFalse(os.path.exists("s3:"))


class TestEtlNlp(BaseEtlSimple):
    """Test case for the cTAKES/cNLP responses"""

    CACHE_FOLDER = "covid_symptom_v4"

    def setUp(self):
        super().setUp()
        # sha256 checksums of the two test patient notes
        self.expected_checksums = [
            "5db841c4c46d8a25fbb1891fd1eb352170278fa2b931c1c5edebe09a06582fb5",
            "47f2c3b7cd114908b8c1a56bb26db2a9d9901a5d5571a327a69474a641a9fc3d",
        ]

    def path_for_checksum(self, prefix, checksum):
        return os.path.join(
            self.phi_path, "ctakes-cache", prefix, checksum[0:4], f"sha256-{checksum}.json"
        )

    def read_symptoms(self):
        """Loads the output symptoms ndjson from disk"""
        path = os.path.join(
            self.output_path, "covid_symptom__nlp_results", "covid_symptom__nlp_results.000.ndjson"
        )
        with open(path, encoding="utf8") as f:
            lines = f.readlines()
        return [json.loads(line) for line in lines]

    async def test_stores_cached_json(self):
        await self.run_etl(tasks=["covid_symptom__nlp_results"])

        self.assertTrue(self.was_ctakes_called())

        facts = [
            "Notes for fever",
            "Notes! for fever",
        ]

        for index, checksum in enumerate(self.expected_checksums):
            ner = fake_ctakes_extract(facts[index])
            self.assertEqual(
                ner.as_json(), common.read_json(self.path_for_checksum(self.CACHE_FOLDER, checksum))
            )
            self.assertEqual(
                [0, 0],
                common.read_json(self.path_for_checksum(f"{self.CACHE_FOLDER}-cnlp_v2", checksum)),
            )

    async def test_does_not_hit_server_if_cache_exists(self):
        for index, checksum in enumerate(self.expected_checksums):
            # Write out some fake results to the cache location
            filename = self.path_for_checksum(self.CACHE_FOLDER, checksum)
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
                                {
                                    "code": "68235000",
                                    "cui": "C0027424",
                                    "codingScheme": "SNOMEDCT_US",
                                    "tui": "T184",
                                },
                            ],
                        }
                    ],
                },
            )

            cnlp_filename = self.path_for_checksum(f"{self.CACHE_FOLDER}-cnlp_v2", checksum)
            os.makedirs(os.path.dirname(cnlp_filename))
            common.write_json(cnlp_filename, [0])

        await self.run_etl(tasks=["covid_symptom__nlp_results"])

        # We should never have called our mock cTAKES server
        self.assertFalse(self.was_ctakes_called())
        self.assertEqual(0, self.cnlp_mock.call_count)

        # And we should see our fake cached results in the output
        symptoms = self.read_symptoms()
        self.assertEqual(2, len(symptoms))
        self.assertEqual({"foobar0", "foobar1"}, {x["match"]["text"] for x in symptoms})
        for symptom in symptoms:
            self.assertEqual(
                {("68235000", "C0027424")},
                {(x["code"], x["cui"]) for x in symptom["match"]["conceptAttributes"]},
            )

    @respx.mock
    async def test_gracefully_allows_no_server_config(self):
        """
        Verify that if no server is provided, we'll just continue as best we can.

        This is useful in cases where some notes have been inlined, but not everything could be.
        """
        # Make new input dir with some docrefs that only have a URL, no data.
        with tempfile.TemporaryDirectory() as tmpdir:
            with common.NdjsonWriter(f"{tmpdir}/DocumentReference.ndjson") as writer:
                src = store.Root(self.input_path)
                docref_iter = common.read_resource_ndjson(src, "DocumentReference")

                # Replace first row's data with a partial URL
                first = next(docref_iter)
                del first["content"][0]["attachment"]["data"]
                first["content"][0]["attachment"]["url"] = "Binary/blarg"
                writer.write(first)

                # And second row with an absolute (forbidden) URL
                second = next(docref_iter)
                del second["content"][0]["attachment"]["data"]
                second["content"][0]["attachment"]["url"] = "https://localhost:12345/Binary/blarg"
                writer.write(second)

            respx.get("https://localhost:12345/Binary/blarg").respond(401)

            # Now run the ETL on that new input dir without any server auth config provided
            with self.assertRaises(SystemExit) as cm:
                await self.run_etl(tasks=["covid_symptom__nlp_results"], input_path=tmpdir)

            # Should exit because the task will mark itself as failed
            self.assertEqual(errors.TASK_FAILED, cm.exception.code)

    async def test_cnlp_rejects(self):
        """Verify that if the cnlp server negates a match, it does not show up"""
        # First match is fever, second is nausea
        self.cnlp_mock.side_effect = lambda *args, **kwargs: [Polarity.neg, Polarity.pos]
        await self.run_etl(tasks=["covid_symptom__nlp_results"])

        symptoms = self.read_symptoms()
        self.assertEqual(2, len(symptoms))
        # Confirm that the only symptom to survive was the second nausea one
        self.assertEqual(
            {("422587007", "C0027497")},
            {(x["code"], x["cui"]) for x in symptoms[0]["match"]["conceptAttributes"]},
        )

    async def test_non_covid_symptoms_skipped(self):
        """Verify that the 'itch' symptom in our mock response does not make it to the output table"""
        await self.run_etl(tasks=["covid_symptom__nlp_results"])

        symptoms = self.read_symptoms()
        # the second word ("for") is the fever word
        self.assertEqual({"for"}, {x["match"]["text"] for x in symptoms})
        attributes = itertools.chain.from_iterable(
            symptom["match"]["conceptAttributes"] for symptom in symptoms
        )
        cuis = {x["cui"] for x in attributes}
        self.assertEqual({"C0027497", "C0015967"}, cuis)  # notably, no C0033774 itch CUI

    async def test_group_updates(self):
        """Verify that if we generate a smaller set of NLP symptoms, any old unused rows get deleted"""
        # This test uses delta lake even though it is a bit slow, just because that's the only current output format
        # that supports this feature (EtlTask.group_field). Other output formats just delete the full table each run.

        def table_ids():
            path = os.path.join(self.output_path, "covid_symptom__nlp_results")
            rows = read_delta_lake(path)
            return [row["id"] for row in rows]

        # Get a baseline, with two symptoms per document
        await self.run_etl(output_format="deltalake", tasks=["covid_symptom__nlp_results"])
        self.assertEqual(
            [
                "c601849ceffe49dba22ee952533ac87928cd7a472dee6d0390d53c9130519971.0",
                "c601849ceffe49dba22ee952533ac87928cd7a472dee6d0390d53c9130519971.1",
                "f29736c29af5b962b3947fd40bed6b8c3e97c642b72aaa08e082fec05148e7dd.0",
                "f29736c29af5b962b3947fd40bed6b8c3e97c642b72aaa08e082fec05148e7dd.1",
            ],
            table_ids(),
        )

        # Now negate the second symptom, and notice that it has been dropped in the results for each docref
        shutil.rmtree(os.path.join(self.phi_path, "ctakes-cache"))  # clear cached results
        self.cnlp_mock.side_effect = lambda *args, **kwargs: [Polarity.pos, Polarity.neg]
        await self.run_etl(output_format="deltalake", tasks=["covid_symptom__nlp_results"])
        self.assertEqual(
            [
                "c601849ceffe49dba22ee952533ac87928cd7a472dee6d0390d53c9130519971.0",
                "f29736c29af5b962b3947fd40bed6b8c3e97c642b72aaa08e082fec05148e7dd.0",
            ],
            table_ids(),
        )
