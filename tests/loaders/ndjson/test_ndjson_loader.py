"""Tests for ndjson loading (both bulk export and local)"""

import datetime
import os
import tempfile
from unittest import mock

from cumulus_etl import cli, common, errors, loaders, store
from cumulus_etl.loaders.fhir.bulk_export import BulkExporter
from tests.utils import AsyncTestCase


class TestNdjsonLoader(AsyncTestCase):
    """
    Test case for the etl pipeline and ndjson loader.

    i.e. tests for cli.py & ndjson_loader.py

    This does no actual bulk loading.
    """

    def setUp(self):
        super().setUp()
        self.jwks_file = tempfile.NamedTemporaryFile()  # pylint: disable=consider-using-with
        self.jwks_path = self.jwks_file.name
        self.jwks_file.write(b'{"fake":"jwks"}')
        self.jwks_file.flush()

        # Mock out the bulk export code by default. We don't care about actually doing any
        # bulk work in this test case, just confirming the flow.
        exporter_patcher = mock.patch("cumulus_etl.loaders.fhir.ndjson_loader.BulkExporter", spec=BulkExporter)
        self.addCleanup(exporter_patcher.stop)
        self.mock_exporter_class = exporter_patcher.start()
        self.mock_exporter = mock.AsyncMock()
        self.mock_exporter_class.return_value = self.mock_exporter

    @staticmethod
    def _write_log_file(path: str, group: str, timestamp: str) -> None:
        with common.NdjsonWriter(path) as writer:
            writer.write(
                {
                    "eventId": "kickoff",
                    "eventDetail": {"exportUrl": f"https://host/Group/{group}/$export"},
                }
            )
            writer.write(
                {
                    "eventId": "status_complete",
                    "eventDetail": {"transactionTime": timestamp},
                }
            )

    async def test_local_happy_path(self):
        """Do a full local load from a folder."""
        with tempfile.TemporaryDirectory() as tmpdir:
            self._write_log_file(f"{tmpdir}/log.ndjson", "G", "1999-03-14T14:12:10")
            with common.NdjsonWriter(f"{tmpdir}/Patient.ndjson") as writer:
                writer.write({"id": "A"})

            loader = loaders.FhirNdjsonLoader(store.Root(tmpdir))
            loaded_dir = await loader.load_all(["Patient"])

        self.assertEqual(["Patient.ndjson"], os.listdir(loaded_dir.name))
        self.assertEqual({"id": "A"}, common.read_json(f"{loaded_dir.name}/Patient.ndjson"))
        self.assertEqual("G", loader.group_name)
        self.assertEqual(datetime.datetime.fromisoformat("1999-03-14T14:12:10"), loader.export_datetime)

    # At some point, we do want to make this fatal.
    # But not while this feature is still optional.
    async def test_log_parsing_is_non_fatal(self):
        """Do a local load with a bad log setup."""
        with tempfile.TemporaryDirectory() as tmpdir:
            self._write_log_file(f"{tmpdir}/log.1.ndjson", "G1", "2001-01-01")
            self._write_log_file(f"{tmpdir}/log.2.ndjson", "G2", "2002-02-02")

            loader = loaders.FhirNdjsonLoader(store.Root(tmpdir))
            await loader.load_all([])

        # We used neither log and didn't error out.
        self.assertIsNone(loader.group_name)
        self.assertIsNone(loader.export_datetime)

    @mock.patch("cumulus_etl.fhir.fhir_client.FhirClient")
    @mock.patch("cumulus_etl.etl.cli.loaders.FhirNdjsonLoader")
    async def test_etl_passes_args(self, mock_loader, mock_client):
        """Verify that we are passed the client ID and JWKS from the command line"""
        mock_loader.side_effect = ValueError  # just to stop the etl pipeline once we get this far

        with self.assertRaises(ValueError):
            await cli.main(
                [
                    "http://localhost:9999",
                    "/tmp/output",
                    "/tmp/phi",
                    "--skip-init-checks",
                    "--input-format=ndjson",
                    "--smart-client-id=x",
                    f"--smart-jwks={self.jwks_path}",
                    "--export-to=/tmp/exported",
                    "--since=2018",
                    "--until=2020",
                ]
            )

        self.assertEqual(1, mock_client.call_count)
        self.assertEqual("x", mock_client.call_args[1]["smart_client_id"])
        self.assertEqual({"fake": "jwks"}, mock_client.call_args[1]["smart_jwks"])
        self.assertEqual(1, mock_loader.call_count)
        self.assertEqual("/tmp/exported", mock_loader.call_args[1]["export_to"])
        self.assertEqual("2018", mock_loader.call_args[1]["since"])
        self.assertEqual("2020", mock_loader.call_args[1]["until"])

    @mock.patch("cumulus_etl.fhir.fhir_client.FhirClient")
    async def test_reads_client_id_from_file(self, mock_client):
        """Verify that we try to read a client ID from a file."""
        mock_client.side_effect = ValueError  # just to stop the etl pipeline once we get this far

        # First, confirm string is used directly if file doesn't exist
        with self.assertRaises(ValueError):
            await cli.main(
                [
                    "http://localhost:9999",
                    "/tmp/output",
                    "/tmp/phi",
                    "--skip-init-checks",
                    "--smart-client-id=/direct-string",
                ]
            )
        self.assertEqual("/direct-string", mock_client.call_args[1]["smart_client_id"])

        # Now read from a file that exists
        with tempfile.NamedTemporaryFile(buffering=0) as file:
            file.write(b"\ninside-file\n")
            with self.assertRaises(ValueError):
                await cli.main(
                    [
                        "http://localhost:9999",
                        "/tmp/output",
                        "/tmp/phi",
                        "--skip-init-checks",
                        f"--smart-client-id={file.name}",
                    ]
                )
            self.assertEqual("inside-file", mock_client.call_args[1]["smart_client_id"])

    @mock.patch("cumulus_etl.fhir.fhir_client.FhirClient")
    async def test_reads_bearer_token(self, mock_client):
        """Verify that we read the bearer token file"""
        mock_client.side_effect = ValueError  # just to stop the etl pipeline once we get this far

        with tempfile.NamedTemporaryFile(buffering=0) as file:
            file.write(b"\ninside-file\n")
            with self.assertRaises(ValueError):
                await cli.main(
                    [
                        "http://localhost:9999",
                        "/tmp/output",
                        "/tmp/phi",
                        "--skip-init-checks",
                        f"--bearer-token={file.name}",
                    ]
                )
            self.assertEqual("inside-file", mock_client.call_args[1]["bearer_token"])

    @mock.patch("cumulus_etl.fhir.fhir_client.FhirClient")
    async def test_reads_basic_auth(self, mock_client):
        """Verify that we read the basic password file and pass it along"""
        mock_client.side_effect = ValueError  # just to stop the etl pipeline once we get this far

        with tempfile.NamedTemporaryFile(buffering=0) as file:
            file.write(b"\ninside-file\n")
            with self.assertRaises(ValueError):
                await cli.main(
                    [
                        "http://localhost:9999",
                        "/tmp/output",
                        "/tmp/phi",
                        "--skip-init-checks",
                        "--basic-user=UserName",
                        f"--basic-passwd={file.name}",
                    ]
                )

        self.assertEqual("UserName", mock_client.call_args[1]["basic_user"])
        self.assertEqual("inside-file", mock_client.call_args[1]["basic_password"])

    @mock.patch("cumulus_etl.fhir.fhir_client.FhirClient")
    async def test_fhir_url(self, mock_client):
        """Verify that we handle the user provided --fhir-client correctly"""
        mock_client.side_effect = ValueError  # just to stop the etl pipeline once we get this far

        # Confirm that we chop an input URL down to a base server URL
        with self.assertRaises(ValueError):
            await cli.main(
                [
                    "https://example.com/hello1/Group/1234",
                    "/tmp/output",
                    "/tmp/phi",
                    "--skip-init-checks",
                ]
            )
        self.assertEqual("https://example.com/hello1/", mock_client.call_args[0][0])

        # Confirm that we don't allow conflicting URLs
        with self.assertRaises(SystemExit):
            await cli.main(
                [
                    "http://localhost:9999",
                    "/tmp/output",
                    "/tmp/phi",
                    "--skip-init-checks",
                    "--fhir-url=https://example.com/hello2",
                ]
            )

        # But a subset --fhir-url is fine
        with self.assertRaises(ValueError):
            await cli.main(
                [
                    "https://example.com/hello3/Group/1234",
                    "/tmp/output",
                    "/tmp/phi",
                    "--skip-init-checks",
                    "--fhir-url=https://example.com/hello3",
                ]
            )
        self.assertEqual("https://example.com/hello3", mock_client.call_args[0][0])

        # Now do a normal use of --fhir-url
        mock_client.side_effect = ValueError  # just to stop the etl pipeline once we get this far
        with self.assertRaises(ValueError):
            await cli.main(
                [
                    "/tmp/input",
                    "/tmp/output",
                    "/tmp/phi",
                    "--skip-init-checks",
                    "--fhir-url=https://example.com/hello4",
                ]
            )
        self.assertEqual("https://example.com/hello4", mock_client.call_args[0][0])

    @mock.patch("cumulus_etl.fhir.fhir_client.FhirClient")
    async def test_export_flow(self, mock_client):
        """
        Verify that we make the right calls down as far as the bulk export helper classes, with the right resources.
        """
        self.mock_exporter.export.side_effect = ValueError  # stop us when we get this far, but also confirm we call it

        with self.assertRaises(ValueError):
            await cli.main(
                [
                    "http://localhost:9999",
                    "/tmp/output",
                    "/tmp/phi",
                    "--skip-init-checks",
                    "--task=condition,encounter",
                ]
            )

        expected_resources = {"Condition", "Encounter"}
        self.assertEqual(1, mock_client.call_count)
        self.assertEqual(expected_resources, mock_client.call_args[0][1])
        self.assertEqual(1, self.mock_exporter_class.call_count)
        self.assertEqual(expected_resources, set(self.mock_exporter_class.call_args[0][1]))

    async def test_fatal_errors_are_fatal(self):
        """Verify that when a FatalError is raised, we do really quit"""
        self.mock_exporter.export.side_effect = errors.FatalError

        with self.assertRaises(SystemExit) as cm:
            await loaders.FhirNdjsonLoader(store.Root("http://localhost:9999"), mock.AsyncMock()).load_all(["Patient"])

        self.assertEqual(1, self.mock_exporter.export.call_count)
        self.assertEqual(errors.BULK_EXPORT_FAILED, cm.exception.code)

    async def test_export_to_folder_happy_path(self):
        with tempfile.TemporaryDirectory() as tmpdir:

            async def fake_export() -> None:
                output_dir = self.mock_exporter_class.call_args[0][3]
                common.write_json(f"{output_dir}/Patient.ndjson", {"id": "A"})
                common.write_json(f"{output_dir}/log.ndjson", {"eventId": "kickoff"})

            self.mock_exporter.export.side_effect = fake_export

            target = f"{tmpdir}/target"
            loader = loaders.FhirNdjsonLoader(store.Root("http://localhost:9999"), mock.AsyncMock(), export_to=target)
            folder = await loader.load_all(["Patient"])

            # Confirm export folder still has the data (and log) we created above in the mock
            self.assertTrue(os.path.isdir(target))
            self.assertEqual(target, self.mock_exporter_class.call_args[0][3])
            self.assertEqual({"Patient.ndjson", "log.ndjson"}, set(os.listdir(target)))
            self.assertEqual({"id": "A"}, common.read_json(f"{target}/Patient.ndjson"))
            self.assertEqual({"eventId": "kickoff"}, common.read_json(f"{target}/log.ndjson"))

            # Confirm the returned dir has only the data (we don't want to confuse MS tool with logs)
            self.assertNotEqual(folder.name, target)
            self.assertEqual({"Patient.ndjson"}, set(os.listdir(folder.name)))
            self.assertEqual({"id": "A"}, common.read_json(f"{folder.name}/Patient.ndjson"))

    async def test_export_internal_folder_happy_path(self):
        """Test that we can also safely export without an export-to folder involved"""

        async def fake_export() -> None:
            output_dir = self.mock_exporter_class.call_args[0][3]
            common.write_json(f"{output_dir}/Patient.ndjson", {"id": "A"})
            common.write_json(f"{output_dir}/log.ndjson", {"eventId": "kickoff"})

        self.mock_exporter.export.side_effect = fake_export

        loader = loaders.FhirNdjsonLoader(store.Root("http://localhost:9999"), mock.AsyncMock())
        folder = await loader.load_all(["Patient"])

        # Confirm the returned dir has only the data (we don't want to confuse MS tool with logs)
        self.assertEqual({"Patient.ndjson"}, set(os.listdir(folder.name)))
        self.assertEqual({"id": "A"}, common.read_json(f"{folder.name}/Patient.ndjson"))

    async def test_export_to_folder_has_contents(self):
        """Verify we fail if an export folder already has contents"""
        with tempfile.TemporaryDirectory() as tmpdir:
            os.mkdir(f"{tmpdir}/stuff")
            loader = loaders.FhirNdjsonLoader(store.Root("http://localhost:9999"), mock.AsyncMock(), export_to=tmpdir)
            with self.assertRaises(SystemExit) as cm:
                await loader.load_all([])
        self.assertEqual(cm.exception.code, errors.FOLDER_NOT_EMPTY)

    async def test_export_to_folder_not_local(self):
        """Verify we fail if an export folder is not local"""
        loader = loaders.FhirNdjsonLoader(store.Root("http://localhost:9999"), mock.AsyncMock(), export_to="http://foo")
        with self.assertRaises(SystemExit) as cm:
            await loader.load_all([])
        self.assertEqual(cm.exception.code, errors.BULK_EXPORT_FOLDER_NOT_LOCAL)
