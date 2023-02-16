"""Tests for ndjson loading (both bulk export and local)"""

import os
import tempfile
import unittest
from unittest import mock

from cumulus import errors, etl, loaders, store
from cumulus.fhir_client import FatalError
from cumulus.loaders.fhir.bulk_export import BulkExporter


class TestNdjsonLoader(unittest.IsolatedAsyncioTestCase):
    """
    Test case for the etl pipeline and ndjson loader.

    i.e. tests for etl.py & ndjson_loader.py

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
        exporter_patcher = mock.patch("cumulus.loaders.fhir.ndjson_loader.BulkExporter", spec=BulkExporter)
        self.addCleanup(exporter_patcher.stop)
        self.mock_exporter_class = exporter_patcher.start()
        self.mock_exporter = mock.AsyncMock()
        self.mock_exporter_class.return_value = self.mock_exporter

    @mock.patch("cumulus.etl.fhir_client.FhirClient")
    @mock.patch("cumulus.etl.loaders.FhirNdjsonLoader")
    async def test_etl_passes_args(self, mock_loader, mock_client):
        """Verify that we are passed the client ID and JWKS from the command line"""
        mock_loader.side_effect = ValueError  # just to stop the etl pipeline once we get this far

        with self.assertRaises(ValueError):
            await etl.main(
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

    @mock.patch("cumulus.etl.fhir_client.FhirClient")
    async def test_reads_client_id_from_file(self, mock_client):
        """Verify that we try to read a client ID from a file."""
        mock_client.side_effect = ValueError  # just to stop the etl pipeline once we get this far

        # First, confirm string is used directly if file doesn't exist
        with self.assertRaises(ValueError):
            await etl.main(
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
                await etl.main(
                    [
                        "http://localhost:9999",
                        "/tmp/output",
                        "/tmp/phi",
                        "--skip-init-checks",
                        f"--smart-client-id={file.name}",
                    ]
                )
            self.assertEqual("inside-file", mock_client.call_args[1]["smart_client_id"])

    @mock.patch("cumulus.etl.fhir_client.FhirClient")
    async def test_reads_bearer_token(self, mock_client):
        """Verify that we read the bearer token file"""
        mock_client.side_effect = ValueError  # just to stop the etl pipeline once we get this far

        with tempfile.NamedTemporaryFile(buffering=0) as file:
            file.write(b"\ninside-file\n")
            with self.assertRaises(ValueError):
                await etl.main(
                    [
                        "http://localhost:9999",
                        "/tmp/output",
                        "/tmp/phi",
                        "--skip-init-checks",
                        f"--bearer-token={file.name}",
                    ]
                )
            self.assertEqual("inside-file", mock_client.call_args[1]["bearer_token"])

    @mock.patch("cumulus.etl.fhir_client.FhirClient")
    async def test_reads_basic_auth(self, mock_client):
        """Verify that we read the basic password file and pass it along"""
        mock_client.side_effect = ValueError  # just to stop the etl pipeline once we get this far

        with tempfile.NamedTemporaryFile(buffering=0) as file:
            file.write(b"\ninside-file\n")
            with self.assertRaises(ValueError):
                await etl.main(
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

    @mock.patch("cumulus.etl.fhir_client.FhirClient")
    async def test_fhir_url(self, mock_client):
        """Verify that we handle the user provided --fhir-client correctly"""
        mock_client.side_effect = ValueError  # just to stop the etl pipeline once we get this far

        # Confirm that we don't allow conflicting URLs
        with self.assertRaises(SystemExit):
            await etl.main(
                [
                    "http://localhost:9999",
                    "/tmp/output",
                    "/tmp/phi",
                    "--skip-init-checks",
                    "--fhir-url=https://example.com/hello",
                ]
            )

        # But a subset --fhir-url is fine
        with self.assertRaises(ValueError):
            await etl.main(
                [
                    "https://example.com/hello/Group/1234",
                    "/tmp/output",
                    "/tmp/phi",
                    "--skip-init-checks",
                    "--fhir-url=https://example.com/hello",
                ]
            )
        self.assertEqual("https://example.com/hello/Group/1234", mock_client.call_args[0][0])

        # Now do a normal use of --fhir-url
        mock_client.side_effect = ValueError  # just to stop the etl pipeline once we get this far
        with self.assertRaises(ValueError):
            await etl.main(
                [
                    "/tmp/input",
                    "/tmp/output",
                    "/tmp/phi",
                    "--skip-init-checks",
                    "--fhir-url=https://example.com/hello",
                ]
            )
        self.assertEqual("https://example.com/hello", mock_client.call_args[0][0])

    @mock.patch("cumulus.etl.fhir_client.FhirClient")
    async def test_export_flow(self, mock_client):
        """
        Verify that we make the right calls down as far as the bulk export helper classes, with the right resources.
        """
        self.mock_exporter.export.side_effect = ValueError  # stop us when we get this far, but also confirm we call it

        with self.assertRaises(ValueError):
            await etl.main(
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
        self.mock_exporter.export.side_effect = FatalError

        with self.assertRaises(SystemExit) as cm:
            await loaders.FhirNdjsonLoader(store.Root("http://localhost:9999"), mock.AsyncMock()).load_all(["Patient"])

        self.assertEqual(1, self.mock_exporter.export.call_count)
        self.assertEqual(errors.BULK_EXPORT_FAILED, cm.exception.code)

    async def test_export_to_folder_happy_path(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            target = f"{tmpdir}/target"
            loader = loaders.FhirNdjsonLoader(store.Root("http://localhost:9999"), mock.AsyncMock(), export_to=target)
            folder = await loader.load_all([])

            self.assertTrue(os.path.isdir(target))  # confirm it got created
            self.assertEqual(target, self.mock_exporter_class.call_args[0][2])
            self.assertEqual(target, folder.name)

    def test_export_to_folder_has_contents(self):
        """Verify we fail if an export folder already has contents"""
        with tempfile.TemporaryDirectory() as tmpdir:
            os.mkdir(f"{tmpdir}/stuff")
            with self.assertRaises(SystemExit) as cm:
                loaders.FhirNdjsonLoader(store.Root("http://localhost:9999"), mock.AsyncMock(), export_to=tmpdir)
        self.assertEqual(cm.exception.code, errors.BULK_EXPORT_FOLDER_NOT_EMPTY)

    def test_export_to_folder_not_local(self):
        """Verify we fail if an export folder is not local"""
        with self.assertRaises(SystemExit) as cm:
            loaders.FhirNdjsonLoader(store.Root("http://localhost:9999"), mock.AsyncMock(), export_to="http://foo/")
        self.assertEqual(cm.exception.code, errors.BULK_EXPORT_FOLDER_NOT_LOCAL)
