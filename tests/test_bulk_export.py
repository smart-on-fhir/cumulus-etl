"""Tests for bulk export support"""

import contextlib
import io
import tempfile
from json import dumps
from unittest import mock

import ddt
import responses
import respx
from jwcrypto import jwk

from cumulus_etl import cli, common, store
from cumulus_etl.fhir_client import FatalError
from cumulus_etl.loaders.fhir.bulk_export import BulkExporter
from tests.utils import AsyncTestCase, make_response


@ddt.ddt
class TestBulkExporter(AsyncTestCase):
    """
    Test case for bulk export logic.

    i.e. tests for bulk_export.py
    """

    def setUp(self):
        super().setUp()
        self.tmpdir = tempfile.TemporaryDirectory()  # pylint: disable=consider-using-with
        self.server = mock.AsyncMock()

    def make_exporter(self, **kwargs) -> BulkExporter:
        return BulkExporter(self.server, ["Condition", "Patient"], "https://localhost/", self.tmpdir.name, **kwargs)

    async def export(self, **kwargs) -> BulkExporter:
        exporter = self.make_exporter(**kwargs)
        await exporter.export()
        return exporter

    async def test_happy_path(self):
        """Verify an end-to-end bulk export with no problems and no waiting works as expected"""
        self.server.request.side_effect = [
            make_response(status_code=202, headers={"Content-Location": "https://example.com/poll"}),  # kickoff
            make_response(
                json_payload={
                    "output": [
                        {"type": "Condition", "url": "https://example.com/con1"},
                        {"type": "Condition", "url": "https://example.com/con2"},
                        {"type": "Patient", "url": "https://example.com/pat1"},
                    ]
                }
            ),  # status
            make_response(json_payload={"type": "Condition1"}, stream=True),  # download
            make_response(json_payload={"type": "Condition2"}, stream=True),  # download
            make_response(json_payload={"type": "Patient1"}, stream=True),  # download
            make_response(status_code=202),  # delete request
        ]

        await self.export()

        self.assertListEqual(
            [
                mock.call(
                    "GET",
                    "https://localhost/$export?_type=Condition%2CPatient",
                    headers={"Prefer": "respond-async"},
                ),
                mock.call("GET", "https://example.com/poll", headers={"Accept": "application/json"}),
                mock.call(
                    "GET", "https://example.com/con1", headers={"Accept": "application/fhir+ndjson"}, stream=True
                ),
                mock.call(
                    "GET", "https://example.com/con2", headers={"Accept": "application/fhir+ndjson"}, stream=True
                ),
                mock.call(
                    "GET", "https://example.com/pat1", headers={"Accept": "application/fhir+ndjson"}, stream=True
                ),
                mock.call("DELETE", "https://example.com/poll", headers=None),
            ],
            self.server.request.call_args_list,
        )

        self.assertEqual({"type": "Condition1"}, common.read_json(f"{self.tmpdir.name}/Condition.000.ndjson"))
        self.assertEqual({"type": "Condition2"}, common.read_json(f"{self.tmpdir.name}/Condition.001.ndjson"))
        self.assertEqual({"type": "Patient1"}, common.read_json(f"{self.tmpdir.name}/Patient.000.ndjson"))

    async def test_since_until(self):
        """Verify that we send since & until parameters correctly to the server"""
        self.server.request.side_effect = (make_response(status_code=500),)  # early exit

        with self.assertRaises(FatalError):
            await self.export(since="2000-01-01T00:00:00+00.00", until="2010")

        self.assertListEqual(
            [
                mock.call(
                    "GET",
                    "https://localhost/$export?"
                    "_type=Condition%2CPatient&_since=2000-01-01T00%3A00%3A00%2B00.00&_until=2010",
                    headers={"Prefer": "respond-async"},
                ),
            ],
            self.server.request.call_args_list,
        )

    async def test_export_error(self):
        """Verify that we download and present any server-reported errors during the bulk export"""
        self.server.request.side_effect = [
            make_response(status_code=202, headers={"Content-Location": "https://example.com/poll"}),  # kickoff
            make_response(
                json_payload={
                    "error": [
                        {"type": "OperationOutcome", "url": "https://example.com/err1"},
                        {"type": "OperationOutcome", "url": "https://example.com/err2"},
                    ],
                    "output": [  # include an output too, to confirm we don't bother trying to download it
                        {"type": "Condition", "url": "https://example.com/con1"},
                    ],
                }
            ),  # status
            # errors
            make_response(
                json_payload={"type": "OperationOutcome", "issue": [{"severity": "error", "diagnostics": "err1"}]}
            ),
            make_response(
                text='{"type": "OperationOutcome", "issue": [{"severity": "fatal", "details": {"text": "err2"}}]}\n'
                '{"type": "OperationOutcome", "issue": [{"severity": "warning", "diagnostics": "warning1"}]}\n'
                '{"type": "OperationOutcome", "issue": ['
                '{"severity": "error", "code": "err3"}, {"severity": "fatal", "code": "err4"}]}\n'
            ),
            make_response(status_code=202),  # delete request
        ]

        with self.assertRaisesRegex(FatalError, "Errors occurred during export:\n - err1\n - err2\n - err3\n - err4"):
            await self.export()

        self.assertListEqual(
            [
                mock.call(
                    "GET",
                    "https://localhost/$export?_type=Condition%2CPatient",
                    headers={"Prefer": "respond-async"},
                ),
                mock.call("GET", "https://example.com/poll", headers={"Accept": "application/json"}),
                mock.call("GET", "https://example.com/err1", headers={"Accept": "application/fhir+ndjson"}),
                mock.call("GET", "https://example.com/err2", headers={"Accept": "application/fhir+ndjson"}),
                mock.call("DELETE", "https://example.com/poll", headers=None),
            ],
            self.server.request.call_args_list,
        )

    async def test_export_warning(self):
        """Verify that we download and present any server-reported warnings during the bulk export"""
        self.server.request.side_effect = [
            make_response(status_code=202, headers={"Content-Location": "https://example.com/poll"}),  # kickoff
            make_response(
                json_payload={
                    "error": [
                        {"type": "OperationOutcome", "url": "https://example.com/warning1"},
                    ],
                }
            ),  # status
            # warning
            make_response(
                json_payload={"type": "OperationOutcome", "issue": [{"severity": "warning", "diagnostics": "warning1"}]}
            ),
            make_response(status_code=202),  # delete request
        ]

        stdout = io.StringIO()
        with contextlib.redirect_stdout(stdout):
            await self.export()

        self.assertIn("Messages from server:\n - warning1\n", stdout.getvalue())

    async def test_unexpected_status_code(self):
        """Verify that we bail if we see a successful code we don't understand"""
        self.server.request.return_value = make_response(status_code=204)  # "no content"
        with self.assertRaisesRegex(FatalError, "Unexpected status code 204"):
            await self.export()

    @mock.patch("cumulus_etl.loaders.fhir.bulk_export.asyncio.sleep")
    async def test_delay(self, mock_sleep):
        """Verify that we wait the amount of time the server asks us to"""
        self.server.request.side_effect = [
            # Kicking off bulk export
            make_response(status_code=429, headers={"Retry-After": "3600"}),  # one hour
            make_response(status_code=202, headers={"Content-Location": "https://example.com/poll"}),  # kickoff done
            # Checking status of bulk export
            make_response(status_code=429),  # default of one minute
            make_response(status_code=202, headers={"Retry-After": "18000"}),  # five hours (gets limited to five min)
            make_response(status_code=429, headers={"Retry-After": "82800"}),  # 23 hours (putting us over a day)
        ]

        exporter = self.make_exporter()
        with self.assertRaisesRegex(FatalError, "Timed out waiting"):
            await exporter.export()

        # 86760 == 24 hours + six minutes
        self.assertEqual(86760, exporter._total_wait_time)  # pylint: disable=protected-access

        self.assertListEqual(
            [
                mock.call(3600),
                mock.call(60),
                mock.call(300),
                mock.call(82800),
            ],
            mock_sleep.call_args_list,
        )

    async def test_delete_if_interrupted(self):
        """Verify that we still delete the export on the server if we raise an exception during the middle of export"""
        self.server.request.side_effect = [
            make_response(status_code=202, headers={"Content-Location": "https://example.com/poll"}),  # kickoff done
            FatalError("Test Status Call Failed"),  # status error
            make_response(status_code=501),  # also verify that an error during delete does not override the first
        ]

        with self.assertRaisesRegex(FatalError, "Test Status Call Failed"):
            await self.export()

        self.assertListEqual(
            [
                mock.call(
                    "GET",
                    "https://localhost/$export?_type=Condition%2CPatient",
                    headers={"Prefer": "respond-async"},
                ),
                mock.call("GET", "https://example.com/poll", headers={"Accept": "application/json"}),
                mock.call("DELETE", "https://example.com/poll", headers=None),
            ],
            self.server.request.call_args_list,
        )


class TestBulkExportEndToEnd(AsyncTestCase):
    """
    Test case for doing an entire bulk export loop, without mocking python code.

    Server responses are mocked, but that's it. This is more of a functional test case than a unit test case.
    """

    def setUp(self) -> None:
        super().setUp()

        self.root = store.Root("http://localhost:9999/fhir")
        self.client_id = "test-client-id"

        self.jwks_file = tempfile.NamedTemporaryFile()  # pylint: disable=consider-using-with
        jwk_token = jwk.JWK.generate(kty="EC", alg="ES384", curve="P-384", kid="a", key_ops=["sign", "verify"]).export(
            as_dict=True
        )
        jwks = {"keys": [jwk_token]}
        self.jwks_file.write(dumps(jwks).encode("utf8"))
        self.jwks_file.flush()
        self.jwks_path = self.jwks_file.name

    def set_up_requests(self, respx_mock):
        # /.well-known/smart-configuration
        respx_mock.get(
            f"{self.root.path}/.well-known/smart-configuration",
            headers={"Accept": "application/json"},
        ).respond(
            json={
                "capabilities": ["client-confidential-asymmetric"],
                "token_endpoint": f"{self.root.path}/token",
                "token_endpoint_auth_methods_supported": ["private_key_jwt"],
                "token_endpoint_auth_signing_alg_values_supported": ["ES384"],
            },
        )

        # /metadata (most of this is just to pass validation -- this endpoint is just for fhirclient to get a token url)
        # Note that we use the 'responses' module for this, because fhirclient uses the 'requests' module
        responses.get(
            f"{self.root.path}/metadata",
            json={
                "date": "1900-01-01",
                "fhirVersion": "4.0.1",
                "format": ["application/fhir+json"],
                "kind": "instance",
                "resourceType": "CapabilityStatement",
                "rest": [
                    {
                        "mode": "server",
                        "security": {
                            "extension": [
                                {
                                    "url": "http://fhir-registry.smarthealthit.org/StructureDefinition/oauth-uris",
                                    "extension": [
                                        # Notably only offer a token URL, just like the bulk-data-server has.
                                        # Some versions of fhirclient also expect an authorize URL, but we should
                                        # still work in cases where that isn't available.
                                        {"url": "token", "valueUri": f"{self.root.path}/token"}
                                    ],
                                }
                            ],
                        },
                    }
                ],
                "status": "active",
            },
        )

        # /token
        # Note that we use the 'responses' module for this, because fhirclient uses the 'requests' module
        responses.post(
            f"{self.root.path}/token",
            json={
                "access_token": "1234567890",
            },
        )

        # /$export
        respx_mock.get(
            f"{self.root.path}/$export",
            headers={
                "Accept": "application/fhir+json",
                "Authorization": "Bearer 1234567890",
                "Prefer": "respond-async",
            },
            params={
                "_type": "Patient",
            },
        ).respond(
            status_code=202,
            headers={"Content-Location": f"{self.root.path}/poll"},
        )

        # /poll
        respx_mock.get(
            f"{self.root.path}/poll",
            headers={
                "Accept": "application/json",
                "Authorization": "Bearer 1234567890",
            },
        ).respond(
            json={
                "output": [{"type": "Patient", "url": f"{self.root.path}/download/patient1"}],
            },
        )

        # /download/patient1
        respx_mock.get(
            f"{self.root.path}/download/patient1",
            headers={
                "Accept": "application/fhir+ndjson",
                "Authorization": "Bearer 1234567890",
            },
        ).respond(
            json={  # content doesn't really matter
                "id": "testPatient1",
                "resourceType": "Patient",
            },
        )

        # DELETE /poll
        respx_mock.delete(
            f"{self.root.path}/poll",
            headers={
                "Accept": "application/fhir+json",
                "Authorization": "Bearer 1234567890",
            },
        ).respond(
            status_code=202,
        )

    @responses.mock.activate(assert_all_requests_are_fired=True)
    async def test_successful_bulk_export(self):
        """Verify a happy path bulk export, from toe to tip"""
        with tempfile.TemporaryDirectory() as tmpdir:
            with respx.mock(assert_all_called=True) as respx_mock:
                self.set_up_requests(respx_mock)

                await cli.main(
                    [
                        self.root.path,
                        f"{tmpdir}/output",
                        f"{tmpdir}/phi",
                        "--skip-init-checks",
                        "--output-format=ndjson",
                        "--task=patient",
                        f"--smart-client-id={self.client_id}",
                        f"--smart-jwks={self.jwks_path}",
                    ]
                )

            self.assertEqual(
                {"id": "4342abf315cf6f243e11f4d460303e36c6c3663a25c91cc6b1a8002476c850dd", "resourceType": "Patient"},
                common.read_json(f"{tmpdir}/output/patient/patient.000.ndjson"),
            )
