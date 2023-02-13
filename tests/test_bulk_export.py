"""Tests for bulk export support"""

import contextlib
import io
import tempfile
import time
import unittest
from json import dumps
from unittest import mock

import ddt
import freezegun
import httpx
import responses
import respx
from jwcrypto import jwk, jwt

from cumulus import common, errors, etl, loaders, store
from cumulus.fhir_client import FatalError, FhirClient
from cumulus.loaders.fhir.bulk_export import BulkExporter


def make_response(status_code=200, json=None, text=None, reason=None, headers=None, stream=False):
    """Makes a fake response for ease of testing"""
    headers = dict(headers or {})
    headers.setdefault("Content-Type", "application/json" if json else "text/plain; charset=utf-8")
    json = dumps(json) if json else None
    body = (json or text or "").encode("utf8")
    stream_contents = None
    if stream:
        stream_contents = httpx.ByteStream(body)
        body = None
    return respx.MockResponse(
        status_code=status_code,
        content=body,
        stream=stream_contents,
        extensions=reason and {"reason_phrase": reason.encode("utf8")},
        headers=headers or {},
        request=httpx.Request("GET", "fake_request_url"),
    )


class TestBulkEtl(unittest.IsolatedAsyncioTestCase):
    """
    Test case for bulk export support in the etl pipeline and ndjson loader.

    i.e. tests for etl.py & fhir_ndjson.py

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
        exporter_patcher = mock.patch("cumulus.loaders.fhir.fhir_ndjson.BulkExporter", spec=BulkExporter)
        self.addCleanup(exporter_patcher.stop)
        self.mock_exporter_class = exporter_patcher.start()
        self.mock_exporter = mock.AsyncMock()
        self.mock_exporter_class.return_value = self.mock_exporter

    @mock.patch("cumulus.etl.fhir_client.FhirClient")
    @mock.patch("cumulus.etl.loaders.FhirNdjsonLoader")
    async def test_etl_passes_args(self, mock_loader, mock_client):
        """Verify that we are passed the client ID and JWKS from the command line"""
        mock_loader.side_effect = ValueError  # just to stop the etl pipeline once we get this far

        with tempfile.NamedTemporaryFile(buffering=0) as bt_file:
            bt_file.write(b"bt")

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
                        f"--bearer-token={bt_file.name}",
                        "--since=2018",
                        "--until=2020",
                    ]
                )

        self.assertEqual(1, mock_client.call_count)
        self.assertEqual("x", mock_client.call_args[1]["client_id"])
        self.assertEqual({"fake": "jwks"}, mock_client.call_args[1]["jwks"])
        self.assertEqual("bt", mock_client.call_args[1]["bearer_token"])
        self.assertEqual(1, mock_loader.call_count)
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
        self.assertEqual("/direct-string", mock_client.call_args[1]["client_id"])

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
            self.assertEqual("inside-file", mock_client.call_args[1]["client_id"])

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


@ddt.ddt
@freezegun.freeze_time("Sep 15th, 2021 1:23:45")
@mock.patch("cumulus.fhir_client.uuid.uuid4", new=lambda: "1234")
class TestBulkServer(unittest.IsolatedAsyncioTestCase):
    """
    Test case for bulk export server oauth2 / request support.

    i.e. tests for backend_service.py
    """

    def setUp(self):
        super().setUp()

        # By default, set up a working server and auth. Tests can break things as needed.

        self.client_id = "my-client-id"
        self.jwk = jwk.JWK.generate(kty="RSA", alg="RS384", kid="a", key_ops=["sign", "verify"]).export(as_dict=True)
        self.jwks = {"keys": [self.jwk]}
        self.server_url = "https://example.com/fhir"
        self.token_url = "https://auth.example.com/token"

        # Generate expected JWT
        token = jwt.JWT(
            header={
                "alg": "RS384",
                "kid": "a",
                "typ": "JWT",
            },
            claims={
                "iss": self.client_id,
                "sub": self.client_id,
                "aud": self.token_url,
                "exp": int(time.time()) + 299,  # aided by freezegun not changing time under us
                "jti": "1234",
            },
        )
        token.make_signed_token(key=jwk.JWK(**self.jwk))
        self.expected_jwt = token.serialize()

        # Initialize responses mock
        self.respx_mock = respx.mock(assert_all_called=False)
        self.addCleanup(self.respx_mock.stop)
        self.respx_mock.start()

        # We ask for smart-configuration to discover the token endpoint
        self.smart_configuration = {
            "capabilities": ["client-confidential-asymmetric"],
            "token_endpoint": self.token_url,
            "token_endpoint_auth_methods_supported": ["private_key_jwt"],
            "token_endpoint_auth_signing_alg_values_supported": ["RS384"],
        }
        self.respx_mock.get(
            f"{self.server_url}/.well-known/smart-configuration",
            headers={"Accept": "application/json"},
        ).respond(
            json=self.smart_configuration,
        )

        # Set up mocks for fhirclient (we don't need to test its oauth code by mocking server responses there)
        self.mock_client = mock.MagicMock()  # FHIRClient instance
        self.mock_server = self.mock_client.server  # FHIRServer instance
        client_patcher = mock.patch("cumulus.fhir_client.fhirclient.client.FHIRClient")
        self.addCleanup(client_patcher.stop)
        self.mock_client_class = client_patcher.start()  # FHIRClient class
        self.mock_client_class.return_value = self.mock_client

    @staticmethod
    def mock_session(server, *args, **kwargs):
        session = mock.AsyncMock(spec=httpx.AsyncClient)
        session.send.return_value = make_response(*args, **kwargs)
        return mock.patch.object(server, "_session", session)

    async def test_required_arguments(self):
        """Verify that we require both a client ID and a JWK Set"""
        # Deny any actual requests during this test
        self.respx_mock.get(f"{self.server_url}/test").respond(status_code=401)

        # Simple helper to open and make a call on client.
        async def use_client(request=False, code=None, url=self.server_url, **kwargs):
            try:
                async with FhirClient(url, [], **kwargs) as client:
                    if request:
                        await client.request("GET", "test")
            except SystemExit as exc:
                if code is None:
                    raise
                self.assertEqual(code, exc.code)

        # No SMART args at all doesn't cause any problem, if we don't make calls
        await use_client()

        # No SMART args at all will raise though if we do make a call
        await use_client(code=errors.SMART_CREDENTIALS_MISSING, request=True)

        # No client ID
        await use_client(code=errors.FHIR_URL_MISSING, request=True, url=None)

        # No JWKS
        await use_client(code=errors.SMART_CREDENTIALS_MISSING, client_id="foo")

        # No client ID
        await use_client(code=errors.SMART_CREDENTIALS_MISSING, jwks=self.jwks)

        # Works fine if both given
        await use_client(client_id="foo", jwks=self.jwks)

    async def test_auth_initial_authorize(self):
        """Verify that we authorize correctly upon class initialization"""
        async with FhirClient(self.server_url, ["Condition", "Patient"], client_id=self.client_id, jwks=self.jwks):
            pass

        # Check initialization of FHIRClient
        self.assertListEqual(
            [
                mock.call(
                    settings={
                        "api_base": f"{self.server_url}/",
                        "app_id": self.client_id,
                        "jwt_token": self.expected_jwt,
                        "scope": "system/Condition.read system/Patient.read",
                    }
                )
            ],
            self.mock_client_class.call_args_list,
        )

        # Check authorization calls to FHIRClient
        self.assertFalse(self.mock_client.wants_patient)  # otherwise fhirclient adds scopes
        self.assertListEqual([mock.call()], self.mock_client.prepare.call_args_list)
        self.assertListEqual([mock.call()], self.mock_client.authorize.call_args_list)

    async def test_auth_with_bearer_token(self):
        """Verify that we pass along the bearer token to the server"""
        self.respx_mock.get(
            f"{self.server_url}/foo",
            headers={"Authorization": "Bearer fob"},
        )

        async with FhirClient(self.server_url, ["Condition", "Patient"], bearer_token="fob") as server:
            await server.request("GET", "foo")

    async def test_get_with_new_header(self):
        """Verify that we issue a GET correctly for the happy path"""
        # This is mostly confirming that we call mocks correctly, but that's important since we're mocking out all
        # of fhirclient. Since we do that, we need to confirm we're driving it well.

        async with FhirClient(self.server_url, [], client_id=self.client_id, jwks=self.jwks) as server:
            with self.mock_session(server) as mock_session:
                # With new header and stream
                await server.request("GET", "foo", headers={"Test": "Value"}, stream=True)

        self.assertEqual(
            [
                mock.call(
                    {
                        "Accept": "application/fhir+json",
                        "Accept-Charset": "UTF-8",
                        "Test": "Value",
                    }
                )
            ],
            self.mock_server.auth.signed_headers.call_args_list,
        )
        self.assertEqual(
            [
                mock.call(
                    "GET",
                    f"{self.server_url}/foo",
                    headers=self.mock_server.auth.signed_headers.return_value,
                )
            ],
            mock_session.build_request.call_args_list,
        )

    async def test_get_with_overriden_header(self):
        """Verify that we issue a GET correctly for the happy path"""
        async with FhirClient(self.server_url, [], client_id=self.client_id, jwks=self.jwks) as server:
            with self.mock_session(server) as mock_session:
                # With overriding a header and default stream (False)
                await server.request("GET", "bar", headers={"Accept": "text/plain"})

        self.assertEqual(
            [
                mock.call(
                    {
                        "Accept": "text/plain",
                        "Accept-Charset": "UTF-8",
                    }
                )
            ],
            self.mock_server.auth.signed_headers.call_args_list,
        )
        self.assertEqual(
            [
                mock.call(
                    "GET",
                    f"{self.server_url}/bar",
                    headers=self.mock_server.auth.signed_headers.return_value,
                )
            ],
            mock_session.build_request.call_args_list,
        )

    @ddt.data(
        {},  # no keys
        {"keys": [{"alg": "RS384"}]},  # no key op
        {"keys": [{"alg": "RS384", "key_ops": ["verify"], "kid": "a"}]},  # bad key op
        {"keys": [{"alg": "RS128", "key_ops": ["sign"]}], "kid": "a"},  # bad algo
        {"keys": [{"alg": "RS384", "key_ops": ["sign"]}]},  # no kid
    )
    async def test_jwks_without_suitable_key(self, bad_jwks):
        with self.assertRaisesRegex(FatalError, "No private ES384 or RS384 key found"):
            async with FhirClient(self.server_url, [], client_id=self.client_id, jwks=bad_jwks):
                pass

    @ddt.data(
        {"token_endpoint_auth_methods_supported": None},
        {"token_endpoint_auth_methods_supported": ["nope"]},
        {"token_endpoint_auth_signing_alg_values_supported": None},
        {"token_endpoint_auth_signing_alg_values_supported": ["nope"]},
        {"token_endpoint": None},
        {"token_endpoint": ""},
    )
    async def test_bad_smart_config(self, bad_config_override):
        """Verify that we require fully correct smart configurations."""
        for entry, value in bad_config_override.items():
            if value is None:
                del self.smart_configuration[entry]
            else:
                self.smart_configuration[entry] = value

        self.respx_mock.reset()
        self.respx_mock.get(
            f"{self.server_url}/.well-known/smart-configuration",
            headers={"Accept": "application/json"},
        ).respond(
            json=self.smart_configuration,
        )

        with self.assertRaisesRegex(FatalError, "does not support the client-confidential-asymmetric protocol"):
            async with FhirClient(self.server_url, [], client_id=self.client_id, jwks=self.jwks):
                pass

    async def test_authorize_error_with_response(self):
        """Verify that we translate authorize http response errors into FatalErrors."""
        error = Exception()
        error.response = mock.MagicMock()
        error.response.json.return_value = {"error_description": "Ouch!"}
        self.mock_client.authorize.side_effect = error
        with self.assertRaisesRegex(FatalError, "Could not authenticate with the FHIR server: Ouch!"):
            async with FhirClient(self.server_url, [], client_id=self.client_id, jwks=self.jwks):
                pass

    async def test_authorize_error_without_response(self):
        """Verify that we translate authorize non-response errors into FatalErrors."""
        self.mock_client.authorize.side_effect = Exception("no memory")
        with self.assertRaisesRegex(FatalError, "Could not authenticate with the FHIR server: no memory"):
            async with FhirClient(self.server_url, [], client_id=self.client_id, jwks=self.jwks):
                pass

    async def test_get_error_401(self):
        """Verify that an expired token is refreshed."""
        async with FhirClient(self.server_url, [], client_id=self.client_id, jwks=self.jwks) as server:
            # Check that we correctly tried to re-authenticate
            with self.mock_session(server) as mock_session:
                mock_session.send.side_effect = [make_response(status_code=401), make_response()]
                self.mock_server.reauthorize.return_value = None  # fhirclient gives None if there is no refresh token
                response = await server.request("GET", "foo")
                self.assertEqual(200, response.status_code)

        self.assertEqual(1, self.mock_server.reauthorize.call_count)
        self.assertEqual(2, self.mock_client_class.call_count)
        self.assertEqual(2, self.mock_client.prepare.call_count)
        self.assertEqual(2, self.mock_client.authorize.call_count)

    async def test_get_error_429(self):
        """Verify that 429 errors are passed through and not treated as exceptions."""
        async with FhirClient(self.server_url, [], client_id=self.client_id, jwks=self.jwks) as server:
            # Confirm 429 passes
            with self.mock_session(server, status_code=429):
                response = await server.request("GET", "foo")
                self.assertEqual(429, response.status_code)

            # Sanity check that 430 does not
            with self.mock_session(server, status_code=430):
                with self.assertRaises(FatalError):
                    await server.request("GET", "foo")

    @ddt.data(
        {"json": {"resourceType": "OperationOutcome", "issue": [{"diagnostics": "testmsg"}]}},  # OperationOutcome
        {"json": {"issue": [{"diagnostics": "msg"}]}, "reason": "testmsg"},  # non-OperationOutcome json
        {"text": "testmsg"},  # just pure text content
        {"reason": "testmsg"},
    )
    async def test_get_error_other(self, response_args):
        """Verify that other http errors are FatalErrors."""
        async with FhirClient(self.server_url, [], client_id=self.client_id, jwks=self.jwks) as server:
            with self.mock_session(server, status_code=500, **response_args):
                with self.assertRaisesRegex(FatalError, "testmsg"):
                    await server.request("GET", "foo")


@ddt.ddt
@freezegun.freeze_time("Sep 15th, 2021 1:23:45")
class TestBulkExporter(unittest.IsolatedAsyncioTestCase):
    """
    Test case for bulk export logic.

    i.e. tests for bulk_export.py
    """

    def setUp(self):
        super().setUp()
        self.tmpdir = tempfile.TemporaryDirectory()  # pylint: disable=consider-using-with
        self.server = mock.AsyncMock()

    def make_exporter(self, **kwargs) -> BulkExporter:
        return BulkExporter(self.server, ["Condition", "Patient"], self.tmpdir.name, **kwargs)

    async def export(self, **kwargs) -> BulkExporter:
        exporter = self.make_exporter(**kwargs)
        await exporter.export()
        return exporter

    async def test_happy_path(self):
        """Verify an end-to-end bulk export with no problems and no waiting works as expected"""
        self.server.request.side_effect = [
            make_response(status_code=202, headers={"Content-Location": "https://example.com/poll"}),  # kickoff
            make_response(
                json={
                    "output": [
                        {"type": "Condition", "url": "https://example.com/con1"},
                        {"type": "Condition", "url": "https://example.com/con2"},
                        {"type": "Patient", "url": "https://example.com/pat1"},
                    ]
                }
            ),  # status
            make_response(json={"type": "Condition1"}, stream=True),  # download
            make_response(json={"type": "Condition2"}, stream=True),  # download
            make_response(json={"type": "Patient1"}, stream=True),  # download
            make_response(status_code=202),  # delete request
        ]

        await self.export()

        self.assertListEqual(
            [
                mock.call(
                    "GET",
                    "$export?_type=Condition%2CPatient",
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
                    "$export?_type=Condition%2CPatient&_since=2000-01-01T00%3A00%3A00%2B00.00&_until=2010",
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
                json={
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
            make_response(json={"type": "OperationOutcome", "issue": [{"severity": "error", "diagnostics": "err1"}]}),
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
                    "$export?_type=Condition%2CPatient",
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
                json={
                    "error": [
                        {"type": "OperationOutcome", "url": "https://example.com/warning1"},
                    ],
                }
            ),  # status
            # warning
            make_response(
                json={"type": "OperationOutcome", "issue": [{"severity": "warning", "diagnostics": "warning1"}]}
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

    @mock.patch("cumulus.loaders.fhir.bulk_export.asyncio.sleep")
    async def test_delay(self, mock_sleep):
        """Verify that we wait the amount of time the server asks us to"""
        self.server.request.side_effect = [
            # Kicking off bulk export
            make_response(status_code=429, headers={"Retry-After": "3600"}),  # one hour
            make_response(status_code=202, headers={"Content-Location": "https://example.com/poll"}),  # kickoff done
            # Checking status of bulk export
            make_response(status_code=429),  # default of one minute
            make_response(status_code=202, headers={"Retry-After": "18000"}),  # five hours
            make_response(status_code=429, headers={"Retry-After": "64800"}),  # 18 hours (putting us over a day)
        ]

        exporter = self.make_exporter()
        with self.assertRaisesRegex(FatalError, "Timed out waiting"):
            await exporter.export()

        # 86460 == 24 hours + one minute
        self.assertEqual(86460, exporter._total_wait_time)  # pylint: disable=protected-access

        self.assertListEqual(
            [
                mock.call(3600),
                mock.call(60),
                mock.call(18000),
                mock.call(64800),
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
                    "$export?_type=Condition%2CPatient",
                    headers={"Prefer": "respond-async"},
                ),
                mock.call("GET", "https://example.com/poll", headers={"Accept": "application/json"}),
                mock.call("DELETE", "https://example.com/poll", headers=None),
            ],
            self.server.request.call_args_list,
        )


@mock.patch("cumulus.deid.codebook.secrets.token_hex", new=lambda x: "1234")  # just to not waste entropy
class TestBulkExportEndToEnd(unittest.IsolatedAsyncioTestCase):
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

    @mock.patch("cumulus.deid.codebook.secrets.token_hex", new=lambda x: "1234")
    @responses.mock.activate(assert_all_requests_are_fired=True)
    async def test_successful_bulk_export(self):
        """Verify a happy path bulk export, from toe to tip"""
        with tempfile.TemporaryDirectory() as tmpdir:
            with respx.mock(assert_all_called=True) as respx_mock:
                self.set_up_requests(respx_mock)

                await etl.main(
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
