"""Tests for FhirClient and similar"""

import argparse
import time
from unittest import mock

import ddt
import freezegun
import httpx
import respx
from jwcrypto import jwk, jwt

from cumulus import errors, store
from cumulus.fhir_client import FatalError, FhirClient, create_fhir_client_for_cli
from tests.utils import AsyncTestCase, make_response


@ddt.ddt
@freezegun.freeze_time("Sep 15th, 2021 1:23:45")
@mock.patch("cumulus.fhir_client.uuid.uuid4", new=lambda: "1234")
class TestFhirClient(AsyncTestCase):
    """
    Test case for FHIR client oauth2 / request support.

    i.e. tests for fhir_client.py
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
        await use_client(code=errors.SMART_CREDENTIALS_MISSING, smart_client_id="foo")

        # No client ID
        await use_client(code=errors.SMART_CREDENTIALS_MISSING, smart_jwks=self.jwks)

        # Works fine if both given
        await use_client(smart_client_id="foo", smart_jwks=self.jwks)

    async def test_auth_initial_authorize(self):
        """Verify that we authorize correctly upon class initialization"""
        async with FhirClient(
            self.server_url, ["Condition", "Patient"], smart_client_id=self.client_id, smart_jwks=self.jwks
        ):
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

    async def test_auth_with_basic_auth(self):
        """Verify that we pass along the basic user/password to the server"""
        self.respx_mock.get(
            f"{self.server_url}/foo",
            headers={"Authorization": "Basic VXNlcjpwNHNzdzByZA=="},
        )

        async with FhirClient(self.server_url, [], basic_user="User", basic_password="p4ssw0rd") as server:
            await server.request("GET", "foo")

    async def test_get_with_new_header(self):
        """Verify that we issue a GET correctly for the happy path"""
        # This is mostly confirming that we call mocks correctly, but that's important since we're mocking out all
        # of fhirclient. Since we do that, we need to confirm we're driving it well.

        async with FhirClient(self.server_url, [], smart_client_id=self.client_id, smart_jwks=self.jwks) as server:
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
        async with FhirClient(self.server_url, [], smart_client_id=self.client_id, smart_jwks=self.jwks) as server:
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
            async with FhirClient(self.server_url, [], smart_client_id=self.client_id, smart_jwks=bad_jwks):
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
            async with FhirClient(self.server_url, [], smart_client_id=self.client_id, smart_jwks=self.jwks):
                pass

    async def test_authorize_error_with_response(self):
        """Verify that we translate authorize http response errors into FatalErrors."""
        error = Exception()
        error.response = mock.MagicMock()
        error.response.json.return_value = {"error_description": "Ouch!"}
        self.mock_client.authorize.side_effect = error
        with self.assertRaisesRegex(FatalError, "Could not authenticate with the FHIR server: Ouch!"):
            async with FhirClient(self.server_url, [], smart_client_id=self.client_id, smart_jwks=self.jwks):
                pass

    async def test_authorize_error_without_response(self):
        """Verify that we translate authorize non-response errors into FatalErrors."""
        self.mock_client.authorize.side_effect = Exception("no memory")
        with self.assertRaisesRegex(FatalError, "Could not authenticate with the FHIR server: no memory"):
            async with FhirClient(self.server_url, [], smart_client_id=self.client_id, smart_jwks=self.jwks):
                pass

    async def test_get_error_401(self):
        """Verify that an expired token is refreshed."""
        async with FhirClient(self.server_url, [], smart_client_id=self.client_id, smart_jwks=self.jwks) as server:
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
        async with FhirClient(self.server_url, [], smart_client_id=self.client_id, smart_jwks=self.jwks) as server:
            # Confirm 429 passes
            with self.mock_session(server, status_code=429):
                response = await server.request("GET", "foo")
                self.assertEqual(429, response.status_code)

            # Sanity check that 430 does not
            with self.mock_session(server, status_code=430):
                with self.assertRaises(FatalError):
                    await server.request("GET", "foo")

    @ddt.data(
        {
            "json_payload": {"resourceType": "OperationOutcome", "issue": [{"diagnostics": "testmsg"}]}
        },  # OperationOutcome
        {"json_payload": {"issue": [{"diagnostics": "msg"}]}, "reason": "testmsg"},  # non-OperationOutcome json
        {"text": "testmsg"},  # just pure text content
        {"reason": "testmsg"},
    )
    async def test_get_error_other(self, response_args):
        """Verify that other http errors are FatalErrors."""
        async with FhirClient(self.server_url, [], smart_client_id=self.client_id, smart_jwks=self.jwks) as server:
            with self.mock_session(server, status_code=500, **response_args):
                with self.assertRaisesRegex(FatalError, "testmsg"):
                    await server.request("GET", "foo")

    @ddt.data(
        ({"DocumentReference", "Patient"}, {"Binary", "DocumentReference", "Patient"}),
        ({"Patient"}, {"Patient"}),
    )
    @ddt.unpack
    @mock.patch("cumulus.fhir_client.FhirClient")
    def test_added_binary_scope(self, resources_in, expected_resources_out, mock_client):
        """Verify that we add a Binary scope if DocumentReference is requested"""
        args = argparse.Namespace(
            fhir_url=None, smart_client_id=None, smart_jwks=None, basic_user=None, basic_passwd=None, bearer_token=None
        )
        create_fhir_client_for_cli(args, store.Root("/tmp"), resources_in)
        self.assertEqual(mock_client.call_args[0][1], expected_resources_out)
