"""Tests for FhirClient and similar"""

import argparse
import time
from unittest import mock

import ddt
import respx
from jwcrypto import jwk, jwt

from cumulus_etl import errors, fhir, store
from tests.utils import AsyncTestCase, make_response


@ddt.ddt
@mock.patch("cumulus_etl.fhir.fhir_auth.uuid.uuid4", new=lambda: "1234")
class TestFhirClient(AsyncTestCase):
    """
    Test case for FHIR client oauth2 / request support.

    i.e. tests for fhir_client.py
    """

    def setUp(self):
        super().setUp()

        # By default, set up a working server and auth. Tests can break things as needed.

        self.client_id = "my-client-id"
        self.jwk = jwk.JWK.generate(
            kty="RSA", alg="RS384", kid="a", key_ops=["sign", "verify"]
        ).export(as_dict=True)
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
                "exp": int(time.time()) + 299,  # aided by time-machine not changing time under us
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
        }
        self.respx_mock.get(
            f"{self.server_url}/.well-known/smart-configuration",
            headers={"Accept": "application/json"},
        ).respond(
            json=self.smart_configuration,
        )

        # empty capabilities (no vendor quirks) by default
        self.respx_mock.get(f"{self.server_url}/metadata").respond(json={})

        self.respx_mock.post(
            self.token_url,
            name="token",
        ).respond(json={"access_token": "1234"})

    async def test_required_arguments(self):
        """Verify that we require both a client ID and a JWK Set"""
        # Deny any actual requests during this test
        self.respx_mock.get(f"{self.server_url}/test").respond(status_code=401)

        # Simple helper to open and make a call on client.
        async def use_client(request=False, code=None, url=self.server_url, **kwargs):
            try:
                async with fhir.FhirClient(url, [], **kwargs) as client:
                    if request:
                        await client.request("GET", "test")
            except SystemExit as exc:
                if code is None:
                    raise
                self.assertEqual(code, exc.code)

        # No SMART args at all doesn't cause any problem, if we don't make calls
        await use_client()

        # No SMART args at all will raise though if we do make a call
        with self.assertRaises(errors.FhirAuthMissing):
            await use_client(request=True)

        # No base URL
        with self.assertRaises(errors.FhirUrlMissing):
            await use_client(request=True, url=None)

        # No JWKS
        await use_client(code=errors.SMART_CREDENTIALS_MISSING, smart_client_id="foo")

        # No client ID
        await use_client(code=errors.SMART_CREDENTIALS_MISSING, smart_jwks=self.jwks)

        # Works fine if both given
        await use_client(smart_client_id="foo", smart_jwks=self.jwks)

    async def test_auth_with_jwks(self):
        """Verify that we authorize correctly upon class initialization"""
        self.respx_mock.get(
            f"{self.server_url}/foo",
            headers={"Authorization": "Bearer 1234"},  # the same access token used in setUp()
        )

        async with fhir.FhirClient(
            self.server_url,
            ["Condition", "Patient"],
            smart_client_id=self.client_id,
            smart_jwks=self.jwks,
        ) as client:
            await client.request("GET", "foo")

        # Check that we asked for a token & we included all the right params
        self.assertEqual(1, self.respx_mock["token"].call_count)
        self.assertEqual(
            "&".join(
                [
                    "grant_type=client_credentials",
                    "scope=system%2FCondition.read+system%2FPatient.read",
                    "client_assertion_type=urn%3Aietf%3Aparams%3Aoauth%3Aclient-assertion-type%3Ajwt-bearer",
                    f"client_assertion={self.expected_jwt}",
                ]
            ),
            self.respx_mock["token"].calls.last.request.content.decode("utf8"),
        )

    async def test_auth_with_bearer_token(self):
        """Verify that we pass along the bearer token to the server"""
        self.respx_mock.get(
            f"{self.server_url}/foo",
            headers={"Authorization": "Bearer fob"},
        )

        async with fhir.FhirClient(
            self.server_url, ["Condition", "Patient"], bearer_token="fob"
        ) as server:
            await server.request("GET", "foo")

    async def test_auth_with_basic_auth(self):
        """Verify that we pass along the basic user/password to the server"""
        self.respx_mock.get(
            f"{self.server_url}/foo",
            headers={"Authorization": "Basic VXNlcjpwNHNzdzByZA=="},
        )

        async with fhir.FhirClient(
            self.server_url, [], basic_user="User", basic_password="p4ssw0rd"
        ) as server:
            await server.request("GET", "foo")

    async def test_get_with_new_header(self):
        """Verify that we can add new headers"""
        self.respx_mock.get(
            f"{self.server_url}/foo",
            headers={
                # just to confirm we don't replace default headers entirely
                "Accept": "application/fhir+json",
                "Test": "Value",
            },
        )

        async with fhir.FhirClient(
            self.server_url, [], smart_client_id=self.client_id, smart_jwks=self.jwks
        ) as server:
            # With new header and stream
            await server.request("GET", "foo", headers={"Test": "Value"}, stream=True)

    async def test_get_with_overriden_header(self):
        """Verify that we can overwrite default headers"""
        self.respx_mock.get(
            f"{self.server_url}/bar",
            headers={
                "Accept": "text/plain",  # yay! it's no longer the default fhir+json one
            },
        )

        async with fhir.FhirClient(
            self.server_url, [], smart_client_id=self.client_id, smart_jwks=self.jwks
        ) as server:
            # With overriding a header and default stream (False)
            await server.request("GET", "bar", headers={"Accept": "text/plain"})

    @ddt.data(
        {},  # no keys
        {"keys": [{"key_ops": ["sign"], "kid": "a"}]},  # no alg
        {"keys": [{"alg": "RS384", "kid": "a"}]},  # no key op
        {"keys": [{"alg": "RS384", "key_ops": ["sign"]}]},  # no kid
        {"keys": [{"alg": "RS384", "key_ops": ["verify"], "kid": "a"}]},  # bad key op
    )
    async def test_jwks_without_suitable_key(self, bad_jwks):
        with self.assertRaisesRegex(errors.FatalError, "No valid private key found"):
            async with fhir.FhirClient(
                self.server_url, [], smart_client_id=self.client_id, smart_jwks=bad_jwks
            ):
                pass

    @ddt.data(
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

        with self.assertRaisesRegex(SystemExit, str(errors.FHIR_AUTH_FAILED)):
            async with fhir.FhirClient(
                self.server_url, [], smart_client_id=self.client_id, smart_jwks=self.jwks
            ):
                pass

    @ddt.data(
        ({"json": {"error_description": "Ouch!"}}, "Ouch!"),
        (
            {"json": {"error_uri": "http://ouch.com/sadface"}},
            'visit "http://ouch.com/sadface" for more details',
        ),
        # If nothing comes back, we use the default httpx error message
        (
            {},
            (
                "Client error '400 Bad Request' for url 'https://auth.example.com/token'\n"
                "For more information check: https://developer.mozilla.org/en-US/docs/Web/HTTP/Status/400"
            ),
        ),
    )
    @ddt.unpack
    async def test_authorize_error(self, response_params, expected_error):
        """Verify that we translate oauth2 errors into fatal errors."""
        self.respx_mock["token"].respond(400, **response_params)

        with mock.patch("cumulus_etl.errors.fatal") as mock_fatal:
            async with fhir.FhirClient(
                self.server_url, [], smart_client_id=self.client_id, smart_jwks=self.jwks
            ):
                pass

        self.assertEqual(
            mock.call(
                f"Could not authenticate with the FHIR server: {expected_error}",
                errors.FHIR_AUTH_FAILED,
            ),
            mock_fatal.call_args,
        )

    def test_file_read_error(self):
        """Verify that if we can't read a provided file, we gracefully error out"""
        args = argparse.Namespace(
            fhir_url=None,
            smart_client_id=None,
            smart_jwks="does-not-exist.txt",
            basic_user=None,
            basic_passwd=None,
            bearer_token=None,
        )
        with self.assertRaises(SystemExit) as cm:
            fhir.create_fhir_client_for_cli(args, store.Root("/tmp"), [])
        self.assertEqual(errors.ARGS_INVALID, cm.exception.code)

    async def test_must_be_context_manager(self):
        """Verify that FHIRClient enforces its use as a context manager."""
        client = fhir.FhirClient(
            self.server_url, [], smart_client_id=self.client_id, smart_jwks=self.jwks
        )
        with self.assertRaisesRegex(RuntimeError, "FhirClient must be used as a context manager"):
            await client.request("GET", "foo")

    @ddt.data(True, False)  # confirm that we handle both stream and non-stream resets
    async def test_get_error_401(self, stream_mode):
        """Verify that an expired token is refreshed."""
        route = self.respx_mock.get(f"{self.server_url}/foo")
        route.side_effect = [make_response(status_code=401), make_response()]

        async with fhir.FhirClient(
            self.server_url, [], smart_client_id=self.client_id, smart_jwks=self.jwks
        ) as server:
            self.assertEqual(1, self.respx_mock["token"].call_count)

            # Check that we correctly tried to re-authenticate
            response = await server.request("GET", "foo", stream=stream_mode)
            self.assertEqual(200, response.status_code)

            self.assertEqual(2, self.respx_mock["token"].call_count)

    async def test_get_error_429(self):
        """Verify that 429 errors are passed through and not treated as exceptions."""
        self.respx_mock.get(f"{self.server_url}/retry-me").respond(429)
        self.respx_mock.get(f"{self.server_url}/nope").respond(430)

        async with fhir.FhirClient(
            self.server_url, [], smart_client_id=self.client_id, smart_jwks=self.jwks
        ) as server:
            # Confirm 429 passes
            response = await server.request("GET", "retry-me")
            self.assertEqual(429, response.status_code)

            # Sanity check that 430 does not
            with self.assertRaises(errors.FatalError):
                await server.request("GET", "nope")

    @ddt.data(
        # OperationOutcome
        {
            "json_payload": {
                "resourceType": "OperationOutcome",
                "issue": [{"diagnostics": "testmsg"}],
            }
        },
        # non-OperationOutcome json
        {"json_payload": {"issue": [{"diagnostics": "msg"}]}, "reason": "testmsg"},
        {"text": "testmsg"},  # just pure text content
        {"reason": "testmsg"},
    )
    async def test_get_error_other(self, response_args):
        """Verify that other http errors are FatalErrors."""
        self.respx_mock.get(
            f"{self.server_url}/foo",
        ).mock(
            return_value=make_response(status_code=500, **response_args),
        )

        async with fhir.FhirClient(
            self.server_url, [], smart_client_id=self.client_id, smart_jwks=self.jwks
        ) as server:
            with self.assertRaisesRegex(errors.FatalError, "testmsg"):
                await server.request("GET", "foo")

    @ddt.data(
        ({"DocumentReference", "Patient"}, {"Binary", "DocumentReference", "Patient"}),
        ({"Patient"}, {"Patient"}),
    )
    @ddt.unpack
    @mock.patch("cumulus_etl.fhir.fhir_client.FhirClient")
    def test_added_binary_scope(self, resources_in, expected_resources_out, mock_client):
        """Verify that we add a Binary scope if DocumentReference is requested"""
        args = argparse.Namespace(
            fhir_url=None,
            smart_client_id=None,
            smart_jwks=None,
            basic_user=None,
            basic_passwd=None,
            bearer_token=None,
        )
        fhir.create_fhir_client_for_cli(args, store.Root("/tmp"), resources_in)
        self.assertEqual(mock_client.call_args[0][1], expected_resources_out)


@ddt.ddt
@mock.patch("cumulus_etl.fhir.fhir_auth.uuid.uuid4", new=lambda: "1234")
class TestFhirClientEpicQuirks(AsyncTestCase):
    """Test case for FHIR client handling of Epic-specific vendor quirks."""

    def setUp(self):
        super().setUp()
        self.server_url = "http://localhost"

        self.respx_mock = respx.mock(assert_all_called=False)
        self.addCleanup(self.respx_mock.stop)
        self.respx_mock.start()

    def mock_as_server_type(self, server_type: str | None):
        response_json = {}
        if server_type == "epic":
            response_json = {"software": {"name": "Epic"}}
        elif server_type == "cerner":
            response_json = {"publisher": "Cerner"}

        self.respx_mock.get(f"{self.server_url}/metadata").respond(json=response_json)

    @ddt.data(
        ("epic", "present"),
        ("cerner", "missing"),
        (None, "missing"),
    )
    @ddt.unpack
    async def test_client_id_in_header(self, server_type, expected_text):
        # Mock with header
        self.respx_mock.get(
            f"{self.server_url}/file",
            headers={"Epic-Client-ID": "my-id"},
        ).respond(
            text="present",
        )
        # And without
        self.respx_mock.get(
            f"{self.server_url}/file",
        ).respond(
            text="missing",
        )

        self.mock_as_server_type(server_type)
        async with fhir.FhirClient(
            self.server_url, [], bearer_token="foo", smart_client_id="my-id"
        ) as server:
            response = await server.request("GET", "file")
            self.assertEqual(expected_text, response.text)
