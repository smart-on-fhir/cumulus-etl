"""Code for the various ways to authenticate against a FHIR server"""

import base64
import sys
import time
import urllib.parse
import uuid
from json import JSONDecodeError
from collections.abc import Iterable

import httpx
from jwcrypto import jwk, jwt

from cumulus_etl import errors


def urljoin(base: str, path: str) -> str:
    """Basically just urllib.parse.urljoin, but with some extra error checking"""
    path_is_absolute = bool(urllib.parse.urlparse(path).netloc)
    if path_is_absolute:
        return path

    if not base:
        print("You must provide a base FHIR server URL with --fhir-url", file=sys.stderr)
        raise SystemExit(errors.FHIR_URL_MISSING)
    return urllib.parse.urljoin(base, path)


class Auth:
    """Abstracted authentication for a FHIR server. By default, does nothing."""

    async def authorize(self, session: httpx.AsyncClient, reauthorize=False) -> None:
        """Authorize (or re-authorize) against the server"""
        del session

        if reauthorize:
            # Abort because we clearly need authentication tokens, but have not been given any parameters for them.
            print(
                "You must provide some authentication parameters (like --smart-client-id) to connect to a server.",
                file=sys.stderr,
            )
            raise SystemExit(errors.SMART_CREDENTIALS_MISSING)

    def sign_headers(self, headers: dict) -> dict:
        """Add signature token to request headers"""
        return headers


class JwksAuth(Auth):
    """Authentication with a JWK Set (typical backend service profile)"""

    def __init__(self, server_root: str, client_id: str, jwks: dict, resources: Iterable[str]):
        super().__init__()
        self._server_root = server_root
        self._client_id = client_id
        self._jwks = jwks
        self._resources = list(resources)
        self._token_endpoint = None
        self._access_token = None

    async def authorize(self, session: httpx.AsyncClient, reauthorize=False) -> None:
        """
        Authenticates against a SMART FHIR server using the Backend Services profile.

        See https://hl7.org/fhir/smart-app-launch/backend-services.html for details.
        """
        if self._token_endpoint is None:  # grab URL if we haven't before
            self._token_endpoint = await self._get_token_endpoint(session)

        auth_params = {
            "grant_type": "client_credentials",
            "scope": " ".join([f"system/{resource}.read" for resource in self._resources]),
            "client_assertion_type": "urn:ietf:params:oauth:client-assertion-type:jwt-bearer",
            "client_assertion": self._make_signed_jwt(),
        }

        try:
            response = await session.post(self._token_endpoint, data=auth_params)
            response.raise_for_status()
            self._access_token = response.json().get("access_token")
        except httpx.HTTPStatusError as exc:
            try:
                response_json = exc.response.json()
            except JSONDecodeError:
                response_json = {}
            message = response_json.get("error_description")  # standard oauth2 error field
            if not message and "error_uri" in response_json:
                # Another standard oauth2 error field, which Cerner usually gives back, and it does have helpful info
                message = f'visit "{response_json.get("error_uri")}" for more details'
            if not message:
                message = str(exc)

            errors.fatal(f"Could not authenticate with the FHIR server: {message}", errors.FHIR_AUTH_FAILED)

    def sign_headers(self, headers: dict) -> dict:
        """Add signature token to request headers"""
        headers["Authorization"] = f"Bearer {self._access_token}"
        return headers

    async def _get_token_endpoint(self, session: httpx.AsyncClient) -> str:
        """
        Returns the oauth2 token endpoint for a SMART FHIR server.

        See https://hl7.org/fhir/smart-app-launch/client-confidential-asymmetric.html for details.

        If the server does not support the client-confidential-asymmetric protocol, an exception will be raised.

        :returns: URL for the server's oauth2 token endpoint
        """
        response = await session.get(
            urljoin(self._server_root, ".well-known/smart-configuration"),
            headers={
                "Accept": "application/json",
            },
            timeout=300,  # five minutes
        )
        response.raise_for_status()

        # Validate that the server can talk the client-confidential-asymmetric protocol with us.
        # Some servers (like Cerner) don't advertise their support with the 'client-confidential-asymmetric'
        # capability keyword, so let's not bother checking for it. But we can confirm that the pieces are there.
        config = response.json()
        if "private_key_jwt" not in config.get("token_endpoint_auth_methods_supported", []) or not config.get(
            "token_endpoint"
        ):
            errors.fatal(
                f"Server {self._server_root} does not support the client-confidential-asymmetric protocol",
                errors.FHIR_AUTH_FAILED,
            )

        return config["token_endpoint"]

    def _make_signed_jwt(self) -> str:
        """
        Creates a signed JWT for use in the client-confidential-asymmetric protocol.

        See https://hl7.org/fhir/smart-app-launch/client-confidential-asymmetric.html for details.

        :returns: a signed JWT string, ready for authentication with the FHIR server
        """
        # Find a usable singing JWK from JWKS
        for key in self._jwks.get("keys", []):
            if key.get("alg") in ["ES384", "RS384"] and "sign" in key.get("key_ops", []) and key.get("kid"):
                break
        else:  # no valid private JWK found
            raise errors.FatalError("No private ES384 or RS384 key found in the provided JWKS file.")

        # Now generate a signed JWT based off the given JWK
        header = {
            "alg": key["alg"],
            "kid": key["kid"],
            "typ": "JWT",
        }
        claims = {
            "iss": self._client_id,
            "sub": self._client_id,
            "aud": self._token_endpoint,
            "exp": int(time.time()) + 299,  # expires inside five minutes
            "jti": str(uuid.uuid4()),
        }
        token = jwt.JWT(header=header, claims=claims)
        token.make_signed_token(key=jwk.JWK(**key))
        return token.serialize()


class BasicAuth(Auth):
    """Authentication with basic user/password"""

    def __init__(self, user: str, password: str):
        super().__init__()
        # Assume utf8 is acceptable -- we should in theory also run these through Unicode normalization, in case they
        # have interesting Unicode characters. But we can always add that in the future.
        combo_bytes = f"{user}:{password}".encode("utf8")
        self._basic_token = base64.standard_b64encode(combo_bytes).decode("ascii")

    async def authorize(self, session: httpx.AsyncClient, reauthorize=False) -> None:
        pass

    def sign_headers(self, headers: dict) -> dict:
        headers["Authorization"] = f"Basic {self._basic_token}"
        return headers


class BearerAuth(Auth):
    """Authentication with a static bearer token"""

    def __init__(self, bearer_token: str):
        super().__init__()
        self._bearer_token = bearer_token

    async def authorize(self, session: httpx.AsyncClient, reauthorize=False) -> None:
        pass

    def sign_headers(self, headers: dict) -> dict:
        headers["Authorization"] = f"Bearer {self._bearer_token}"
        return headers


def create_auth(
    server_root: str | None,
    resources: Iterable[str],
    basic_user: str | None,
    basic_password: str | None,
    bearer_token: str | None,
    smart_client_id: str | None,
    smart_jwks: dict | None,
) -> Auth:
    """Determine which auth method to use based on user provided arguments"""
    valid_smart_jwks = smart_jwks is not None  # compared to a falsy (but technically usable) empty dict for example

    # Check if the user tried to specify multiple types of auth, and help them out
    has_basic_args = bool(basic_user or basic_password)
    has_bearer_args = bool(bearer_token)
    has_smart_args = bool(valid_smart_jwks)
    total_auth_types = has_basic_args + has_bearer_args + has_smart_args
    if total_auth_types > 1:
        print(
            "Multiple authentication methods have been specified. Double check your arguments to Cumulus ETL.",
            file=sys.stderr,
        )
        raise SystemExit(errors.ARGS_CONFLICT)

    if basic_user and basic_password:
        return BasicAuth(basic_user, basic_password)
    elif basic_user or basic_password:
        print(
            "You must provide both --basic-user and --basic-password to connect to a Basic auth server.",
            file=sys.stderr,
        )
        raise SystemExit(errors.BASIC_CREDENTIALS_MISSING)

    if bearer_token:
        return BearerAuth(bearer_token)

    if smart_client_id and valid_smart_jwks:
        return JwksAuth(server_root, smart_client_id, smart_jwks, resources)
    elif smart_client_id or valid_smart_jwks:
        print(
            "You must provide both --smart-client-id and --smart-jwks to connect to a SMART FHIR server.",
            file=sys.stderr,
        )
        raise SystemExit(errors.SMART_CREDENTIALS_MISSING)

    return Auth()
