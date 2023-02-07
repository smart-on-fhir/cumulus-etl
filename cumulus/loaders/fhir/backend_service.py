"""Support for SMART App Launch Backend Services"""

import abc
import re
import sys
import time
import urllib.parse
import uuid
from json import JSONDecodeError
from typing import List, Optional

import httpx
from fhirclient.client import FHIRClient
from jwcrypto import jwk, jwt

from cumulus import errors


class FatalError(Exception):
    """An unrecoverable error"""


class Auth(abc.ABC):
    """Abstracted authentication for a FHIR server"""

    @abc.abstractmethod
    async def authorize(self, session: httpx.AsyncClient) -> None:
        """Authorize (or re-authorize) against the server"""

    @abc.abstractmethod
    def sign_headers(self, headers: Optional[dict]) -> dict:
        """Add signature token to request headers"""


class JwksAuth(Auth):
    """Authentication with a JWK Set (typical backend service profile)"""

    def __init__(self, server_root: str, client_id: str, jwks: dict, resources: List[str]):
        super().__init__()
        self._server_root = server_root
        self._client_id = client_id
        self._jwks = jwks
        self._resources = list(resources)
        self._server = None
        self._token_endpoint = None

    async def authorize(self, session: httpx.AsyncClient) -> None:
        """
        Authenticates against a SMART FHIR server using the Backend Services profile.

        See https://hl7.org/fhir/smart-app-launch/backend-services.html for details.
        """
        # Have we authorized before?
        if self._token_endpoint is None:
            self._token_endpoint = await self._get_token_endpoint(session)
        if self._server is not None and self._server.reauthorize():
            return
        # Else we must not have been issued a refresh token, let's just authorize from scratch below

        signed_jwt = self._make_signed_jwt()
        scope = " ".join([f"system/{resource}.read" for resource in self._resources])
        client = FHIRClient(
            settings={
                "api_base": self._server_root,
                "app_id": self._client_id,
                "jwt_token": signed_jwt,
                "scope": scope,
            }
        )
        client.wants_patient = False
        client.prepare()

        try:
            client.authorize()
        except Exception as exc:  # pylint: disable=broad-except
            # This handles both the normal HTTPError and the custom errors that fhirclient uses
            message = None
            if hasattr(exc, "response") and exc.response:
                response_json = exc.response.json()
                message = response_json.get("error_description")  # oauth2 error field
            if not message:
                message = str(exc)

            raise FatalError(f"Could not authenticate with the FHIR server: {message}") from exc

        self._server = client.server

    def sign_headers(self, headers: Optional[dict]) -> dict:
        """Add signature token to request headers"""
        return self._server.auth.signed_headers(headers)

    async def _get_token_endpoint(self, session: httpx.AsyncClient) -> str:
        """
        Returns the oauth2 token endpoint for a SMART FHIR server.

        See https://hl7.org/fhir/smart-app-launch/client-confidential-asymmetric.html for details.

        If the server does not support the client-confidential-asymmetric protocol, an exception will be raised.

        :returns: URL for the server's oauth2 token endpoint
        """
        response = await session.get(
            urllib.parse.urljoin(self._server_root, ".well-known/smart-configuration"),
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
        if (
            "private_key_jwt" not in config.get("token_endpoint_auth_methods_supported", [])
            or not {"ES384", "RS384"} & set(config.get("token_endpoint_auth_signing_alg_values_supported", []))
            or not config.get("token_endpoint")
        ):
            raise FatalError(f"Server {self._server_root} does not support the client-confidential-asymmetric protocol")

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
            raise FatalError("No private ES384 or RS384 key found in the provided JWKS file.")

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


class BearerAuth(Auth):
    """Authentication with a static bearer token"""

    def __init__(self, bearer_token: str):
        super().__init__()
        self._bearer_token = bearer_token

    async def authorize(self, session: httpx.AsyncClient) -> None:
        pass

    def sign_headers(self, headers: Optional[dict]) -> dict:
        headers = headers or {}
        headers["Authorization"] = f"Bearer {self._bearer_token}"
        return headers


class BackendServiceServer:
    """
    Manages authentication and requests for a server that supports the Backend Service SMART profile.

    Use this as a context manager (like you would an httpx.AsyncClient instance).

    See https://hl7.org/fhir/smart-app-launch/backend-services.html for details.
    """

    def __init__(
        self, url: str, resources: List[str], client_id: str = None, jwks: dict = None, bearer_token: str = None
    ):
        """
        Initialize and authorize a BackendServiceServer context manager.

        :param url: base URL of the SMART FHIR server
        :param resources: a list of FHIR resource names to tightly scope our own permissions
        :param client_id: the ID assigned by the FHIR server when registering a new backend service app
        :param jwks: content of a JWK Set file, containing the private key for the registered public key
        :param bearer_token: a bearer token, containing the secret key to sign https requests (instead of JWKS)
        """
        self._base_url = url  # all requests are relative to this URL
        if not self._base_url.endswith("/"):
            self._base_url += "/"
        # The base URL may not be the server root (like it may be a Group export URL). Let's find the root.
        self._server_root = self._base_url
        self._server_root = re.sub(r"/Patient/$", "/", self._server_root)
        self._server_root = re.sub(r"/Group/[^/]+/$", "/", self._server_root)
        self._auth = self._make_auth(resources, client_id, jwks, bearer_token)
        self._session: Optional[httpx.AsyncClient] = None

    async def __aenter__(self):
        # Limit the number of connections open at once, because EHRs tend to be very busy.
        limits = httpx.Limits(max_connections=5)
        self._session = httpx.AsyncClient(limits=limits)
        await self._auth.authorize(self._session)
        return self

    async def __aexit__(self, exc_type, exc_value, traceback):
        if self._session:
            await self._session.aclose()

    def _make_auth(self, resources: List[str], client_id: str, jwks: dict, bearer_token: str) -> Auth:
        """Determine which auth method to use based on user provided arguments"""

        if bearer_token and (client_id or jwks):
            print("--bearer-token cannot be used with --smart-client-id or --smart-jwks", file=sys.stderr)
            raise SystemExit(errors.ARGS_CONFLICT)

        if bearer_token:
            return BearerAuth(bearer_token)

        # Confirm that all required SMART arguments were provided
        error_list = []
        if not client_id:
            error_list.append("You must provide a client ID with --smart-client-id to connect to a SMART FHIR server.")
        if jwks is None:
            error_list.append("You must provide a JWKS file with --smart-jwks to connect to a SMART FHIR server.")
        if error_list:
            print("\n".join(error_list), file=sys.stderr)
            raise SystemExit(errors.SMART_CREDENTIALS_MISSING)

        return JwksAuth(self._server_root, client_id, jwks, resources)

    async def request(self, method: str, path: str, headers: dict = None, stream: bool = False) -> httpx.Response:
        """
        Issues an HTTP request.

        This is a lightly modified version of FHIRServer._get(), but additionally supports streaming and
        reauthorization.

        Will raise a FatalError for an HTTP error, except for 429 which gets returned like a success code.

        :param method: HTTP method to issue
        :param path: relative path from the server root to request
        :param headers: optional header dictionary
        :param stream: whether to stream content in or load it all into memory at once
        :returns: The response object
        """
        url = urllib.parse.urljoin(self._base_url, path)

        final_headers = {
            "Accept": "application/fhir+json",
            "Accept-Charset": "UTF-8",
        }
        # merge in user headers with defaults
        final_headers.update(headers or {})

        response = await self._request_with_signed_headers(method, url, final_headers, stream=stream)

        # Check if our access token expired and thus needs to be refreshed
        if response.status_code == 401:
            await self._auth.authorize(self._session)
            if stream:
                await response.aclose()
            response = await self._request_with_signed_headers(method, url, final_headers, stream=stream)

        try:
            response.raise_for_status()
        except httpx.HTTPStatusError as exc:
            if exc.response.status_code == 429:
                # 429 is a special kind of error -- it's not fatal, just a request to wait a bit. So let it pass.
                return exc.response

            if stream:
                await response.aclose()

            # All other 4xx or 5xx codes are treated as fatal errors
            message = None
            try:
                json_response = exc.response.json()
                if json_response.get("resourceType") == "OperationOutcome":
                    issue = json_response["issue"][0]  # just grab first issue
                    message = issue.get("details", {}).get("text")
                    message = message or issue.get("diagnostics")
            except JSONDecodeError:
                message = exc.response.text
            if not message:
                message = str(exc)

            raise FatalError(f'An error occurred when connecting to "{url}": {message}') from exc

        return response

    ###################################################################################################################
    #
    # Helpers
    #
    ###################################################################################################################

    async def _request_with_signed_headers(
        self, method: str, url: str, headers: dict = None, **kwargs
    ) -> httpx.Response:
        """
        Issues a GET request and sign the headers with the current access token.

        :param method: HTTP method to issue
        :param url: full server url to request
        :param headers: header dictionary
        :returns: The response object
        """
        headers = self._auth.sign_headers(headers)
        request = self._session.build_request(method, url, headers=headers)
        # Follow redirects by default -- some EHRs definitely use them for bulk download files,
        # and might use them in other cases, who knows.
        return await self._session.send(request, follow_redirects=True, **kwargs)
