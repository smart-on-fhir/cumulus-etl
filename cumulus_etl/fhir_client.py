"""HTTP client that talk to a FHIR server"""

import argparse
import base64
import re
import sys
import time
import urllib.parse
import uuid
from json import JSONDecodeError
from collections.abc import Iterable

import httpx
from jwcrypto import jwk, jwt

from cumulus_etl import common, errors, store


class FatalError(Exception):
    """An unrecoverable error"""


def _urljoin(base: str, path: str) -> str:
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
            _urljoin(self._server_root, ".well-known/smart-configuration"),
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


class FhirClient:
    """
    Manages authentication and requests for a FHIR server.

    Supports a few different auth methods, but most notably the Backend Service SMART profile.

    Use this as a context manager (like you would an httpx.AsyncClient instance).

    See https://hl7.org/fhir/smart-app-launch/backend-services.html for details.
    """

    def __init__(
        self,
        url: str | None,
        resources: Iterable[str],
        basic_user: str = None,
        basic_password: str = None,
        bearer_token: str = None,
        smart_client_id: str = None,
        smart_jwks: dict = None,
    ):
        """
        Initialize and authorize a BackendServiceServer context manager.

        :param url: base URL of the SMART FHIR server
        :param resources: a list of FHIR resource names to tightly scope our own permissions
        :param basic_user: username for Basic authentication
        :param basic_password: password for Basic authentication
        :param bearer_token: a bearer token, containing the secret key to sign https requests (instead of JWKS)
        :param smart_client_id: the ID assigned by the FHIR server when registering a new backend service app
        :param smart_jwks: content of a JWK Set file, containing the private key for the registered public key
        """
        self._server_root = url  # all requests are relative to this URL
        if self._server_root and not self._server_root.endswith("/"):
            self._server_root += "/"  # This will ensure the last segment does not get chopped off by urljoin
        self._auth = self._make_auth(resources, basic_user, basic_password, bearer_token, smart_client_id, smart_jwks)
        self._session: httpx.AsyncClient | None = None

    async def __aenter__(self):
        # Limit the number of connections open at once, because EHRs tend to be very busy.
        limits = httpx.Limits(max_connections=5)
        self._session = httpx.AsyncClient(limits=limits, timeout=300)  # five minutes to be generous
        await self._auth.authorize(self._session)
        return self

    async def __aexit__(self, exc_type, exc_value, traceback):
        if self._session:
            await self._session.aclose()

    async def request(self, method: str, path: str, headers: dict = None, stream: bool = False) -> httpx.Response:
        """
        Issues an HTTP request.

        The default Accept type is application/fhir+json, but can be overridden by a provided header.

        This is a lightly modified version of FHIRServer._get(), but additionally supports streaming and
        reauthorization.

        Will raise a FatalError for an HTTP error, except for 429 which gets returned like a success code.

        :param method: HTTP method to issue
        :param path: relative path from the server root to request
        :param headers: optional header dictionary
        :param stream: whether to stream content in or load it all into memory at once
        :returns: The response object
        """
        url = _urljoin(self._server_root, path)

        final_headers = {
            "Accept": "application/fhir+json",
            "Accept-Charset": "UTF-8",
        }
        # merge in user headers with defaults
        final_headers.update(headers or {})

        response = await self._request_with_signed_headers(method, url, final_headers, stream=stream)

        # Check if our access token expired and thus needs to be refreshed
        if response.status_code == 401:
            await self._auth.authorize(self._session, reauthorize=True)
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

    def _make_auth(
        self,
        resources: Iterable[str],
        basic_user: str,
        basic_password: str,
        bearer_token: str,
        smart_client_id: str,
        smart_jwks: dict,
    ) -> Auth:
        """Determine which auth method to use based on user provided arguments"""
        valid_smart_jwks = smart_jwks is not None  # compared to a falsy (but technically usable) empty dict for example

        # Check if the user tried to specify multiple types of auth, and help them out
        has_basic_args = bool(basic_user or basic_password)
        has_bearer_args = bool(bearer_token)
        has_smart_args = bool(smart_client_id or valid_smart_jwks)
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
            return JwksAuth(self._server_root, smart_client_id, smart_jwks, resources)
        elif smart_client_id or valid_smart_jwks:
            print(
                "You must provide both --smart-client-id and --smart-jwks to connect to a SMART FHIR server.",
                file=sys.stderr,
            )
            raise SystemExit(errors.SMART_CREDENTIALS_MISSING)

        return Auth()

    async def _request_with_signed_headers(self, method: str, url: str, headers: dict, **kwargs) -> httpx.Response:
        """
        Issues a GET request and sign the headers with the current access token.

        :param method: HTTP method to issue
        :param url: full server url to request
        :param headers: header dictionary
        :returns: The response object
        """
        if not self._session:
            raise RuntimeError("FhirClient must be used as a context manager")

        headers = self._auth.sign_headers(headers)
        request = self._session.build_request(method, url, headers=headers)
        # Follow redirects by default -- some EHRs definitely use them for bulk download files,
        # and might use them in other cases, who knows.
        return await self._session.send(request, follow_redirects=True, **kwargs)


def create_fhir_client_for_cli(
    args: argparse.Namespace,
    root_input: store.Root,
    resources: Iterable[str],
) -> FhirClient:
    """
    Create a FhirClient instance, based on user input from the CLI.

    The usual FHIR server authentication options should be represented in args.
    """
    client_base_url = args.fhir_url
    if root_input.protocol in {"http", "https"}:
        if args.fhir_url and not root_input.path.startswith(args.fhir_url):
            print(
                "You provided both an input FHIR server and a different --fhir-url. Try dropping --fhir-url.",
                file=sys.stderr,
            )
            raise SystemExit(errors.ARGS_CONFLICT)
        elif not client_base_url:
            # Use the input URL as the base URL. But note that it may not be the server root.
            # For example, it may be a Group export URL. Let's try to find the actual root.
            client_base_url = root_input.path
            client_base_url = re.sub(r"/Patient/?$", "/", client_base_url)
            client_base_url = re.sub(r"/Group/[^/]+/?$", "/", client_base_url)

    try:
        try:
            # Try to load client ID from file first (some servers use crazy long ones, like SMART's bulk-data-server)
            smart_client_id = common.read_text(args.smart_client_id).strip() if args.smart_client_id else None
        except FileNotFoundError:
            smart_client_id = args.smart_client_id

        smart_jwks = common.read_json(args.smart_jwks) if args.smart_jwks else None
        basic_password = common.read_text(args.basic_passwd).strip() if args.basic_passwd else None
        bearer_token = common.read_text(args.bearer_token).strip() if args.bearer_token else None
    except OSError as exc:
        print(exc, file=sys.stderr)
        raise SystemExit(errors.ARGS_INVALID) from exc

    client_resources = set(resources)
    if "DocumentReference" in client_resources:
        # A DocumentReference scope implies a Binary scope as well, since we'll usually need to download attachments
        client_resources.add("Binary")

    return FhirClient(
        client_base_url,
        client_resources,
        basic_user=args.basic_user,
        basic_password=basic_password,
        bearer_token=bearer_token,
        smart_client_id=smart_client_id,
        smart_jwks=smart_jwks,
    )
