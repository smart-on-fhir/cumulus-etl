"""HTTP client that talk to a FHIR server"""

import argparse
import enum
import re
import sys
from collections.abc import Iterable
from json import JSONDecodeError

import httpx

from cumulus_etl import common, errors, store
from cumulus_etl.fhir import fhir_auth


class ServerType(enum.Enum):
    UNKNOWN = enum.auto()
    CERNER = enum.auto()
    EPIC = enum.auto()


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
        basic_user: str | None = None,
        basic_password: str | None = None,
        bearer_token: str | None = None,
        smart_client_id: str | None = None,
        smart_jwks: dict | None = None,
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
            # This will ensure the last segment does not get chopped off by urljoin
            self._server_root += "/"

        self._client_id = smart_client_id
        self._server_type = ServerType.UNKNOWN
        self._auth = fhir_auth.create_auth(
            self._server_root,
            resources,
            basic_user,
            basic_password,
            bearer_token,
            smart_client_id,
            smart_jwks,
        )
        self._session: httpx.AsyncClient | None = None
        self._capabilities: dict = {}

    async def __aenter__(self):
        # Limit the number of connections open at once, because EHRs tend to be very busy.
        limits = httpx.Limits(max_connections=5)
        timeout = 300  # five minutes to be generous
        # Follow redirects by default -- some EHRs definitely use them for bulk download files,
        # and might use them in other cases, who knows.
        self._session = httpx.AsyncClient(limits=limits, timeout=timeout, follow_redirects=True)
        await self._read_capabilities()  # discover server type, etc
        await self._auth.authorize(self._session)
        return self

    async def __aexit__(self, exc_type, exc_value, traceback):
        if self._session:
            await self._session.aclose()

    async def request(
        self, method: str, path: str, headers: dict | None = None, stream: bool = False
    ) -> httpx.Response:
        """
        Issues an HTTP request.

        The default Accept type is application/fhir+json, but can be overridden by a provided
        header.

        This is a lightly modified version of FHIRServer._get(), but additionally supports
        streaming and reauthorization.

        Will raise a FatalError for an HTTP error, except for 429 which gets returned like a
        success code.

        :param method: HTTP method to issue
        :param path: relative path from the server root to request
        :param headers: optional header dictionary
        :param stream: whether to stream content in or load it all into memory at once
        :returns: The response object
        """
        url = fhir_auth.urljoin(self._server_root, path)

        final_headers = {
            "Accept": "application/fhir+json",
            "Accept-Charset": "UTF-8",
        }
        # merge in user headers with defaults
        final_headers.update(headers or {})

        response = await self._request_with_signed_headers(
            method, url, final_headers, stream=stream
        )

        # Check if our access token expired and thus needs to be refreshed
        if response.status_code == 401:
            await self._auth.authorize(self._session, reauthorize=True)
            if stream:
                await response.aclose()
            response = await self._request_with_signed_headers(
                method, url, final_headers, stream=stream
            )

        try:
            response.raise_for_status()
        except httpx.HTTPStatusError as exc:
            if exc.response.status_code == 429:
                # 429 is a special kind of error -- it's not fatal, just a request to wait a bit. So let it pass.
                return exc.response

            if stream:
                await response.aread()
                await response.aclose()

            # All other 4xx or 5xx codes are treated as fatal errors
            message = None
            try:
                json_response = exc.response.json()
                if not isinstance(json_response, dict):
                    message = exc.response.text
                elif json_response.get("resourceType") == "OperationOutcome":
                    issue = json_response["issue"][0]  # just grab first issue
                    message = issue.get("details", {}).get("text")
                    message = message or issue.get("diagnostics")
            except JSONDecodeError:
                message = exc.response.text
            if not message:
                message = str(exc)

            raise errors.NetworkError(
                f'An error occurred when connecting to "{url}": {message}',
                response,
            ) from exc

        return response

    def get_capabilities(self) -> dict:
        """
        Returns the server's CapabilityStatement, if available.

        See https://www.hl7.org/fhir/R4/capabilitystatement.html

        If the statement could not be retrieved, this returns an empty dict.
        """
        return self._capabilities

    #############################################################################################
    #
    # Helpers
    #
    #############################################################################################

    async def _read_capabilities(self) -> None:
        """
        Reads the server's CapabilityStatement and sets any properties as a result.

        Notably, this gathers the server/vendor type.
        This is expected to be called extremely early, right as the http session is opened.
        """
        if not self._server_root:
            return

        print("Connecting to server…")

        try:
            response = await self._session.get(
                fhir_auth.urljoin(self._server_root, "metadata"),
                headers={
                    "Accept": "application/json",
                    "Accept-Charset": "UTF-8",
                },
            )
            response.raise_for_status()
        except httpx.HTTPError:
            return  # That's fine - just skip this optional metadata

        try:
            capabilities = response.json()
        except JSONDecodeError:
            return

        if capabilities.get("publisher") == "Cerner":
            # Example: https://fhir-ehr-code.cerner.com/r4/ec2458f2-1e24-41c8-b71b-0e701af7583d/metadata?_format=json
            self._server_type = ServerType.CERNER
        elif capabilities.get("software", {}).get("name") == "Epic":
            # Example: https://fhir.epic.com/interconnect-fhir-oauth/api/FHIR/R4/metadata?_format=json
            self._server_type = ServerType.EPIC

        self._capabilities = capabilities

    async def _request_with_signed_headers(
        self, method: str, url: str, headers: dict, **kwargs
    ) -> httpx.Response:
        """
        Issues a GET request and sign the headers with the current access token.

        :param method: HTTP method to issue
        :param url: full server url to request
        :param headers: header dictionary
        :returns: The response object
        """
        if not self._session:
            raise RuntimeError("FhirClient must be used as a context manager")

        # Epic wants to see the Epic-Client-ID header, especially for non-OAuth flows.
        # (but I've heard reports of also wanting it in OAuth flows too)
        # See https://fhir.epic.com/Documentation?docId=oauth2&section=NonOauth_Epic-Client-ID-Header
        if self._server_type == ServerType.EPIC and self._client_id:
            headers["Epic-Client-ID"] = self._client_id

        headers = self._auth.sign_headers(headers)
        request = self._session.build_request(method, url, headers=headers)
        return await self._session.send(request, **kwargs)


def create_fhir_client_for_cli(
    args: argparse.Namespace,
    root_input: store.Root,
    resources: Iterable[str],
) -> FhirClient:
    """
    Create a FhirClient instance, based on user input from the CLI.

    The usual FHIR server authentication options should be represented in args.
    """
    client_base_url = getattr(args, "fhir_url", None)
    if root_input.protocol in {"http", "https"}:
        if client_base_url and not root_input.path.startswith(client_base_url):
            print(
                "You provided both an input FHIR server and a different --fhir-url. Try dropping --fhir-url.",
                file=sys.stderr,
            )
            raise SystemExit(errors.ARGS_CONFLICT)
        elif not client_base_url:
            # Use the input URL as the base URL. But note that it may not be the server root.
            # For example, it may be a Group export URL. Let's try to find the actual root.
            client_base_url = root_input.path
            client_base_url = re.sub(r"/\$export(\?.*)?$", "/", client_base_url)
            client_base_url = re.sub(r"/Patient/?$", "/", client_base_url)
            client_base_url = re.sub(r"/Group/[^/]+/?$", "/", client_base_url)

    try:
        try:
            # Try to load client ID from file first (some servers use crazy long ones, like SMART's bulk-data-server)
            smart_client_id = (
                common.read_text(args.smart_client_id).strip() if args.smart_client_id else None
            )
        except FileNotFoundError:
            smart_client_id = args.smart_client_id

        smart_jwks = common.read_json(args.smart_jwks) if args.smart_jwks else None
        basic_password = common.read_text(args.basic_passwd).strip() if args.basic_passwd else None
        bearer_token = common.read_text(args.bearer_token).strip() if args.bearer_token else None
    except OSError as exc:
        print(exc, file=sys.stderr)
        raise SystemExit(errors.ARGS_INVALID) from exc

    client_resources = set(resources)
    if {"DiagnosticReport", "DocumentReference"} & client_resources:
        # Resources with attachments imply a Binary scope as well,
        # since we'll usually need to download the referenced content.
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
