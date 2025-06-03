"""HTTP client that talk to a FHIR server"""

import argparse
import enum
from collections.abc import Callable, Iterable
from json import JSONDecodeError

import httpx

from cumulus_etl import common, errors, fhir, http, store
from cumulus_etl.fhir import fhir_auth, fhir_utils


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

    # Limit the number of connections open at once, because EHRs tend to be very busy.
    MAX_CONNECTIONS = 5

    def __init__(
        self,
        url: str | None,
        resources: Iterable[str],
        basic_user: str | None = None,
        basic_password: str | None = None,
        bearer_token: str | None = None,
        smart_client_id: str | None = None,
        smart_jwks: dict | None = None,
        smart_pem: str | None = None,
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
            smart_pem,
        )
        self._session: httpx.AsyncClient | None = None
        self._capabilities: dict = {}

    async def __aenter__(self):
        limits = httpx.Limits(max_connections=self.MAX_CONNECTIONS)
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
        self,
        method: str,
        path: str,
        headers: dict | None = None,
        stream: bool = False,
        retry_delays: Iterable[int] | None = None,
        request_callback: Callable[[], None] | None = None,
        error_callback: Callable[[errors.NetworkError], None] | None = None,
        retry_callback: Callable[[httpx.Response | None, int], None] | None = None,
    ) -> httpx.Response:
        """
        Issues an HTTP request.

        The default Accept type is application/fhir+json, but can be overridden by a provided
        header.

        May raise a NetworkError.

        :param method: HTTP method to issue
        :param path: relative path from the server root to request
        :param headers: optional header dictionary
        :param stream: whether to stream content in or load it all into memory at once
        :param retry_delays: how many minutes to wait between retries, and how many retries to do,
                             defaults to [1, 1] which is three total tries across two minutes.
        :param request_callback: called right before each request
        :param error_callback: called after each network error
        :param retry_callback: called right before sleeping
        :returns: The response object
        """
        if not self._session:
            raise RuntimeError("FhirClient must be used as a context manager")

        url = fhir_auth.urljoin(self._server_root, path)

        final_headers = {
            "Accept": "application/fhir+json",
            "Accept-Charset": "UTF-8",
        }
        # merge in user headers with defaults
        final_headers.update(headers or {})

        # Epic wants to see the Epic-Client-ID header, especially for non-OAuth flows.
        # (but I've heard reports of also wanting it in OAuth flows too)
        # See https://fhir.epic.com/Documentation?docId=oauth2&section=NonOauth_Epic-Client-ID-Header
        if self._server_type == ServerType.EPIC and self._client_id:
            final_headers["Epic-Client-ID"] = self._client_id

        final_headers.update(self._auth.sign_headers())

        return await http.request(
            self._session,
            method,
            url,
            headers=final_headers,
            stream=stream,
            retry_delays=retry_delays,
            request_callback=request_callback,
            error_callback=error_callback,
            retry_callback=retry_callback,
            auth_callback=self._reauthorize,
        )

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
            response = await http.request(
                self._session,
                "GET",
                fhir_auth.urljoin(self._server_root, "metadata"),
                headers={
                    "Accept": "application/json",
                    "Accept-Charset": "UTF-8",
                },
            )
        except errors.NetworkError:
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

    async def _reauthorize(self) -> dict[str, str]:
        await self._auth.authorize(self._session, reauthorize=True)
        return self._auth.sign_headers()


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
            errors.fatal(
                "You provided both an input FHIR server and a different --fhir-url. "
                "Try dropping --fhir-url.",
                errors.ARGS_CONFLICT,
            )
        elif not client_base_url:
            # Use the input URL as the base URL. But note that it may not be the server root.
            # For example, it may be a Group export URL. Let's try to find the actual root.
            client_base_url = fhir_utils.FhirUrl(root_input.path).root_url

    try:
        try:
            # Try to load client ID from file first (some servers use crazy long ones, like SMART's bulk-data-server)
            smart_client_id = (
                common.read_text(args.smart_client_id).strip() if args.smart_client_id else None
            )
        except FileNotFoundError:
            smart_client_id = args.smart_client_id

        # Check deprecated --smart-jwks argument first
        smart_jwks = common.read_json(args.smart_jwks) if args.smart_jwks else None
        smart_pem = None
        if args.smart_key:
            folded = args.smart_key.casefold()
            if folded.endswith(".jwks"):
                smart_jwks = common.read_json(args.smart_key)
            elif folded.endswith(".pem"):
                smart_pem = common.read_text(args.smart_key).strip()
            else:
                raise OSError(
                    f"Unrecognized private key file '{args.smart_key}'\n"
                    "(must end in .jwks or .pem)."
                )

        basic_password = common.read_text(args.basic_passwd).strip() if args.basic_passwd else None
        bearer_token = common.read_text(args.bearer_token).strip() if args.bearer_token else None
    except OSError as exc:
        errors.fatal(str(exc), errors.ARGS_INVALID)

    client_resources = set(resources)

    # If resources have other linked resources, add them to the scope,
    # since we'll usually need to download the referenced content.
    client_resources |= fhir.linked_resources(client_resources)

    return FhirClient(
        client_base_url,
        client_resources,
        basic_user=args.basic_user,
        basic_password=basic_password,
        bearer_token=bearer_token,
        smart_client_id=smart_client_id,
        smart_jwks=smart_jwks,
        smart_pem=smart_pem,
    )
