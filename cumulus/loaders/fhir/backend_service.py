"""Support for SMART App Launch Backend Services"""

import re
import time
import urllib.parse
import uuid
from json import JSONDecodeError
from typing import List

import requests
from fhirclient.client import FHIRClient
from jwcrypto import jwk, jwt


class FatalError(Exception):
    """An unrecoverable error"""


class BackendServiceServer:
    """
    Manages authentication and requests for a server that supports the Backend Service SMART profile.

    See https://hl7.org/fhir/smart-app-launch/backend-services.html for details.
    """

    def __init__(self, url: str, client_id: str, jwks: dict, resources: List[str]):
        """
        Initialize and authorize a BackendServiceServer instance.

        :param url: base URL of the SMART FHIR server
        :param client_id: the ID assigned by the FHIR server when registering a new backend service app
        :param jwks: content of a JWK Set file, containing the private key for the registered public key
        :param resources: a list of FHIR resource names to tightly scope our own permissions
        """
        super().__init__()
        self._base_url = url  # all requests are relative to this URL
        if not self._base_url.endswith("/"):
            self._base_url += "/"
        # The base URL may not be the server root (like it may be a Group export URL). Let's find the root.
        self._server_root = self._base_url
        self._server_root = re.sub(r"/Patient/$", "/", self._server_root)
        self._server_root = re.sub(r"/Group/[^/]+/$", "/", self._server_root)
        self._client_id = client_id
        self._jwks = jwks
        self._resources = list(resources)
        self._server = None
        self._token_endpoint = self._get_token_endpoint()
        self._authorize()

    def request(self, method: str, path: str, headers: dict = None, stream: bool = False) -> requests.Response:
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

        response = self._request_with_signed_headers(method, url, final_headers, stream=stream)

        # Check if our access token expired and thus needs to be refreshed
        if response.status_code == 401:
            if not self._server.reauthorize():
                # We must not have been issued a refresh token, let's just authorize from scratch
                self._authorize()
            response = self._request_with_signed_headers(method, url, final_headers, stream=stream)

        try:
            response.raise_for_status()
        except requests.exceptions.HTTPError as exc:
            if exc.response.status_code == 429:
                # 429 is a special kind of error -- it's not fatal, just a request to wait a bit. So let it pass.
                return exc.response

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

    def _authorize(self) -> None:
        """
        Authenticates against a SMART FHIR server using the Backend Services profile.

        See https://hl7.org/fhir/smart-app-launch/backend-services.html for details.
        """
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

    def _request_with_signed_headers(self, method: str, url: str, headers: dict = None, **kwargs) -> requests.Response:
        """
        Issues a GET request and sign the headers with the current access token.

        :param method: HTTP method to issue
        :param url: full server url to request
        :param headers: header dictionary
        :returns: The response object
        """
        headers = self._server.auth.signed_headers(headers)
        return self._server.session.request(method, url, headers=headers, **kwargs)

    def _get_token_endpoint(self) -> str:
        """
        Returns the oauth2 token endpoint for a SMART FHIR server.

        See https://hl7.org/fhir/smart-app-launch/client-confidential-asymmetric.html for details.

        If the server does not support the client-confidential-asymmetric protocol, an exception will be raised.

        :returns: URL for the server's oauth2 token endpoint
        """
        response = requests.get(
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
