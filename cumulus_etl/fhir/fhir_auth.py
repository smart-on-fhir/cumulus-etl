"""Code for the various ways to authenticate against a FHIR server"""

import base64
import time
import urllib.parse
import uuid
from collections.abc import Iterable

import httpx
from jwcrypto import jwk, jwt

from cumulus_etl import errors, http


def urljoin(base: str, path: str) -> str:
    """Basically just urllib.parse.urljoin, but with some extra error checking"""
    path_is_absolute = bool(urllib.parse.urlparse(path).netloc)
    if path_is_absolute:
        return path

    if not base:
        raise errors.FhirUrlMissing()
    return urllib.parse.urljoin(base, path)


class Auth:
    """Abstracted authentication for a FHIR server. By default, does nothing."""

    async def authorize(self, session: httpx.AsyncClient, reauthorize=False) -> None:
        """Authorize (or re-authorize) against the server"""
        del session

        if reauthorize:
            # We clearly need auth tokens, but have not been given any parameters for them.
            raise errors.FhirAuthMissing()

    def sign_headers(self) -> dict[str, str]:
        """New headers to add, with the signature token"""
        return {}


class JwtAuth(Auth):
    """Authentication with a JWT (typical OAuth2 backend service profile)"""

    def __init__(
        self, server_root: str, client_id: str, jwks: dict, pem: str, resources: Iterable[str]
    ):
        super().__init__()
        self._server_root = server_root
        self._client_id = client_id
        self._jwks = jwks
        self._pem = pem
        self._resources = list(resources)
        self._config = None
        self._access_token = None

    async def authorize(self, session: httpx.AsyncClient, reauthorize=False) -> None:
        """
        Authenticates against a SMART FHIR server using the Backend Services profile.

        See https://hl7.org/fhir/smart-app-launch/backend-services.html for details.
        """
        await self._ensure_config(session)

        # Use v1 scopes unless the server says is only uses v2
        capabilities = self._config.get("capabilities", [])
        if "permission-v2" in capabilities and "permission-v1" not in capabilities:
            scope = "rs"
        else:
            scope = "read"

        auth_params = {
            "grant_type": "client_credentials",
            "scope": " ".join([f"system/{resource}.{scope}" for resource in self._resources]),
            "client_assertion_type": "urn:ietf:params:oauth:client-assertion-type:jwt-bearer",
            "client_assertion": self._make_signed_jwt(),
        }

        try:
            response = await http.request(
                session, "POST", self._config["token_endpoint"], data=auth_params
            )
            self._access_token = response.json().get("access_token")
        except errors.NetworkError as exc:
            errors.fatal(str(exc), errors.FHIR_AUTH_FAILED)

    def sign_headers(self) -> dict[str, str]:
        """Add signature token to request headers"""
        return {"Authorization": f"Bearer {self._access_token}"}

    async def _ensure_config(self, session: httpx.AsyncClient) -> None:
        """Grabs the SMART configuration, with useful bits like the OAuth endpoint"""
        if self._config is not None:
            return

        response = await http.request(
            session,
            "GET",
            urljoin(self._server_root, ".well-known/smart-configuration"),
            headers={"Accept": "application/json"},
        )
        self._config = response.json()

        # We used to validate some other pieces of this response (like support for the
        # 'client-confidential-asymmetric' capability keyword or 'private_key_jwt' in the
        # 'token_endpoint_auth_methods_supported' field). But servers rarely advertise correctly
        # (No one seems to use the asymmetric capability keyword and Veradigm doesn't fill out the
        # endpoint auth methods field with all methods it supports). So :shrug: we'll just assume
        # things are fine and error out later if they aren't fine. The only thing we _need_ is the
        # token endpoint.
        if not self._config.get("token_endpoint"):
            errors.fatal(
                f"Server {self._server_root} does not expose an OAuth token endpoint",
                errors.FHIR_AUTH_FAILED,
            )

    def _make_pem_jwk(self) -> tuple[str, jwk.JWK]:
        try:
            jwk_key = jwk.JWK.from_pem(self._pem.encode("utf8"))
        except ValueError:
            jwk_key = None  # will fail below

        if jwk_key and jwk_key.has_private:
            # Unfortunately, we can't just ask jcrypto "hey what JWT alg value should I use here?".
            # So instead, we check for a few common values.
            if jwk_key.get("kty") == "RSA":
                # Could pick any RS* value (like RS256, RS384, or RS512), assuming the server
                # supports it. Since 384 is practically as secure as 512, but more common, we'll
                # use that.
                return "RS384", jwk_key
            elif jwk_key.get("kty") == "EC" and jwk_key.get("crv") == "P-256":
                return "ES256", jwk_key
            elif jwk_key.get("kty") == "EC" and jwk_key.get("crv") == "P-384":
                return "ES384", jwk_key
            elif jwk_key.get("kty") == "EC" and jwk_key.get("crv") == "P-521":
                # Yes, P-521 is not a typo, it's what the curve is called.
                # The curve uses 521 bits, but it's hashed with SHA512, so it's called ES512.
                return "ES512", jwk_key

        errors.fatal(
            "No supported private key found in the provided PEM file.", errors.BAD_SMART_CREDENTIAL
        )

    def _make_jwks_jwk(self) -> tuple[str, jwk.JWK]:
        # Find a usable signing JWK from JWKS
        for key in self._jwks.get("keys", []):
            if "sign" in key.get("key_ops", []) and key.get("kid") and key.get("alg"):
                return key["alg"], jwk.JWK(**key)
        errors.fatal(
            "No valid private key found in the provided JWKS file.", errors.BAD_SMART_CREDENTIAL
        )

    def _make_signed_jwt(self) -> str:
        """
        Creates a signed JWT for use in the client-confidential-asymmetric protocol.

        See https://hl7.org/fhir/smart-app-launch/client-confidential-asymmetric.html for details.

        :returns: a signed JWT string, ready for authentication with the FHIR server
        """
        if self._pem:
            generator = self._make_pem_jwk
        else:
            generator = self._make_jwks_jwk
        algorithm, jwk_key = generator()

        # Now generate a signed JWT based off the given JWK
        header = {
            "alg": algorithm,
            "typ": "JWT",
        }
        if "kid" in jwk_key:
            header["kid"] = jwk_key["kid"]
        claims = {
            "iss": self._client_id,
            "sub": self._client_id,
            "aud": self._config["token_endpoint"],
            "exp": int(time.time()) + 299,  # expires inside five minutes
            "jti": str(uuid.uuid4()),
        }
        token = jwt.JWT(header=header, claims=claims)
        token.make_signed_token(key=jwk_key)
        return token.serialize()


class BasicAuth(Auth):
    """Authentication with basic user/password"""

    def __init__(self, user: str, password: str):
        super().__init__()
        # Assume utf8 is acceptable -- we should in theory also run these through Unicode normalization, in case they
        # have interesting Unicode characters. But we can always add that in the future.
        combo_bytes = f"{user}:{password}".encode()
        self._basic_token = base64.standard_b64encode(combo_bytes).decode("ascii")

    async def authorize(self, session: httpx.AsyncClient, reauthorize=False) -> None:
        pass

    def sign_headers(self) -> dict[str, str]:
        return {"Authorization": f"Basic {self._basic_token}"}


class BearerAuth(Auth):
    """Authentication with a static bearer token"""

    def __init__(self, bearer_token: str):
        super().__init__()
        self._bearer_token = bearer_token

    async def authorize(self, session: httpx.AsyncClient, reauthorize=False) -> None:
        pass

    def sign_headers(self) -> dict[str, str]:
        return {"Authorization": f"Bearer {self._bearer_token}"}


def create_auth(
    server_root: str | None,
    resources: Iterable[str],
    basic_user: str | None,
    basic_password: str | None,
    bearer_token: str | None,
    smart_client_id: str | None,
    smart_jwks: dict | None,
    smart_pem: str | None,
) -> Auth:
    """Determine which auth method to use based on user provided arguments"""
    valid_smart_jwks = smart_jwks is not None
    valid_smart_pem = smart_pem is not None

    # Check if the user tried to specify multiple types of auth, and help them out
    has_basic_args = bool(basic_user or basic_password)
    has_bearer_args = bool(bearer_token)
    has_smart_args = bool(valid_smart_jwks or valid_smart_pem)
    total_auth_types = has_basic_args + has_bearer_args + has_smart_args
    if total_auth_types > 1:
        errors.fatal(
            "Multiple authentication methods have been specified. "
            "Double check your arguments to Cumulus ETL.",
            errors.ARGS_CONFLICT,
        )

    if basic_user and basic_password:
        return BasicAuth(basic_user, basic_password)
    elif basic_user or basic_password:
        errors.fatal(
            "You must provide both --basic-user and --basic-password "
            "to connect to a Basic auth server.",
            errors.BASIC_CREDENTIALS_MISSING,
        )

    if bearer_token:
        return BearerAuth(bearer_token)

    if smart_client_id and has_smart_args:
        return JwtAuth(server_root, smart_client_id, smart_jwks, smart_pem, resources)
    elif smart_client_id or has_smart_args:
        errors.fatal(
            "You must provide both --smart-client-id and --smart-key "
            "to connect to a SMART FHIR server.",
            errors.SMART_CREDENTIALS_MISSING,
        )

    return Auth()
