"""HTTP helper methods"""

import asyncio
import email
from collections.abc import Awaitable, Callable, Iterable
from json import JSONDecodeError

import httpx

from cumulus_etl import common, errors


def get_retry_after(response: httpx.Response, default: int) -> int:
    """
    Returns the value of the Retry-After header, in seconds.

    Parsing can be tricky because the header is also allowed to be in http-date format,
    providing a specific timestamp.

    Since seconds is easier to work with for the ETL, we normalize to seconds.

    See https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Retry-After
    """
    value = response.headers.get("Retry-After", default)
    try:
        return max(0, int(value))
    except ValueError:
        pass

    try:
        retry_time = email.utils.parsedate_to_datetime(value)
    except ValueError:
        return default

    delay = retry_time - common.datetime_now()
    return max(0, delay.total_seconds())


async def request(
    client: httpx.AsyncClient,
    method: str,
    url: str,
    *,
    headers: dict | None = None,
    stream: bool = False,
    retry_delays: Iterable[int] | None = None,
    request_callback: Callable[[], None] | None = None,
    error_callback: Callable[[errors.NetworkError], None] | None = None,
    retry_callback: Callable[[httpx.Response | None, int], None] | None = None,
    auth_callback: Callable[[], Awaitable[dict[str, str]]] | None = None,
    **kwargs,  # passed on to AsyncClient
) -> httpx.Response:
    """
    Issues an HTTP request with retries.

    May raise a NetworkError.

    :param method: Client to use
    :param method: HTTP method to issue
    :param url: URL to hit
    :param headers: optional header dictionary
    :param stream: whether to stream content in or load it all into memory at once
    :param retry_delays: how many minutes to wait between retries, and how many retries to do,
                         defaults to [1, 1] which is three total tries across two minutes.
    :param request_callback: called right before each request
    :param error_callback: called after each network error
    :param retry_callback: called right before sleeping
    :param auth_callback: called to get new auth headers, if they are required
    :returns: The response object
    """
    # A small note on this default retry value:
    # We want to retry a few times, because EHRs can be flaky. But we don't want to retry TOO
    # hard, since EHRs can disguise fatal errors behind a retryable error code (like 500 or
    # 504). At least, I've seen Cerner seemingly do both. (Who can truly say if I retried that
    # 504 error 100 times instead of 50, I'd have gotten through - but I'm assuming it was
    # fatal.) It's not the worst thing to try hard to be certain, but since this is a widely
    # used default value, let's not get too crazy with the delays unless the caller opts-in
    # by providing even bigger delays as an argument.
    retry_delays = [1, 1] if retry_delays is None else list(retry_delays)
    retry_delays.append(None)  # add a final no-delay request for the loop below

    headers = dict(headers or {})  # make copy, because we may modify it for auth

    # Actually loop, attempting the request multiple times as needed
    for delay in retry_delays:
        if request_callback:
            request_callback()

        error = None
        try:
            return await _request_once(
                client, method, url, headers=headers, stream=stream, **kwargs
            )
        except errors.NetworkError as exc:
            error = exc

        # If we hit an authentication error, get new headers and try once more (without
        # counting against the retry count - this is not a "real" error but just an expected
        # timeout of auth)
        if error and error.response and error.response.status_code == 401 and auth_callback:
            headers.update(await auth_callback())
            try:
                return await _request_once(
                    client, method, url, headers=headers, stream=stream, **kwargs
                )
            except errors.NetworkError as exc:
                error = exc

        if error:
            if error_callback:
                error_callback(error)

            if delay is None or isinstance(error, errors.FatalNetworkError):
                raise error

            response = error.response  # Note: may be None in case of DNS issues or the like

        # Respect Retry-After, but only if it lets us request faster than we would have
        # otherwise. Which is maybe a little hostile, but this assumes that we are using
        # reasonable delays ourselves (for example, our retry_delay list is in *minutes* not
        # seconds). The point of this logic is so that the caller can reliably predict that
        # if they give delays totaling 10 minutes, that's the longest we'll wait.
        delay *= 60  # switch from minutes to seconds
        if response:
            delay = min(get_retry_after(response, delay), delay)

        if retry_callback:
            retry_callback(response, delay)

        # And actually do the waiting
        await asyncio.sleep(delay)


async def _request_once(
    client: httpx.AsyncClient,
    method: str,
    url: str,
    *,
    headers: dict | None = None,
    stream: bool = False,
    **kwargs,  # passed on to AsyncClient
) -> httpx.Response:
    """
    Issues a single HTTP request.

    This is a lightly modified version of FHIRServer._get(), but additionally supports
    streaming and reauthorization.

    Will raise a FatalNetworkError for a fatal HTTP error, and TemporaryNetworkError if retrying
    might help.

    :param client: Client to use
    :param method: HTTP method to issue
    :param url: URL to hit
    :param headers: optional header dictionary
    :param stream: whether to stream content in or load it all into memory at once
    :returns: The response object
    """
    request = client.build_request(method, url, headers=headers, **kwargs)
    try:
        response = await client.send(request, stream=stream)
    except httpx.HTTPError as exc:
        raise errors.FatalNetworkError(str(exc), None) from exc

    try:
        response.raise_for_status()
    except httpx.HTTPStatusError as exc:
        if stream:
            await response.aread()
            await response.aclose()

        # Find a nice message to show user, if possible
        message = None
        try:
            json_response = exc.response.json()
            if not isinstance(json_response, dict):
                message = exc.response.text
            elif json_response.get("resourceType") == "OperationOutcome":
                issue = json_response["issue"][0]  # just grab first issue
                message = issue.get("details", {}).get("text")
                message = message or issue.get("diagnostics")
            elif "error_description" in json_response:  # standard oauth2 error field
                message = json_response["error_description"]
            elif "error_uri" in json_response:  # another standard oauth2 error field
                message = f'visit "{json_response["error_uri"]}" for more details'
        except JSONDecodeError:
            message = exc.response.text
        if not message:
            message = str(exc)

        # Check if this is a retryable error, and flag it up the chain if so.
        # See https://developer.mozilla.org/en-US/docs/Web/HTTP/Status for more details.
        if response.status_code in {
            408,  # request timeout
            429,  # too many requests (server is busy)
            # 500 is so generic an error that servers may give it both for retryable cases and
            # non-retryable cases. Cerner does this, for example. Since we can't distinguish
            # between those cases, just always retry it.
            500,  # internal server error (can be temporary blip)
            502,  # bad gateway (can be temporary blip)
            503,  # service unavailable (temporary blip)
            504,  # gateway timeout (temporary blip)
        }:
            error_class = errors.TemporaryNetworkError
        else:
            error_class = errors.FatalNetworkError

        raise error_class(
            f'An error occurred when connecting to "{url}": {message}',
            response,
        ) from exc

    return response
