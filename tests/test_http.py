"""Tests for http.py"""

from unittest import mock

import ddt
import httpx
import respx

from cumulus_etl import errors, http
from tests.utils import AsyncTestCase, make_response


@ddt.ddt
class TestHttpUtils(AsyncTestCase):
    """
    Test case for http utility methods
    """

    def setUp(self):
        super().setUp()

        # Initialize responses mock
        self.respx_mock = respx.mock(assert_all_called=False)
        self.addCleanup(self.respx_mock.stop)
        self.respx_mock.start()

    @ddt.data(True, False)  # confirm that we handle both stream and non-stream resets
    async def test_get_error_401(self, stream_mode):
        """Verify that we call the auth callback."""
        route = self.respx_mock.get("http://example.com/")
        route.side_effect = [make_response(status_code=401), make_response()]

        auth_callback = mock.AsyncMock(return_value={})
        response = await http.request(
            httpx.AsyncClient(),
            "GET",
            "http://example.com/",
            stream=stream_mode,
            auth_callback=auth_callback,
        )
        self.assertEqual(response.status_code, 200)
        self.assertEqual(auth_callback.call_count, 1)

    async def test_get_error_401_persistent_error(self):
        """Verify that we surface persistent errors during auth."""
        self.respx_mock.get("http://example.com/").respond(status_code=401)

        auth_callback = mock.AsyncMock(return_value={})
        with self.assertRaises(errors.FatalError) as cm:
            await http.request(
                httpx.AsyncClient(),
                "GET",
                "http://example.com/",
                retry_delays=[],
                auth_callback=auth_callback,
            )
        self.assertEqual(cm.exception.response.status_code, 401)
        self.assertEqual(auth_callback.call_count, 1)

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
        {"json_payload": {"error_description": "testmsg"}},
        {"json_payload": {"error_uri": "http://testmsg.com/"}},
    )
    async def test_get_error_other(self, response_args):
        """Verify that other http errors are FatalErrors."""
        self.respx_mock.get("http://example.com/").mock(
            return_value=make_response(status_code=400, **response_args),
        )
        with self.assertRaisesRegex(errors.NetworkError, "testmsg"):
            await http.request(httpx.AsyncClient(), "GET", "http://example.com/")

    @ddt.data(
        (None, 120),  # default to the caller's retry delay
        ("10", 10),  # parse simple integers
        ("Tue, 14 Sep 2021 21:23:58 GMT", 13),  # parse http-dates too
        ("abc", 120),  # if parsing fails, use caller's retry delay
        ("-5", 0),  # floor of zero
        ("Mon, 13 Sep 2021 21:23:58 GMT", 0),  # floor of zero on dates too
    )
    @ddt.unpack
    async def test_get_retry_after(self, retry_after_header, expected_delay):
        headers = {"Retry-After": retry_after_header} if retry_after_header else {}
        response = make_response(headers=headers)
        self.assertEqual(http.get_retry_after(response, 120), expected_delay)

    @ddt.data(
        # status, expect_retry
        (300, False),
        (400, False),
        (408, True),
        (429, True),
        (500, True),
        (501, False),
        (502, True),
        (503, True),
        (504, True),
    )
    @ddt.unpack
    async def test_retry_codes(self, status_code, expect_retry):
        self.respx_mock.get("http://example.com/").respond(status_code=status_code)

        with self.assertRaises(errors.NetworkError) as cm:
            await http.request(httpx.AsyncClient(), "GET", "http://example.com/", retry_delays=[1])

        self.assertEqual(self.sleep_mock.call_count, 1 if expect_retry else 0)
        self.assertIsInstance(
            cm.exception,
            errors.TemporaryNetworkError if expect_retry else errors.FatalNetworkError,
        )
