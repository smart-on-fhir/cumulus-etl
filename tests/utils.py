"""Various test helper methods"""

import contextlib
import filecmp
import functools
import inspect
import json
import os
import time
import unittest

import httpx
import respx


class AsyncTestCase(unittest.IsolatedAsyncioTestCase):
    """
    Test case to work around an async test case bug in python 3.10 and below.

    This seems to be some version of https://github.com/python/cpython/issues/83282 but fixed & never backported.
    I was not able to find a separate bug report for this specific issue.

    Given the following two test methods (for Pythons before 3.11), only the second one will hang:

        async def test_fails_correctly(self):
            raise BaseException("OK")
        async def test_hangs_forever(self):
            raise SystemExit("Nope")

    This class works around that by wrapping all test methods and translating uncaught SystemExits into other errors.
    It can be deleted or made into a pass-through class once we no longer use python 3.10 in our testing suite.
    """

    async def _catch_system_exit(self, method):
        try:
            ret = method()
            if inspect.isawaitable(ret):
                return await ret
            return ret
        except SystemExit:
            self.fail("Raised unexpected system exit")

    def _callTestMethod(self, method):
        return super()._callTestMethod(functools.partial(self._catch_system_exit, method))


class TreeCompareMixin(unittest.TestCase):
    """Mixin that provides a simple way to diff two trees of files"""

    def setUp(self):
        super().setUp()

        filecmp.clear_cache()

        # you'll always want this when debugging
        self.maxDiff = None  # pylint: disable=invalid-name

    def assert_etl_output_equal(self, left: str, right: str):
        """Compares the etl output with the expected json structure"""
        # We don't compare contents of the job config because it includes a lot of paths etc.
        # But we can at least confirm that it was created.
        self.assertTrue(os.path.exists(os.path.join(right, "JobConfig")))
        dircmp = filecmp.dircmp(left, right, ignore=["JobConfig"])
        self.assert_file_tree_equal(dircmp)

    def assert_file_tree_equal(self, dircmp):
        """
        Compare a tree of file content.

        filecmp.dircmp by itself likes to only do shallow comparisons that
        notice changes like timestamps. But we want the contents themselves.
        """
        self.assertEqual([], dircmp.left_only, dircmp.left)
        self.assertEqual([], dircmp.right_only, dircmp.right)

        for filename in dircmp.common_files:
            left_path = os.path.join(dircmp.left, filename)
            right_path = os.path.join(dircmp.right, filename)

            with open(left_path, "rb") as f:
                left_contents = f.read()
            with open(right_path, "rb") as f:
                right_contents = f.read()

            # Try to avoid comparing json files byte-for-byte. We may reasonably
            # change formatting, or even want the test files in an
            # easier-to-read format than the actual output files. In theory all
            # json files are equal once parsed.
            if filename.endswith(".json"):
                left_json = json.loads(left_contents.decode("utf8"))
                right_json = json.loads(right_contents.decode("utf8"))
                self.assertEqual(left_json, right_json, f"{right_path} vs {left_path}")
            elif filename.endswith(".ndjson"):
                left_split = left_contents.decode("utf8").splitlines()
                right_split = right_contents.decode("utf8").splitlines()
                left_rows = list(map(json.loads, left_split))
                right_rows = list(map(json.loads, right_split))
                self.assertEqual(left_rows, right_rows, f"{right_path} vs {left_path}")
            else:
                self.assertEqual(left_contents, right_contents, f"{right_path} vs {left_path}")

        for subdircmp in dircmp.subdirs.values():
            self.assert_file_tree_equal(subdircmp)


def make_response(status_code=200, json_payload=None, text=None, reason=None, headers=None, stream=False):
    """
    Makes a fake respx response for ease of testing.

    Usually you'll want to use respx.get(...) etc directly.
    But if you want to mock out the client <-> server interaction entirely, you can use this method to fake a
    Response object from a method that returns one.

    Example:
        server.request.return_value = make_response()
    """
    headers = dict(headers or {})
    headers.setdefault("Content-Type", "application/json" if json_payload else "text/plain; charset=utf-8")
    json_payload = json.dumps(json_payload) if json_payload else None
    body = (json_payload or text or "").encode("utf8")
    stream_contents = None
    if stream:
        stream_contents = httpx.ByteStream(body)
        body = None
    return respx.MockResponse(
        status_code=status_code,
        content=body,
        stream=stream_contents,
        extensions=reason and {"reason_phrase": reason.encode("utf8")},
        headers=headers or {},
        request=httpx.Request("GET", "fake_request_url"),
    )


@contextlib.contextmanager
def timeit(desc: str = None):
    """Tiny little timer context manager that is useful when debugging"""
    start = time.perf_counter()
    yield
    end = time.perf_counter()
    suffix = f" ({desc})" if desc else ""
    print(f"TIMEIT: {end - start:.2f}s{suffix}")
