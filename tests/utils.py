"""Various test helper methods"""

import contextlib
import datetime
import filecmp
import functools
import inspect
import json
import os
import time
import unittest
from unittest import mock

import freezegun
import httpx
import respx

from cumulus_etl.formats.deltalake import DeltaLakeFormat

# Pass a non-UTC time to freezegun to help notice any bad timezone handling.
# But only bother exposing the UTC version to other test code, since that's what will be most useful/common.
_FROZEN_TIME = datetime.datetime(2021, 9, 15, 1, 23, 45, tzinfo=datetime.timezone(datetime.timedelta(hours=4)))
FROZEN_TIME_UTC = _FROZEN_TIME.astimezone(datetime.timezone.utc)


# Several tests involve timestamps in some form, so just pick a standard time for all tests.
# We ignore socketserver because it checks the result of time() when evaluating timeouts.
@freezegun.freeze_time(_FROZEN_TIME, ignore=["socketserver"])
class AsyncTestCase(unittest.IsolatedAsyncioTestCase):
    """
    Test case to hold some common code (suitable for async *OR* sync tests)

    It also works around a particularly annoying async test case bug in Python 3.10.
    """

    def setUp(self):
        super().setUp()
        self.patch(
            "cumulus_etl.deid.codebook.secrets.token_hex", new=lambda x: "1234"
        )  # keep all codebook IDs consistent

        # It's so common to want to see more than the tiny default fragment -- just enable this across the board.
        self.maxDiff = None  # pylint: disable=invalid-name

        # Make it easy to grab test data, regardless of where the test is
        self.datadir = os.path.join(os.path.dirname(__file__), "data")

    def patch(self, *args, **kwargs) -> mock.Mock:
        """Syntactic sugar to ease making a mock over a test's lifecycle, without decorators"""
        patcher = mock.patch(*args, **kwargs)
        self.addCleanup(patcher.stop)
        return patcher.start()

    def patch_dict(self, *args, **kwargs) -> mock.Mock:
        """Syntactic sugar to ease making a dictionary mock over a test's lifecycle, without decorators"""
        patcher = mock.patch.dict(*args, **kwargs)
        self.addCleanup(patcher.stop)
        return patcher.start()

    def patch_object(self, *args, **kwargs) -> mock.Mock:
        """Syntactic sugar to ease making an object mock over a test's lifecycle, without decorators"""
        patcher = mock.patch.object(*args, **kwargs)
        self.addCleanup(patcher.stop)
        return patcher.start()

    async def _catch_system_exit(self, method):
        try:
            ret = method()
            if inspect.isawaitable(ret):
                return await ret
            return ret
        except SystemExit:
            self.fail("Raised unexpected system exit")

    def _callTestMethod(self, method):
        """
        Works around an async test case bug in python 3.10 and below.

        This seems to be some version of https://github.com/python/cpython/issues/83282 but fixed & never backported.
        I was not able to find a separate bug report for this specific issue.

        Given the following two test methods (for Pythons before 3.11), only the second one will hang:

            async def test_fails_correctly(self):
                raise BaseException("OK")
            async def test_hangs_forever(self):
                raise SystemExit("Nope")

        This class works around that by wrapping all test methods and translating uncaught SystemExits into failures.
        _callTestMethod() can be deleted once we no longer use python 3.10 in our testing suite.
        """
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
            self.assert_files_equal(left_path, right_path)

        for subdircmp in dircmp.subdirs.values():
            self.assert_file_tree_equal(subdircmp)

    def assert_files_equal(self, left_path: str, right_path: str) -> None:
        with open(left_path, "rb") as f:
            left_contents = f.read()
        with open(right_path, "rb") as f:
            right_contents = f.read()

        # Try to avoid comparing json files byte-for-byte. We may reasonably
        # change formatting, or even want the test files in an
        # easier-to-read format than the actual output files. In theory all
        # json files are equal once parsed.
        if left_path.endswith(".json"):
            left_json = json.loads(left_contents.decode("utf8"))
            right_json = json.loads(right_contents.decode("utf8"))
            self.assertEqual(left_json, right_json, f"{right_path} vs {left_path}")
        elif left_path.endswith(".ndjson"):
            left_split = left_contents.decode("utf8").strip().splitlines()
            right_split = right_contents.decode("utf8").strip().splitlines()
            left_rows = list(map(json.loads, left_split))
            right_rows = list(map(json.loads, right_split))
            self.assertEqual(left_rows, right_rows, f"{right_path} vs {left_path}")
        else:
            self.assertEqual(left_contents, right_contents, f"{right_path} vs {left_path}")


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


def read_delta_lake(lake_path: str, *, version: int = None) -> list[dict]:
    """
    Reads in a delta lake folder at a certain time, sorted by id.

    DeltaLakeFormat.initialize_class() must have already been called.

    Compare the results to a pandas dataframe with df.to_dict(orient="records") or just as a list of dicts.
    """
    # Read spark table
    reader = DeltaLakeFormat.spark.read
    if version is not None:
        reader = reader.option("versionAsOf", version)

    table_spark = reader.format("delta").load(lake_path).sort("id")

    # Convert the spark table to Python primitives.
    # Going to rdd or pandas and then to Python keeps inserting spark-specific constructs like Row().
    # So instead, convert to a JSON string and then back to Python.
    return [json.loads(row) for row in table_spark.toJSON().collect()]


@contextlib.contextmanager
def timeit(desc: str = None):
    """Tiny little timer context manager that is useful when debugging"""
    start = time.perf_counter()
    yield
    end = time.perf_counter()
    suffix = f" ({desc})" if desc else ""
    print(f"TIMEIT: {end - start:.2f}s{suffix}")
