"""Various test helper methods"""

import contextlib
import datetime
import filecmp
import functools
import inspect
import json
import os
import tempfile
import time
import tracemalloc
import unittest
from unittest import mock

import httpx
import respx
import time_machine
from jwcrypto import jwk

from cumulus_etl import fhir
from cumulus_etl.formats.deltalake import DeltaLakeFormat

# Pass a non-UTC time to time-machine to help notice any bad timezone handling.
# But only bother exposing the UTC version to other test code,
# since that's what will be most useful/common.
_FROZEN_TIME = datetime.datetime(
    2021, 9, 15, 1, 23, 45, tzinfo=datetime.timezone(datetime.timedelta(hours=4))
)
FROZEN_TIME_UTC = _FROZEN_TIME.astimezone(datetime.timezone.utc)


class AsyncTestCase(unittest.IsolatedAsyncioTestCase):
    """
    Test case to hold some common code (suitable for async *OR* sync tests)

    It also works around a particularly annoying async test case bug in Python 3.10.
    """

    def setUp(self):
        super().setUp()

        # keep all codebook IDs consistent
        self.patch("cumulus_etl.deid.codebook.secrets.token_hex", new=lambda x: "1234")

        # It's so common to want to see more than the tiny default fragment.
        # So we just enable this across the board.
        self.maxDiff = None

        # Make it easy to grab test data, regardless of where the test is
        self.datadir = os.path.join(os.path.dirname(__file__), "data")

        # Lock our version in place (it's referenced in some static files)
        self.patch("cumulus_etl.__version__", new="1.0.0+test")

        # Avoid long delays when testing networking errors
        self.sleep_mock = self.patch("asyncio.sleep")

        # Several tests involve timestamps in some form, so just pick a standard time for all tests.
        traveller = time_machine.travel(_FROZEN_TIME, tick=False)
        self.addCleanup(traveller.stop)
        self.time_machine = traveller.start()

    def make_tempdir(self) -> str:
        """Creates a temporary dir that will be automatically cleaned up"""
        tempdir = tempfile.TemporaryDirectory()
        self.addCleanup(tempdir.cleanup)
        return tempdir.name

    def patch(self, *args, **kwargs) -> mock.Mock:
        """Syntactic sugar to ease making a mock over a test's lifecycle, without decorators"""
        patcher = mock.patch(*args, **kwargs)
        self.addCleanup(patcher.stop)
        return patcher.start()

    def patch_dict(self, *args, **kwargs) -> mock.Mock:
        """Syntactic sugar for making a dict mock over a test's lifecycle, without decorators"""
        patcher = mock.patch.dict(*args, **kwargs)
        self.addCleanup(patcher.stop)
        return patcher.start()

    def patch_object(self, *args, **kwargs) -> mock.Mock:
        """Syntactic sugar for making an object mock over a test's lifecycle, without decorators"""
        patcher = mock.patch.object(*args, **kwargs)
        self.addCleanup(patcher.stop)
        return patcher.start()

    @contextlib.contextmanager
    def assert_fatal_exit(self, code: int | None = None):
        with self.assertRaises(SystemExit) as cm:
            yield
        if code is not None:
            self.assertEqual(cm.exception.code, code)

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

        This seems to be some version of https://github.com/python/cpython/issues/83282
        but fixed & never backported.
        I was not able to find a separate bug report for this specific issue.

        Given the following two test methods (for Pythons before 3.11),
        only the second one will hang:

            async def test_fails_correctly(self):
                raise BaseException("OK")
            async def test_hangs_forever(self):
                raise SystemExit("Nope")

        This class works around that by wrapping all test methods and translating uncaught
        SystemExits into failures.
        _callTestMethod() can be deleted once we no longer use python 3.10 in our testing suite.
        """
        return super()._callTestMethod(functools.partial(self._catch_system_exit, method))


class TreeCompareMixin(unittest.TestCase):
    """Mixin that provides a simple way to diff two trees of files"""

    def setUp(self):
        super().setUp()

        filecmp.clear_cache()

        # you'll always want this when debugging
        self.maxDiff = None

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
        if left_path.endswith(".json") or left_path.endswith(".meta"):
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


class FhirClientMixin(unittest.TestCase):
    """Mixin that provides a realistic FhirClient"""

    def setUp(self):
        super().setUp()

        self.fhir_base = "http://localhost:9999/fhir"
        self.fhir_url = f"{self.fhir_base}/Group/MyGroup"
        self.fhir_client_id = "test-client-id"
        self.fhir_bearer = "1234567890"  # the provided oauth bearer token

        jwk_token = jwk.JWK.generate(
            kty="EC", alg="ES384", curve="P-384", kid="a", key_ops=["sign", "verify"]
        ).export(as_dict=True)
        self.fhir_jwks = {"keys": [jwk_token]}

        self._fhir_jwks_file = tempfile.NamedTemporaryFile(suffix=".jwks")
        self._fhir_jwks_file.write(json.dumps(self.fhir_jwks).encode("utf8"))
        self._fhir_jwks_file.flush()
        self.addCleanup(self._fhir_jwks_file.close)
        self.fhir_jwks_path = self._fhir_jwks_file.name

        # Do not unset assert_all_called - existing tests rely on it
        self.respx_mock = respx.MockRouter(assert_all_called=True)
        self.addCleanup(self.respx_mock.stop)
        self.respx_mock.start()

        self.mock_fhir_auth()

    def mock_fhir_auth(self) -> None:
        # /metadata
        self.respx_mock.get(
            f"{self.fhir_base}/metadata",
        ).respond(
            json={
                "fhirVersion": "4.0.1",
                "software": {
                    "name": "Test",
                    "version": "0.git",
                    "releaseDate": "today",
                },
            }
        )

        # /.well-known/smart-configuration
        self.respx_mock.get(
            f"{self.fhir_base}/.well-known/smart-configuration",
            headers={"Accept": "application/json"},
        ).respond(
            json={
                "capabilities": ["client-confidential-asymmetric"],
                "token_endpoint": f"{self.fhir_base}/token",
                "token_endpoint_auth_methods_supported": ["private_key_jwt"],
            },
        )

        # /token
        self.respx_mock.post(
            f"{self.fhir_base}/token",
        ).respond(
            json={
                "access_token": self.fhir_bearer,
            },
        )

    def fhir_client(self, resources: list[str]) -> fhir.FhirClient:
        return fhir.FhirClient(
            self.fhir_base,
            resources,
            smart_client_id=self.fhir_client_id,
            smart_jwks=self.fhir_jwks,
        )


def make_response(
    status_code=200, json_payload=None, text=None, reason=None, headers=None, stream=False
):
    """
    Makes a fake respx response for ease of testing.

    Usually you'll want to use respx.get(...) etc directly.
    But if you want to mock out the client <-> server interaction entirely,
    you can use this method to fake a Response object from a method that returns one.

    Example:
        server.request.return_value = make_response()
    """
    headers = dict(headers or {})
    headers.setdefault(
        "Content-Type", "application/json" if json_payload else "text/plain; charset=utf-8"
    )
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


def read_delta_lake(lake_path: str, *, version: int | None = None) -> list[dict]:
    """
    Reads in a delta lake folder at a certain time, sorted by id.

    DeltaLakeFormat.initialize_class() must have already been called.

    Compare the results to a pyarrow table with table.to_pylist() or just as a list of dicts.
    """
    # Read spark table
    reader = DeltaLakeFormat.spark.read
    if version is not None:
        reader = reader.option("versionAsOf", version)

    table_spark = reader.format("delta").load(lake_path)

    # Convert the spark table to Python primitives.
    # Going to rdd or pandas and then to Python keeps inserting spark-specific constructs like
    # Row(). So instead, convert to a JSON string and then back to Python.
    rows = [json.loads(row) for row in table_spark.toJSON().collect()]

    # Try to sort by id, but if that doesn't exist (which happens for some completion tables),
    # just use all dict values as a sort key.
    return sorted(rows, key=lambda x: x.get("id", sorted(x.items())))


@contextlib.contextmanager
def time_it(desc: str | None = None):
    """Tiny little timer context manager that is useful when debugging"""
    start = time.perf_counter()
    yield
    end = time.perf_counter()
    suffix = f" ({desc})" if desc else ""
    print(f"TIME IT: {end - start:.2f}s{suffix}")


@contextlib.contextmanager
def mem_it(desc: str | None = None):
    """Tiny little context manager to measure memory usage"""
    start_tracing = not tracemalloc.is_tracing()
    if start_tracing:
        tracemalloc.start()

    before, before_peak = tracemalloc.get_traced_memory()
    yield
    after, after_peak = tracemalloc.get_traced_memory()

    if start_tracing:
        tracemalloc.stop()

    suffix = f" ({desc})" if desc else ""
    if after_peak > before_peak:
        suffix = f"{suffix} ({after_peak - before_peak:,} PEAK change)"
    print(f"MEM IT: {after - before:,}{suffix}")
