"""Tests for inliner/cli.py"""

import ddt

from cumulus_etl import cli, errors, fhir, store
from tests import utils


@ddt.ddt
class TestInlinerCli(utils.AsyncTestCase, utils.FhirClientMixin):
    """Tests for the inline CLI"""

    def setUp(self):
        super().setUp()
        self.inliner = self.patch("cumulus_etl.inliner.inliner")

    async def run_inline(self, *args) -> None:
        await cli.main(
            [
                "inline",
                "/bogus/path",
                self.fhir_url,
                f"--smart-client-id={self.fhir_client_id}",
                f"--smart-key={self.fhir_jwks_path}",
                *args,
            ]
        )

    async def test_args_pass_down(self):
        await self.run_inline(
            "--resource=diagnosticreport",
            "--mimetype=a",
            "--mimetype=B,C",
        )
        self.assertEqual(self.inliner.call_count, 1)
        self.assertIsInstance(self.inliner.call_args[0][0], fhir.FhirClient)
        self.assertEqual(self.inliner.call_args[0][1].path, "/bogus/path")
        self.assertEqual(self.inliner.call_args[0][2], {"DiagnosticReport"})
        self.assertEqual(self.inliner.call_args[0][3], {"a", "b", "c"})

    @ddt.data(
        ("--resource=diagnosticreport,DOCUMENTREFERENCE",),
        ("--resource=diagnosticreport", "--resource=DOCUMENTREFERENCE"),
    )
    async def test_resources_combined(self, args):
        await self.run_inline(*args)
        self.assertEqual(self.inliner.call_count, 1)
        self.assertEqual(self.inliner.call_args[0][2], {"DiagnosticReport", "DocumentReference"})

    async def test_defaults(self):
        await self.run_inline()
        self.assertEqual(self.inliner.call_count, 1)
        self.assertEqual(self.inliner.call_args[0][2], {"DiagnosticReport", "DocumentReference"})
        self.assertEqual(
            self.inliner.call_args[0][3], {"text/plain", "text/html", "application/xhtml+xml"}
        )

    async def test_sets_fs_options(self):
        await self.run_inline("--s3-region=us-west-1")
        self.assertEqual(
            store.get_fs_options("s3"),
            {
                "client_kwargs": {"region_name": "us-west-1"},
                "s3_additional_kwargs": {"ServerSideEncryption": "aws:kms"},
            },
        )

    async def test_bad_resource(self):
        self.respx_mock.stop(quiet=True)  # silence "not all mocks called"
        with self.assert_fatal_exit(errors.ARGS_INVALID):
            await self.run_inline("--resource=patient")

    async def test_bad_url(self):
        self.respx_mock.stop(quiet=True)  # silence "not all mocks called"
        with self.assert_fatal_exit(errors.ARGS_INVALID):
            await cli.main(
                [
                    "inline",
                    "/bogus/path",
                    "/bogus/url",
                ]
            )
