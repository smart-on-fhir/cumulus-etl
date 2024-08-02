"""Tests for export/cli.py"""

from unittest import mock

import ddt

from cumulus_etl import cli
from cumulus_etl.etl.tasks.task_factory import get_default_tasks
from tests.utils import AsyncTestCase


@ddt.ddt
class TestExportCLI(AsyncTestCase):
    """Tests for high-level export support."""

    def setUp(self):
        super().setUp()
        self.loader_mock = mock.AsyncMock()
        self.loader_init_mock = self.patch(
            "cumulus_etl.loaders.FhirNdjsonLoader", return_value=self.loader_mock
        )
        self.client_mock = self.patch("cumulus_etl.fhir.create_fhir_client_for_cli")

    async def run_export(self, *args) -> None:
        await cli.main(["export", "https://example.com", "fake/path", *args])

    @ddt.data(
        ([], True),
        (["--task=patient"], False),
        (["--task-filter=gpu"], False),
    )
    @ddt.unpack
    async def test_prefer_url_resources(self, args, expected_prefer):
        """Verify that if no task filtering is done, we flag to prefer the url's _type"""
        await self.run_export(*args)
        self.assertEqual(
            expected_prefer,
            self.loader_mock.load_from_bulk_export.call_args.kwargs["prefer_url_resources"],
        )

    @ddt.data(
        ([], ["*default*"]),  # special value that the test will expand
        (["--task=patient,condition"], ["Condition", "Patient"]),
        (["--task-filter=covid_symptom"], ["DocumentReference"]),
    )
    @ddt.unpack
    async def test_task_selection(self, args, expected_resources):
        """Verify that we do the expected task filtering as requested"""
        await self.run_export(*args)
        if expected_resources == ["*default*"]:
            expected_resources = sorted(t.resource for t in get_default_tasks())
        self.assertEqual(
            expected_resources,
            self.loader_mock.load_from_bulk_export.call_args.args[0],
        )

    async def test_arg_passthrough(self):
        """Verify that we accept and send down all our different args"""
        await self.run_export(
            "--since=1920",
            "--until=1923",
            "--smart-client-id=ID",
            "--smart-jwks=jwks.json",
            "--basic-user=alice",
            "--basic-passwd=passwd.txt",
            "--bearer-token=token.txt",
            "--resume=my-url",
        )
        # built-in positional args
        self.assertEqual("https://example.com", self.loader_init_mock.call_args.args[0].path)
        self.assertEqual("fake/path", self.loader_init_mock.call_args.kwargs["export_to"])
        # custom args from above
        self.assertEqual("1920", self.loader_init_mock.call_args.kwargs["since"])
        self.assertEqual("1923", self.loader_init_mock.call_args.kwargs["until"])
        self.assertEqual("my-url", self.loader_init_mock.call_args.kwargs["resume"])
        self.assertEqual("ID", self.client_mock.call_args.args[0].smart_client_id)
        self.assertEqual("jwks.json", self.client_mock.call_args.args[0].smart_jwks)
        self.assertEqual("alice", self.client_mock.call_args.args[0].basic_user)
        self.assertEqual("passwd.txt", self.client_mock.call_args.args[0].basic_passwd)
        self.assertEqual("token.txt", self.client_mock.call_args.args[0].bearer_token)
