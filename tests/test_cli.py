"""Tests for cli.py"""

import contextlib
import io
from unittest import mock

import ddt

from cumulus_etl import cli
from tests.utils import AsyncTestCase


@ddt.ddt
class TestCumulusCLI(AsyncTestCase):
    """
    Unit tests for our toplevel command line interface.
    """

    @ddt.data(
        ([], "usage: cumulus-etl [OPTION]..."),
        (["chart-review"], "usage: cumulus-etl upload-notes [OPTION]..."),
        (["convert"], "usage: cumulus-etl convert [OPTION]..."),
        (["etl"], "usage: cumulus-etl etl [OPTION]..."),
        (["upload-notes"], "usage: cumulus-etl upload-notes [OPTION]..."),
    )
    @ddt.unpack
    async def test_usage(self, argv, expected_usage):
        """Verify that we print the right usage for the various subcommands"""
        stdout = io.StringIO()
        with contextlib.redirect_stdout(stdout):
            with self.assertRaises(SystemExit):
                await cli.main(argv + ["--help"])

        self.assertTrue(stdout.getvalue().startswith(expected_usage), stdout.getvalue())

    @ddt.data(
        ([], "cumulus_etl.etl.run_etl"),
        (["chart-review"], "cumulus_etl.upload_notes.run_upload_notes"),
        (["convert"], "cumulus_etl.etl.convert.run_convert"),
        (["etl"], "cumulus_etl.etl.run_etl"),
        (["upload-notes"], "cumulus_etl.upload_notes.run_upload_notes"),
    )
    @ddt.unpack
    async def test_routing(self, argv, main_method):
        """Verify that we print the right usage for the various subcommands"""
        with mock.patch(main_method) as mock_main:
            await cli.main(argv)

        self.assertTrue(mock_main.called)
