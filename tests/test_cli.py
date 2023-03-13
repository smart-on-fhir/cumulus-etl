"""Tests for cli.py"""

import contextlib
import io
import unittest
from unittest import mock

import ddt

from cumulus import cli


@ddt.ddt
class TestCumulusCLI(unittest.IsolatedAsyncioTestCase):
    """
    Unit tests for our toplevel command line interface.
    """

    @ddt.data(
        ([], "usage: cumulus-etl [OPTION]..."),
        (["etl"], "usage: cumulus-etl etl [OPTION]..."),
        (["chart-review"], "usage: cumulus-etl chart-review [OPTION]..."),
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
        ([], "cumulus.cli.run_etl"),
        (["etl"], "cumulus.cli.run_etl"),
        (["chart-review"], "cumulus.cli.run_chart_review"),
    )
    @ddt.unpack
    async def test_routing(self, argv, main_method):
        """Verify that we print the right usage for the various subcommands"""
        with mock.patch(main_method) as mock_main:
            await cli.main(argv)

        self.assertTrue(mock_main.called)
