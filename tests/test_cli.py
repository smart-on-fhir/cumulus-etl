"""Tests for cli.py and cli_utils.py"""

import contextlib
import io
import socket
from unittest import mock

import ddt

from cumulus_etl import cli, cli_utils
from tests.utils import AsyncTestCase


@ddt.ddt
class TestCumulusCLI(AsyncTestCase):
    """
    Unit tests for our toplevel command line interface.
    """

    @ddt.data(
        ([], "usage: cumulus-etl [OPTION]..."),
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
                await cli.main([*argv, "--help"])

        self.assertTrue(stdout.getvalue().startswith(expected_usage), stdout.getvalue())

    @ddt.data(
        ([], "cumulus_etl.etl.run_etl"),
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


class TestCumulusCLIUtils(AsyncTestCase):
    """
    Unit tests for our CLI helpers.
    """

    async def test_url_is_unresolvable(self):
        """Verify that a hostname that doesn't exist is immediately failed"""
        # First test the plumbing is real and work
        self.assertFalse(cli_utils.is_url_available("http://nope.invalid"))

        # Now mock the plumbing to confirm we don't retry
        with mock.patch("socket.create_connection") as mock_conn:
            mock_conn.side_effect = socket.gaierror
            self.assertFalse(cli_utils.is_url_available("http://nope.invalid"))
            self.assertEqual(mock_conn.call_count, 1)

    @mock.patch("socket.create_connection", side_effect=ConnectionRefusedError)
    @mock.patch("time.sleep", new=lambda x: None)  # don't sleep during retries
    async def test_url_is_not_ready(self, mock_conn):
        """Verify that a refused connection is retried"""
        self.assertFalse(cli_utils.is_url_available("http://nope.invalid"))
        self.assertEqual(mock_conn.call_count, 6)
