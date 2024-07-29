"""Tests for oracle connections"""

import os
import unittest
from unittest import mock

import ddt

from cumulus_etl import errors
from cumulus_etl.loaders.i2b2.oracle import connect
from tests import utils


@ddt.ddt
class TestOracleConnect(utils.AsyncTestCase):
    """Test case for connecting to oracle"""

    @ddt.data(None, "")
    def test_username_required(self, username):
        """Verify that a missing password will exit the program"""
        env = {"CUMULUS_SQL_PASSWORD": "p4sswd"}
        if username is not None:
            env["CUMULUS_SQL_USER"] = username

        with mock.patch.dict(os.environ, env, clear=True):
            with self.assertRaises(SystemExit) as cm:
                connect.connect("foo")
            self.assertEqual(errors.SQL_USER_MISSING, cm.exception.code)

    @ddt.data(None, "")
    def test_password_required(self, password):
        """Verify that a missing password will exit the program"""
        env = {"CUMULUS_SQL_USER": "test-user"}
        if password is not None:
            env["CUMULUS_SQL_PASSWORD"] = password

        with mock.patch.dict(os.environ, env, clear=True):
            with self.assertRaises(SystemExit) as cm:
                connect.connect("foo")
            self.assertEqual(errors.SQL_PASSWORD_MISSING, cm.exception.code)

    @mock.patch("cumulus_etl.loaders.i2b2.oracle.connect.oracledb")
    @mock.patch.dict(
        os.environ, {"CUMULUS_SQL_USER": "test-user", "CUMULUS_SQL_PASSWORD": "p4sswd"}
    )
    def test_connect(self, mock_oracledb):
        """Verify that we pass all the right parameters to Oracle when connecting"""
        connect.connect("tcp://localhost/foo")
        self.assertEqual(
            [mock.call(dsn="tcp://localhost/foo", user="test-user", password="p4sswd")],
            mock_oracledb.connect.call_args_list,
        )


if __name__ == "__main__":
    unittest.main()
