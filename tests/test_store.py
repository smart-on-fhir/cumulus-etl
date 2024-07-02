"""Tests for store.py"""

import ddt

from cumulus_etl import store
from tests.utils import AsyncTestCase


@ddt.ddt
class TestRoot(AsyncTestCase):
    """Test case for the Root filesystem abstraction class."""

    def setUp(self):
        super().setUp()
        self.fs_mock = self.patch("cumulus_etl.store.fsspec.filesystem")()

    def test_file_protocol(self):
        """Verify that we set a protocol of file:// for local paths"""
        self.assertEqual("file", store.Root("/").protocol)
