"""Tests for store.py"""

import ddt

from cumulus import store
from tests.utils import AsyncTestCase


@ddt.ddt
class TestRoot(AsyncTestCase):
    """Test case for the Root filesystem abstraction class."""

    def setUp(self):
        super().setUp()
        self.fs_mock = self.patch("cumulus.store.fsspec.filesystem")()

    @ddt.data(
        # root, ls return, expected value
        ("s3://bucket", ["bucket/file.txt"], ["s3://bucket/file.txt"]),  # standard prefixing
        ("s3://bucket", ["s3://bucket/file.txt"], ["s3://bucket/file.txt"]),  # we won't double-prefix
        ("/local/path", ["bucket/file.txt"], ["bucket/file.txt"]),  # we'll leave local paths alone
        ("s3://bucket", [], []),  # we'll gracefully handle empty lists
    )
    @ddt.unpack
    def test_ls_guaranteed_prefix(self, root_path, ls_return, expected):
        """Verify that we correctly guarantee that ls() filenames will have a protocol prefix"""
        self.fs_mock.ls.return_value = ls_return
        self.assertEqual(expected, store.Root(root_path).ls())
