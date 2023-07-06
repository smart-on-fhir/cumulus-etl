"""Tests for common.py"""
import contextlib
import io
import os
import tempfile
from unittest import mock

import ddt
import fsspec
from fsspec.implementations.local import LocalFileOpener

from cumulus_etl import common
from tests import utils


@ddt.ddt
class TestLogging(utils.AsyncTestCase):
    """
    Test case for common logging methods.
    """

    @ddt.data(
        (0, "0KB"),
        (2000, "2KB"),
        (2000000, "1.9MB"),
        (2000000000, "1.9GB"),
    )
    @ddt.unpack
    def test_human_file_size(self, byte_count, expected_str):
        """Verify human_file_size works correctly"""
        self.assertEqual(expected_str, common.human_file_size(byte_count))

    @ddt.data(
        (0, "0s"),
        (59, "59s"),
        (60, "1m"),
        (9010, "2.5h"),
    )
    @ddt.unpack
    def test_human_time_offset(self, seconds, expected_str):
        """Verify human_time_offset works correctly"""
        self.assertEqual(expected_str, common.human_time_offset(seconds))


@ddt.ddt
class TestIOUtils(utils.AsyncTestCase):
    """Tests for our read/write helper methods"""

    @contextlib.contextmanager
    def exploding_text(self):
        """Yields text data that when fed to S3 writes, will explode after some but not all data has been uploaded"""
        orig_write = LocalFileOpener.write

        def exploding_write(*args, **kwargs):
            orig_write(*args, **kwargs)
            raise KeyboardInterrupt

        with mock.patch("fsspec.implementations.local.LocalFileOpener.write", new=exploding_write):
            with self.assertRaises(KeyboardInterrupt):
                yield "1" * io.DEFAULT_BUFFER_SIZE

    async def test_writes_are_atomic(self):
        """Verify that our write utilities are atomic."""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Try a couple of our write methods, confirm that nothing makes it through
            with self.exploding_text() as text:
                common.write_text(f"{tmpdir}/atomic.txt", text)
            with self.exploding_text() as text:
                common.write_json(f"{tmpdir}/atomic.json", {"hello": text})
            self.assertEqual([], os.listdir(tmpdir))

            # By default, fsspec writes are not atomic - just sanity check that text _can_ get through exploding_text
            with self.exploding_text() as text:
                with fsspec.open(f"{tmpdir}/partial.txt", "w") as f:
                    f.write(text)
            self.assertEqual(["partial.txt"], os.listdir(tmpdir))
