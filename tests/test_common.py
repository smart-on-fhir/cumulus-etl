"""Tests for common.py"""

import contextlib
import itertools
import tempfile
from unittest import mock

import ddt
import fsspec
import s3fs

from cumulus_etl import common
from tests import s3mock, utils


@ddt.ddt
class TestHelpers(utils.AsyncTestCase):
    """
    Test case for common helper methods.
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

    def test_temp_dir_requires_init(self):
        common.set_global_temp_dir(None)  # reset the global temp dir
        with self.assertRaisesRegex(ValueError, "No temporary directory was created yet"):
            common.get_temp_dir("blarg")


@ddt.ddt
class TestIOUtils(s3mock.S3Mixin, utils.AsyncTestCase):
    """
    Tests for our read/write helper methods.

    Mostly against S3 because that's such an important target FS and s3fs might have its own set of bugs/behavior.
    """

    @contextlib.contextmanager
    def exploding_text(self):
        """Yields text data that when fed to S3 writes, will explode after some but not all data has been uploaded"""
        orig_write = s3fs.core.S3File.write

        def exploding_write(*args, **kwargs):
            orig_write(*args, **kwargs)
            raise KeyboardInterrupt

        with mock.patch("s3fs.core.S3File.write", new=exploding_write):
            with self.assertRaises(KeyboardInterrupt):
                yield "1" * (fsspec.spec.AbstractBufferedFile.DEFAULT_BLOCK_SIZE + 1)

    def test_writes_are_atomic(self):
        """Verify that our write utilities are atomic."""
        # Try a couple of our write methods, confirm that nothing makes it through
        with self.exploding_text() as text:
            common.write_text(f"{self.bucket_url}/atomic.txt", text)
        with self.exploding_text() as text:
            common.write_json(f"{self.bucket_url}/atomic.json", {"hello": text})
        self.assertEqual([], self.s3fs.ls(self.bucket_url, detail=False))

        # By default, fsspec writes are not atomic - just sanity check that text _can_ get through exploding_text
        with self.exploding_text() as text:
            with fsspec.open(
                f"{self.bucket_url}/partial.txt", "w", endpoint_url=s3mock.S3Mixin.ENDPOINT_URL
            ) as f:
                f.write(text)
        self.assertEqual(
            [f"{self.bucket}/partial.txt"], self.s3fs.ls(self.bucket_url, detail=False)
        )

    @ddt.idata(
        # Every combination of these sizes, backends, and data formats:
        itertools.product(
            [5, fsspec.spec.AbstractBufferedFile.DEFAULT_BLOCK_SIZE + 1],
            ["local", "s3"],
            ["json", "text"],
        )
    )
    @ddt.unpack
    def test_writes_happy_path(self, size, backend, data_format):
        """
        Verify that writes of various sizes and formats are written out correctly.

        This may seem paranoid, but we've seen S3FS not write them out inside a transaction,
        because we forgot to close or flush the file.
        """
        match data_format:
            case "text":
                write = common.write_text
                read = common.read_text
                data = "1" * size
            case "json":
                write = common.write_json
                read = common.read_json
                data = ["1" * size]
            case _:
                raise ValueError

        with tempfile.TemporaryDirectory() as tmpdir:
            match backend:
                case "local":
                    directory = tmpdir
                case "s3":
                    directory = self.bucket_url
                case _:
                    raise ValueError

            write(f"{directory}/file.txt", data)
            result = read(f"{directory}/file.txt")

        self.assertEqual(data, result)

    @ddt.data(
        ("1", 1),
        ("1\n2\n", 2),
        ("1\r\n2\r\n3\r\n", 3),
    )
    @ddt.unpack
    def test_read_local_line_count(self, contents, expected_length):
        with tempfile.NamedTemporaryFile() as tmpfile:
            common.write_text(tmpfile.name, contents)
            found_length = common.read_local_line_count(tmpfile.name)
        self.assertEqual(expected_length, found_length)
