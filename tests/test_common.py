"""Tests for common.py"""

import tempfile

import cumulus_fhir_support as cfs
import ddt

from cumulus_etl import common
from tests import utils


@ddt.ddt
class TestHelpers(utils.AsyncTestCase):
    """
    Test case for common helper methods.
    """

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
class TestIOUtils(utils.AsyncTestCase):
    """
    Tests for our read/write helper methods.
    """

    @ddt.data(
        ("1", 1),
        ("1\n2\n", 2),
        ("1\r\n2\r\n3\r\n", 3),
    )
    @ddt.unpack
    def test_read_local_line_count(self, contents, expected_length):
        with tempfile.NamedTemporaryFile() as tmpfile:
            cfs.FsPath(tmpfile.name).write_text(contents)
            found_length = common.read_local_line_count(tmpfile.name)
        self.assertEqual(expected_length, found_length)
