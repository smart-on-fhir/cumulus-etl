"""Tests for common.py"""

import ddt

from cumulus import common
from tests.utils import AsyncTestCase


@ddt.ddt
class TestLogging(AsyncTestCase):
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
