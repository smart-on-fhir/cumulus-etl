"""Tests for etl/config.py"""

import unittest
from socket import gethostname

import freezegun

from cumulus.etl import config


class TestConfigSummary(unittest.TestCase):
    """Test case for JobSummary"""

    @freezegun.freeze_time("Sep 15th, 2021 1:23:45")
    def test_empty_summary(self):
        summary = config.JobSummary("empty")
        expected = {
            "attempt": 0,
            "hostname": gethostname(),
            "label": "empty",
            "success": 0,
            "success_rate": 1.0,
            "timestamp": "2021-09-15 01:23:45",
        }
        self.assertEqual(expected, summary.as_json())
