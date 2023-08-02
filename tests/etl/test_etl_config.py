"""Tests for etl/config.py"""

from socket import gethostname

from cumulus_etl.etl import config
from tests.utils import FROZEN_TIME_UTC, AsyncTestCase


class TestConfigSummary(AsyncTestCase):
    """Test case for JobSummary"""

    def test_empty_summary(self):
        summary = config.JobSummary("empty")
        expected = {
            "attempt": 0,
            "had_errors": False,
            "hostname": gethostname(),
            "label": "empty",
            "success": 0,
            "success_rate": 1.0,
            "timestamp": FROZEN_TIME_UTC.strftime("%Y-%m-%d %H:%M:%S"),
        }
        self.assertEqual(expected, summary.as_json())
