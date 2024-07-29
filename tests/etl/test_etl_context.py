"""Tests for etl/context.py"""

import datetime
import json
import tempfile

from cumulus_etl.etl.context import JobContext
from tests import utils


class TestJobContext(utils.AsyncTestCase):
    """Test case for JobContext"""

    def test_missing_file_context(self):
        context = JobContext("nope")
        self.assertEqual({}, context.as_json())

    def test_save_and_load(self):
        with tempfile.NamedTemporaryFile(mode="w+") as f:
            json.dump(
                {
                    "last_successful_input_dir": "/input/dir",
                },
                f,
            )
            f.flush()

            context = JobContext(f.name)
            self.assertEqual(
                {
                    "last_successful_input_dir": "/input/dir",
                },
                context.as_json(),
            )

            context.last_successful_datetime = datetime.datetime(
                2008, 5, 1, 14, 30, 30, tzinfo=datetime.timezone.utc
            )
            self.assertEqual(
                {
                    "last_successful_datetime": "2008-05-01T14:30:30+00:00",
                    "last_successful_input_dir": "/input/dir",
                },
                context.as_json(),
            )

            context.save()
            context2 = JobContext(f.name)
            self.assertEqual(context.as_json(), context2.as_json())

    def test_last_successful_props(self):
        context = JobContext("nope")
        context.last_successful_datetime = datetime.datetime(
            2008, 5, 1, 14, 30, 30, tzinfo=datetime.timezone.utc
        )
        context.last_successful_input_dir = "/input"
        context.last_successful_output_dir = "/output"
        self.assertEqual(
            {
                "last_successful_datetime": "2008-05-01T14:30:30+00:00",
                "last_successful_input_dir": "/input",
                "last_successful_output_dir": "/output",
            },
            context.as_json(),
        )
