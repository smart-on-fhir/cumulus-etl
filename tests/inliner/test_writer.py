"""Tests for inliner/writer.py"""

import cumulus_fhir_support

from cumulus_etl.inliner import writer
from tests import utils


class TestWriter(utils.AsyncTestCase):
    def test_basic_writing(self):
        tmpdir = self.make_tempdir()
        with writer.OrderedNdjsonWriter(f"{tmpdir}/test.ndjson") as ordered_writer:
            ordered_writer.write(1, {"id": "one"})
            ordered_writer.write(0, {"id": "zero"})
            ordered_writer.write(5, {"id": "five"})
            ordered_writer.write(4, {"id": "four"})
            ordered_writer.write(2, {"id": "two"})
            ordered_writer.write(3, {"id": "three"})

        rows = list(cumulus_fhir_support.read_multiline_json(f"{tmpdir}/test.ndjson"))
        self.assertEqual(
            rows,
            [
                {"id": "zero"},
                {"id": "one"},
                {"id": "two"},
                {"id": "three"},
                {"id": "four"},
                {"id": "five"},
            ],
        )
