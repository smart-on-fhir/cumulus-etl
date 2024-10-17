"""Tests for bulk export log parsing"""

import datetime
import tempfile

import ddt

from cumulus_etl import common, store
from cumulus_etl.loaders.fhir.export_log import BulkExportLogParser
from tests.utils import AsyncTestCase


def kickoff(group: str, export_id: str = "test-export") -> dict:
    url = f"https://host/Group/{group}" if group else "https://host/"
    return {
        "eventId": "kickoff",
        "exportId": export_id,
        "eventDetail": {
            "exportUrl": url,
        },
    }


def status_complete(timestamp: str, export_id: str = "test-export") -> dict:
    return {
        "eventId": "status_complete",
        "exportId": export_id,
        "eventDetail": {
            "transactionTime": timestamp,
        },
    }


@ddt.ddt
class TestBulkExportLogParser(AsyncTestCase):
    """Test case for parsing bulk export logs."""

    def _assert_results(self, path, expected_result) -> None:
        if isinstance(expected_result, tuple):
            parser = BulkExportLogParser(store.Root(path))
            expected_group = expected_result[0]
            expected_datetime = datetime.datetime.fromisoformat(expected_result[1])
            self.assertEqual(expected_group, parser.group_name)
            self.assertEqual(expected_datetime, parser.export_datetime)
        else:
            with self.assertRaises(expected_result):
                BulkExportLogParser(store.Root(path))

    @ddt.data(
        # Happy cases:
        (["log.ndjson"], None),
        (["log.blarg.ndjson"], None),
        (["log.0001.ndjson"], None),
        (["log.ndjson", "log.1.ndjson"], None),
        # Error cases:
        ([], BulkExportLogParser.NoLogs),
        (["log.1.ndjson", "log.2.ndjson"], BulkExportLogParser.MultipleLogs),
    )
    @ddt.unpack
    def test_finding_the_log(self, files, error):
        with tempfile.TemporaryDirectory() as tmpdir:
            common.write_text(f"{tmpdir}/distraction.txt", "hello")
            common.write_text(f"{tmpdir}/log.ndjson.bak", "bye")
            for file in files:
                with common.NdjsonWriter(f"{tmpdir}/{file}") as writer:
                    writer.write(kickoff("G"))
                    writer.write(status_complete("2020-10-17"))

            error = error or ("G", "2020-10-17")
            self._assert_results(tmpdir, error)

    def test_no_dir(self):
        self._assert_results("/path/does/not/exist", BulkExportLogParser.NoLogs)

    @ddt.data(
        # Happy cases:
        (  # basic simple case
            [kickoff("G"), status_complete("2020-10-17")],
            ("G", "2020-10-17"),
        ),
        (  # multiple rows - we should pick last events for the last kickoff
            [
                kickoff("1st", export_id="1st"),
                kickoff("2nd", export_id="2nd"),
                # shouldn't be two status completes, but just in case there are, we grab last
                status_complete("2002-02-01", export_id="2nd"),
                status_complete("2002-02-02", export_id="2nd"),
                status_complete("2001-01-01", export_id="1st"),
            ],
            ("2nd", "2002-02-02"),
        ),
        ([kickoff(""), status_complete("2020-10-17")], ("", "2020-10-17")),  # global export group
        # Error cases:
        ([status_complete("2010-03-09")], BulkExportLogParser.IncompleteLog),  # missing group
        ([kickoff("G")], BulkExportLogParser.IncompleteLog),  # missing time
        ([], BulkExportLogParser.IncompleteLog),  # missing all
        (  # missing eventDetail
            [{"eventId": "kickoff", "exportId": "test"}],
            BulkExportLogParser.IncompleteLog,
        ),
        (  # missing transactionTime
            [{"eventId": "status_complete", "eventDetail": {}}],
            BulkExportLogParser.IncompleteLog,
        ),
    )
    @ddt.unpack
    def test_parsing(self, rows, expected_result):
        with tempfile.TemporaryDirectory() as tmpdir:
            with common.NdjsonWriter(f"{tmpdir}/log.ndjson", allow_empty=True) as writer:
                for row in rows:
                    writer.write(row)

            self._assert_results(tmpdir, expected_result)
