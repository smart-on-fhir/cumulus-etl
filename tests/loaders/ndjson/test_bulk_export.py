"""Tests for bulk export support"""

import contextlib
import datetime
import io
import json
import os
import tempfile
from unittest import mock

import cumulus_fhir_support
import ddt
import httpx
import respx

from cumulus_etl import cli, common, errors, store
from cumulus_etl.loaders.fhir.bulk_export import BulkExporter
from cumulus_etl.loaders.fhir.export_log import BulkExportLogParser, BulkExportLogWriter
from tests import utils


@ddt.ddt
@mock.patch("cumulus_etl.loaders.fhir.export_log.uuid.uuid4", new=lambda: "12345678")
class TestBulkExporter(utils.AsyncTestCase, utils.FhirClientMixin):
    """
    Test case for bulk export logic.

    i.e. tests for bulk_export.py
    """

    def setUp(self):
        super().setUp()
        self.tmpdir = self.make_tempdir()
        self.exporter = None

    async def export(self, **kwargs) -> None:
        resources = ["Condition", "Patient"]
        async with self.fhir_client(resources) as client:
            self.exporter = BulkExporter(client, resources, self.fhir_url, self.tmpdir, **kwargs)
            await self.exporter.export()

    def assert_log_equals(self, *rows, uuid_export_id: bool = False) -> None:
        found_rows = list(cumulus_fhir_support.read_multiline_json(f"{self.tmpdir}/log.ndjson"))

        # Do we use the same export ID throughout?
        all_export_ids = {x["exportId"] for x in found_rows}
        if uuid_export_id:
            self.assertEqual(all_export_ids, {"12345678"})  # from our uuid4 patch above
        else:
            self.assertEqual(all_export_ids, {"https://example.com/poll"})

        # Are timestamps increasing?
        all_timestamps = [x["timestamp"] for x in found_rows]
        self.assertListEqual(all_timestamps, sorted(all_timestamps))

        # Max one kickoff and one completion
        all_event_ids = [x["eventId"] for x in found_rows]
        self.assertLessEqual(all_event_ids.count("kickoff"), 1)
        self.assertLessEqual(all_event_ids.count("export_complete"), 1)

        # Verify that duration is correct
        if "export_complete" in all_event_ids:
            kickoff_index = all_event_ids.index("kickoff")
            complete_index = all_event_ids.index("export_complete")
            kickoff_timestamp = found_rows[kickoff_index]["timestamp"]
            complete_timestamp = found_rows[complete_index]["timestamp"]
            kickoff_datetime = datetime.datetime.fromisoformat(kickoff_timestamp)
            complete_datetime = datetime.datetime.fromisoformat(complete_timestamp)
            one_ms = datetime.timedelta(milliseconds=1)
            expected_duration = (complete_datetime - kickoff_datetime) // one_ms
            found_duration = found_rows[complete_index]["eventDetail"]["duration"]
            self.assertEqual(found_duration, expected_duration)

        # Confirm we add debugging info to the kickoff event
        kickoff_index = all_event_ids.index("kickoff")
        self.assertEqual(found_rows[kickoff_index]["_client"], "cumulus-etl")
        self.assertEqual(found_rows[kickoff_index]["_clientVersion"], "1.0.0+test")

        extracted_details = [(x["eventId"], x["eventDetail"]) for x in found_rows]

        # Match and reorder download requests/completes because those can be async/non-deterministic
        reordered_details = []
        completion = []
        downloads = {}
        for event_id, detail in extracted_details:
            if event_id == "download_request":
                event_list = downloads.setdefault(detail["fileUrl"], [])
                event_list.append((event_id, detail))
            elif event_id in ("download_complete", "download_error"):
                self.assertIn(detail["fileUrl"], downloads)
                downloads[detail["fileUrl"]].append((event_id, detail))
            elif event_id == "export_complete":
                completion.append((event_id, detail))
            else:
                reordered_details.append((event_id, detail))
        for file_url in sorted(downloads):
            reordered_details += downloads[file_url]
        reordered_details += completion

        self.assertEqual(len(reordered_details), len(rows), reordered_details)
        for index, row in enumerate(rows):
            self.assertEqual(reordered_details[index][0], row[0])
            if row[1] is not None:
                self.assertEqual(reordered_details[index][1], row[1])

    def mock_kickoff(
        self,
        params: str = "?_type=Condition%2CPatient",
        side_effect: list | None = None,
        **kwargs,
    ) -> None:
        kwargs.setdefault("status_code", 202)
        route = self.respx_mock.get(
            f"{self.fhir_url}/$export{params}",
            headers={"Prefer": "respond-async"},
        )
        if side_effect:
            route.side_effect = side_effect
        else:
            route.respond(
                headers={
                    "Content-Location": "https://example.com/poll",
                    "Vendor-Transaction-ID": "1234",
                },
                **kwargs,
            )

    def mock_delete(self, **kwargs) -> None:
        kwargs.setdefault("status_code", 202)
        self.respx_mock.delete("https://example.com/poll").respond(**kwargs)

    async def test_happy_path(self):
        """Verify an end-to-end bulk export with no problems and no waiting works as expected"""
        self.mock_kickoff()
        self.mock_delete()
        self.respx_mock.get(
            "https://example.com/poll",
            headers={"Accept": "application/json"},
        ).respond(
            json={
                "transactionTime": "2015-02-07T13:28:17.239+02:00",
                "output": [
                    {"type": "Condition", "url": "https://example.com/con1"},
                    {"type": "Condition", "url": "https://example.com/con2"},
                    {"type": "Patient", "url": "https://example.com/pat1"},
                ],
            },
        )
        con1 = json.dumps({"resourceType": "Condition", "id": "1"})
        self.respx_mock.get(
            "https://example.com/con1",
            headers={"Accept": "application/fhir+ndjson"},
        ).respond(text=con1)
        con2 = json.dumps({"resourceType": "Condition", "id": "2"})
        self.respx_mock.get(
            "https://example.com/con2",
            headers={"Accept": "application/fhir+ndjson"},
        ).respond(text=con2)
        pat1 = json.dumps({"resourceType": "Patient", "id": "P"})
        self.respx_mock.get(
            "https://example.com/pat1",
            headers={"Accept": "application/fhir+ndjson"},
        ).respond(text=pat1)

        await self.export()

        self.assertEqual("MyGroup", self.exporter.group_name)
        self.assertEqual(
            "2015-02-07T13:28:17.239000+02:00", self.exporter.export_datetime.isoformat()
        )

        # Ensure we can read back our own log and parse the above values too
        parser = BulkExportLogParser(store.Root(self.tmpdir))
        self.assertEqual("MyGroup", parser.group_name)
        self.assertEqual("2015-02-07T13:28:17.239000+02:00", parser.export_datetime.isoformat())

        self.assertEqual(
            {"resourceType": "Condition", "id": "1"},
            common.read_json(f"{self.tmpdir}/Condition.000.ndjson"),
        )
        self.assertEqual(
            {"resourceType": "Condition", "id": "2"},
            common.read_json(f"{self.tmpdir}/Condition.001.ndjson"),
        )
        self.assertEqual(
            {"resourceType": "Patient", "id": "P"},
            common.read_json(f"{self.tmpdir}/Patient.000.ndjson"),
        )

        self.assert_log_equals(
            (
                "kickoff",
                {
                    "exportUrl": f"{self.fhir_url}/$export?_type=Condition%2CPatient",
                    "softwareName": "Test",
                    "softwareVersion": "0.git",
                    "softwareReleaseDate": "today",
                    "fhirVersion": "4.0.1",
                    "requestParameters": {"_type": "Condition,Patient"},
                    "errorCode": None,
                    "errorBody": None,
                    "responseHeaders": {
                        "content-location": "https://example.com/poll",
                        "vendor-transaction-id": "1234",
                    },
                },
            ),
            (
                "status_complete",
                {
                    "transactionTime": "2015-02-07T13:28:17.239+02:00",
                },
            ),
            (
                "status_page_complete",
                {
                    "deletedFileCount": 0,
                    "errorFileCount": 0,
                    "outputFileCount": 3,
                    "transactionTime": "2015-02-07T13:28:17.239+02:00",
                },
            ),
            (
                "manifest_complete",
                {
                    "totalDeletedFileCount": 0,
                    "totalErrorFileCount": 0,
                    "totalOutputFileCount": 3,
                    "totalManifests": 1,
                    "transactionTime": "2015-02-07T13:28:17.239+02:00",
                },
            ),
            (
                "download_request",
                {
                    "fileUrl": "https://example.com/con1",
                    "itemType": "output",
                    "resourceType": "Condition",
                },
            ),
            (
                "download_complete",
                {"fileSize": len(con1), "fileUrl": "https://example.com/con1", "resourceCount": 1},
            ),
            (
                "download_request",
                {
                    "fileUrl": "https://example.com/con2",
                    "itemType": "output",
                    "resourceType": "Condition",
                },
            ),
            (
                "download_complete",
                {"fileSize": len(con2), "fileUrl": "https://example.com/con2", "resourceCount": 1},
            ),
            (
                "download_request",
                {
                    "fileUrl": "https://example.com/pat1",
                    "itemType": "output",
                    "resourceType": "Patient",
                },
            ),
            (
                "download_complete",
                {"fileSize": len(pat1), "fileUrl": "https://example.com/pat1", "resourceCount": 1},
            ),
            (
                "export_complete",
                {
                    "attachments": None,
                    "bytes": len(con1) + len(con2) + len(pat1),
                    "duration": 0,
                    "files": 3,
                    "resources": 3,
                },
            ),
        )

    async def test_since_until(self):
        """Verify that we send since & until parameters correctly to the server"""
        self.mock_kickoff(
            params="?_type=Condition%2CPatient&_since=2000-01-01T00%3A00%3A00%2B00.00&_until=2010",
            status_code=500,  # early exit
        )

        with self.assertRaises(errors.FatalError):
            await self.export(since="2000-01-01T00:00:00+00.00", until="2010")

    async def test_type_filter(self):
        self.mock_kickoff(
            params="?_type=Condition%2CPatient&"
            "_typeFilter=Patient%3Factive%3Dfalse%2CPatient%3Factive%3Dtrue",
            status_code=500,  # early exit
        )

        with self.assertRaises(errors.FatalError):
            await self.export(type_filter=["Patient?active=false", "Patient?active=true"])

    async def test_export_error(self):
        """Verify that we download and present any server-reported errors during the bulk export"""
        self.mock_kickoff()
        self.mock_delete()
        self.respx_mock.get(
            "https://example.com/poll",
            headers={"Accept": "application/json"},
        ).respond(
            json={
                "transactionTime": "bogus-time",  # just confirm we gracefully handle this
                "error": [
                    {"type": "OperationOutcome", "url": "https://example.com/err1"},
                    {"type": "OperationOutcome", "url": "https://example.com/err2"},
                ],
                "output": [  # include an output too, to confirm we do download it
                    {"type": "Condition", "url": "https://example.com/con1"},
                ],
            },
        )
        err1 = (
            '{"resourceType": "OperationOutcome",'
            ' "issue": [{"severity": "error", "diagnostics": "err1"}]}'
        )
        self.respx_mock.get(
            "https://example.com/err1",
            headers={"Accept": "application/fhir+ndjson"},
        ).respond(text=err1)
        err2 = (
            '{"resourceType": "OperationOutcome",'
            '"issue": [{"severity": "fatal", "details": {"text": "err2"}}]}\n'
            '{"resourceType": "OperationOutcome",'
            '"issue": [{"severity": "warning", "diagnostics": "warning1"}]}\n'
            '{"resourceType": "OperationOutcome",'
            '"issue": ['
            '{"severity": "error", "code": "err3"},'
            '{"severity": "fatal", "code": "err4"}'
            "]}\n"
        )
        self.respx_mock.get(
            "https://example.com/err2",
            headers={"Accept": "application/fhir+ndjson"},
        ).respond(text=err2)
        con1 = '{"resourceType": "Condition"}'
        self.respx_mock.get(
            "https://example.com/con1",
            headers={"Accept": "application/fhir+ndjson"},
        ).respond(text=con1)

        with self.assertRaisesRegex(
            errors.FatalError, "Errors occurred during export:\n - err1\n - err2\n - err3\n - err4"
        ):
            await self.export()

        self.assertIsNone(self.exporter.export_datetime)  # date time couldn't be parsed

        err1_file = common.read_text(f"{self.tmpdir}/error/OperationOutcome.000.ndjson")
        self.assertEqual(err1_file, err1)
        err2_file = common.read_text(f"{self.tmpdir}/error/OperationOutcome.001.ndjson")
        self.assertEqual(err2_file, err2)

        self.assert_log_equals(
            ("kickoff", None),
            (
                "status_complete",
                {
                    "transactionTime": "bogus-time",
                },
            ),
            (
                "status_page_complete",
                {
                    "deletedFileCount": 0,
                    "errorFileCount": 2,
                    "outputFileCount": 1,
                    "transactionTime": "bogus-time",
                },
            ),
            (
                "manifest_complete",
                {
                    "totalDeletedFileCount": 0,
                    "totalErrorFileCount": 2,
                    "totalOutputFileCount": 1,
                    "totalManifests": 1,
                    "transactionTime": "bogus-time",
                },
            ),
            (
                "download_request",
                {
                    "fileUrl": "https://example.com/con1",
                    "itemType": "output",
                    "resourceType": "Condition",
                },
            ),
            (
                "download_complete",
                {"fileSize": len(con1), "fileUrl": "https://example.com/con1", "resourceCount": 1},
            ),
            (
                "download_request",
                {
                    "fileUrl": "https://example.com/err1",
                    "itemType": "error",
                    "resourceType": "OperationOutcome",
                },
            ),
            (
                "download_complete",
                {"fileSize": len(err1), "fileUrl": "https://example.com/err1", "resourceCount": 1},
            ),
            (
                "download_request",
                {
                    "fileUrl": "https://example.com/err2",
                    "itemType": "error",
                    "resourceType": "OperationOutcome",
                },
            ),
            (
                "download_complete",
                {"fileSize": len(err2), "fileUrl": "https://example.com/err2", "resourceCount": 3},
            ),
            (
                "export_complete",
                {
                    "attachments": None,
                    "bytes": len(con1) + len(err1) + len(err2),
                    "duration": 0,
                    "files": 3,
                    "resources": 5,
                },
            ),
        )

    async def test_export_warning(self):
        """Verify that we download and present any server-reported warnings during the bulk export"""
        self.mock_kickoff()
        self.mock_delete()
        self.respx_mock.get("https://example.com/poll").respond(
            json={
                "transactionTime": "2015-02-07T13:28:17.239+02:00",
                "error": [
                    {"type": "OperationOutcome", "url": "https://example.com/warning1"},
                ],
            },
        )
        self.respx_mock.get("https://example.com/warning1").respond(
            json={
                "resourceType": "OperationOutcome",
                "issue": [{"severity": "warning", "diagnostics": "warning1"}],
            },
        )

        stdout = io.StringIO()
        with contextlib.redirect_stdout(stdout):
            await self.export()

        self.assertIn("Messages from server:\n - warning1\n", stdout.getvalue())

    async def test_deleted_resources(self):
        """Verify that we preserve the list of resources to be deleted"""
        self.mock_kickoff()
        self.mock_delete()
        self.respx_mock.get("https://example.com/poll").respond(
            json={
                "deleted": [
                    {"type": "Bundle", "url": "https://example.com/deleted1"},
                ],
            },
        )
        deleted1 = json.dumps(
            {
                "resourceType": "Bundle",
                "type": "transaction",
                "entry": [
                    {
                        "request": {"method": "DELETE", "url": "Patient/123"},
                    }
                ],
            }
        )
        self.respx_mock.get("https://example.com/deleted1").respond(text=deleted1)

        await self.export()

        bundle = common.read_text(f"{self.tmpdir}/deleted/Bundle.000.ndjson")
        self.assertEqual(bundle, deleted1)

        self.assert_log_equals(
            ("kickoff", None),
            ("status_complete", None),
            ("status_page_complete", None),
            ("manifest_complete", None),
            (
                "download_request",
                {
                    "fileUrl": "https://example.com/deleted1",
                    "itemType": "deleted",
                    "resourceType": "Bundle",
                },
            ),
            (
                "download_complete",
                {
                    "fileSize": len(deleted1),
                    "fileUrl": "https://example.com/deleted1",
                    "resourceCount": 1,
                },
            ),
            (
                "export_complete",
                {
                    "attachments": None,
                    "bytes": len(deleted1),
                    "duration": 0,
                    "files": 1,
                    "resources": 1,
                },
            ),
        )

    async def test_file_download_error(self):
        """Verify that we correctly handle a resource download failure"""
        self.mock_kickoff()
        self.respx_mock.get("https://example.com/poll").respond(
            json={
                "transactionTime": "2015-02-07T13:28:17.239+02:00",
                "output": [
                    {"type": "Condition", "url": "https://example.com/con1"},
                ],
            },
        )
        self.respx_mock.get("https://example.com/con1").respond(
            status_code=502, content=b'["error"]'
        )

        with self.assertRaisesRegex(
            errors.FatalError,
            r'An error occurred when connecting to "https://example.com/con1": \["error"\]',
        ):
            await self.export()

        self.assert_log_equals(
            ("kickoff", None),
            ("status_complete", None),
            ("status_page_complete", None),
            ("manifest_complete", None),
            ("download_request", None),
            ("download_error", None),
            ("download_request", None),
            ("download_error", None),
            ("download_request", None),
            ("download_error", None),
            ("download_request", None),
            ("download_error", None),
            ("download_request", None),
            (
                "download_error",
                {
                    "fileUrl": "https://example.com/con1",
                    "body": '["error"]',
                    "code": 502,
                    "message": 'An error occurred when connecting to "https://example.com/con1": '
                    '["error"]',
                    "responseHeaders": {"content-length": "9"},
                },
            ),
        )

    async def test_file_download_error_during_stream(self):
        """Verify that we correctly handle a resource download failure during the streaming"""
        self.mock_kickoff()
        self.respx_mock.get("https://example.com/poll").respond(
            json={
                "output": [
                    {"type": "Condition", "url": "https://example.com/con1"},
                ],
            },
        )

        async def exploding_stream() -> bytes:
            raise ZeroDivisionError("oops")
            yield b'{"id":"con1"}'

        self.respx_mock.get("https://example.com/con1").respond(stream=exploding_stream())

        with self.assertRaisesRegex(
            errors.FatalError,
            "Error downloading 'https://example.com/con1': oops",
        ):
            await self.export()

        self.assert_log_equals(
            ("kickoff", None),
            ("status_complete", None),
            ("status_page_complete", None),
            ("manifest_complete", None),
            ("download_request", None),
            (
                "download_error",
                {
                    "fileUrl": "https://example.com/con1",
                    "body": None,
                    "code": None,
                    "message": "oops",
                    "responseHeaders": None,
                },
            ),
        )

    async def test_unexpected_status_code(self):
        """Verify that we bail if we see a successful code we don't understand"""
        self.mock_kickoff(status_code=204)  # "no content"

        with self.assertRaisesRegex(errors.FatalError, "Unexpected status code 204"):
            await self.export()

        self.assert_log_equals(
            (
                "kickoff",
                {
                    "exportUrl": f"{self.fhir_url}/$export?_type=Condition%2CPatient",
                    "softwareName": "Test",
                    "softwareVersion": "0.git",
                    "softwareReleaseDate": "today",
                    "fhirVersion": "4.0.1",
                    "requestParameters": {"_type": "Condition,Patient"},
                    "errorCode": 204,
                    "errorBody": "",
                    "responseHeaders": {
                        "content-location": "https://example.com/poll",
                        "vendor-transaction-id": "1234",
                    },
                },
            ),
            uuid_export_id=True,
        )

    async def test_delay(self):
        """Verify that we wait the amount of time the server asks us to"""
        self.mock_kickoff(
            side_effect=[
                # Before returning a successful kickoff, pause for a minute (1h is capped to 1m)
                respx.MockResponse(status_code=429, headers={"Retry-After": "3600"}),
                respx.MockResponse(
                    status_code=202, headers={"Content-Location": "https://example.com/poll"}
                ),
            ]
        )
        self.respx_mock.get("https://example.com/poll").respond(
            # five hours (though 202 responses will get limited to five min)
            status_code=202,
            headers={"Retry-After": "18000", "X-Progress": "chill"},
            content=b"...",
        )

        with self.assertRaisesRegex(errors.FatalError, "Timed out waiting"):
            await self.export()

        # 2592060 == 30 days + one minute
        self.assertEqual(2592060, self.exporter._total_wait_time)

        self.assertListEqual(
            [
                mock.call(60),
                mock.call(300),
                mock.call(300),
                mock.call(300),
            ],
            self.sleep_mock.call_args_list[:4],
        )

    async def test_no_delete_if_interrupted(self):
        """Verify that we don't delete the export on the server if we raise an exception during the middle of export"""
        self.mock_kickoff()
        self.respx_mock.get("https://example.com/poll").respond(
            status_code=400,
            content=b"Test Status Call Failed",
        )

        with self.assertRaisesRegex(errors.FatalError, "Test Status Call Failed"):
            await self.export()

        self.assert_log_equals(
            ("kickoff", None),
            (
                "status_error",
                {
                    "body": "Test Status Call Failed",
                    "code": 400,
                    "message": (
                        'An error occurred when connecting to "https://example.com/poll": '
                        "Test Status Call Failed"
                    ),
                    "responseHeaders": {"content-length": "23"},
                },
            ),
        )

    async def test_delete_error_is_ignored(self):
        """Verify that we don't freak out if our DELETE call fails"""
        self.mock_kickoff()
        self.mock_delete(status_code=500)
        self.respx_mock.get("https://example.com/poll").respond(
            json={"transactionTime": "2015-02-07T13:28:17.239+02:00"},
        )
        await self.export()  # no exception thrown

    async def test_log_duration(self):
        """Verify that we calculate the correct export duration for the logs"""

        def status_check(request):
            del request
            future = utils.FROZEN_TIME_UTC + datetime.timedelta(
                hours=3, milliseconds=192, microseconds=252
            )
            self.time_machine.move_to(future)
            return respx.MockResponse(
                json={
                    "transactionTime": "2015-02-07T13:28:17.239+02:00",
                }
            )

        self.mock_kickoff()
        self.mock_delete()
        self.respx_mock.get("https://example.com/poll").mock(side_effect=status_check)

        await self.export()

        self.assert_log_equals(
            ("kickoff", None),
            ("status_complete", None),
            ("status_page_complete", None),
            ("manifest_complete", None),
            (
                "export_complete",
                {"attachments": None, "bytes": 0, "duration": 10800192, "files": 0, "resources": 0},
            ),
        )

    async def test_resume(self):
        """Verify that we just skip kickoff entirely if given a resume URL"""
        # Do initial (interrupted by server error) export
        self.mock_kickoff()
        self.respx_mock.get("https://example.com/poll").respond(status_code=400)
        with self.assertRaises(errors.FatalError):
            await self.export()
        self.assert_log_equals(
            ("kickoff", None),
            ("status_error", None),
        )

        # Now prep for resumed backup
        self.mock_kickoff(status_code=500)  # detect any attempt to start a new kickoff
        self.mock_delete()
        self.respx_mock.get("https://example.com/poll").respond(json={})
        await self.export(resume="https://example.com/poll")
        self.assert_log_equals(  # confirm we append to previous log
            ("kickoff", None),
            ("status_error", None),
            ("status_complete", None),
            ("status_page_complete", None),
            ("manifest_complete", None),
            ("export_complete", None),
        )

    async def test_retry_status_poll_then_failure(self):
        """Verify that we retry polling the status URL on server errors"""
        self.mock_kickoff()
        self.respx_mock.get("https://example.com/poll").respond(
            status_code=503,
            content=b"Test Status Call Failed",
        )

        with self.assertRaisesRegex(errors.FatalError, "Test Status Call Failed"):
            await self.export()

        self.assert_log_equals(
            ("kickoff", None),
            ("status_error", None),
            ("status_error", None),
            ("status_error", None),
            ("status_error", None),
            ("status_error", None),
        )

        self.assertEqual(
            [mock.call(x) for x in (60, 120, 240, 480)], self.sleep_mock.call_args_list
        )

    async def test_retry_status_poll_then_success(self):
        """Verify that we can recover from server errors"""
        self.mock_kickoff()
        self.mock_delete()
        self.respx_mock.get("https://example.com/poll").mock(
            side_effect=[
                httpx.Response(429),
                httpx.Response(408),
                httpx.Response(202, json={}),
                httpx.Response(504),
                httpx.Response(502, headers={"Retry-After": "20"}),
                httpx.Response(503),
                httpx.Response(504),
                httpx.Response(200, json={}),
            ],
        )

        await self.export()

        self.assert_log_equals(
            ("kickoff", None),
            ("status_error", None),
            ("status_error", None),
            ("status_error", None),
            ("status_error", None),
            ("status_error", None),
            ("status_error", None),
            ("status_complete", None),
            ("status_page_complete", None),
            ("manifest_complete", None),
            ("export_complete", None),
        )

        self.assertEqual(
            [mock.call(x) for x in (60, 120, 60, 60, 20, 240, 480)], self.sleep_mock.call_args_list
        )

    async def test_cancel_without_resume(self):
        async with self.fhir_client([]) as client:
            exporter = BulkExporter(client, set(), self.fhir_url, self.tmpdir)
            self.assertFalse(await exporter.cancel())


class TestBulkExportLogWriter(utils.AsyncTestCase):
    async def test_log_writer_multiple_params(self):
        """
        Verify that we handle writing a log with repeated params.

        This is something the bulk exporter *could* do and the spec kinda encourages,
        so we want the log writer to be able to handle it, if we change the bulk exporter
        to do it. But also, some servers seem to complain if you do it (even though the spec
        likes it). So this support is not normally tested - except here.
        """
        with tempfile.TemporaryDirectory() as tmpdir:
            log = BulkExportLogWriter(store.Root(tmpdir))
            log.kickoff("https://localhost/?_type=Patient&_type=Condition", {}, ValueError())
            written = common.read_json(f"{tmpdir}/log.ndjson")
        self.assertEqual(
            written["eventDetail"]["requestParameters"], {"_type": "Patient,Condition"}
        )

    async def test_log_non_dict_response(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            log = BulkExportLogWriter(store.Root(tmpdir))
            log.kickoff(
                "https://localhost/",
                {},
                errors.NetworkError(
                    "whoops",
                    utils.make_response(status_code=500, json_payload={"msg": "internal error"}),
                ),
            )
            written = common.read_json(f"{tmpdir}/log.ndjson")
        self.assertEqual(written["eventDetail"]["errorBody"], {"msg": "internal error"})


@ddt.ddt
class TestBulkExporterInit(utils.AsyncTestCase):
    """Tests for just creating the exporter, without any mocking needed"""

    @ddt.data(
        ("http://a.com", {}, "http://a.com/$export?_type=Patient"),  # no extra args
        (  # all the args, including merged _type fields and overridden _since!
            "http://a.com/$export?_type=Condition&_since=2000",
            {"since": "2020", "until": "2024"},
            "http://a.com/$export?_type=Condition%2CPatient&_since=2020&_until=2024",
        ),
        (  # extraneous args are kept
            "http://a.com/$export?_elements=birthDate",
            {},
            "http://a.com/$export?_elements=birthDate&_type=Patient",
        ),
        (  # can ignore provided resources when _type is present
            "http://a.com/$export?_type=Condition",
            {"prefer_url_resources": True},
            "http://a.com/$export?_type=Condition",
        ),
        (  # will not ignore provided resources when _type is *not* present
            "http://a.com/$export",
            {"prefer_url_resources": True},
            "http://a.com/$export?_type=Patient",
        ),
    )
    @ddt.unpack
    def test_param_merging(self, original_url, kwargs, expected_url):
        """Verify that we allow existing params and add some in ourselves"""
        exporter = BulkExporter(None, ["Patient"], original_url, "fake_dir", **kwargs)
        self.assertEqual(exporter._url, expected_url)

    @ddt.data(
        ("http://example.com/?info=a%2Cb", "http://example.com/?info=a%2Cb"),
        ("http%3A//example.com/%3Finfo%3Da%252Cb", "http://example.com/?info=a%2Cb"),
    )
    @ddt.unpack
    def test_resume_unquoting(self, resume_url, expected_url):
        """Verify that we correctly unquote the incoming URL as needed"""
        exporter = BulkExporter(
            None, ["Patient"], "http://example.com/", "fake_dir", resume=resume_url
        )
        self.assertEqual(exporter._resume, expected_url)


class TestBulkExportEndToEnd(utils.AsyncTestCase, utils.FhirClientMixin):
    """
    Test case for doing an entire bulk export loop, without mocking python code.

    Server responses are mocked, but that's it. This is more of a functional test case than a unit test case.
    """

    def set_up_requests(self):
        # /$export
        self.respx_mock.get(
            f"{self.fhir_url}/$export",
            headers={
                "Accept": "application/fhir+json",
                "Authorization": f"Bearer {self.fhir_bearer}",
                "Prefer": "respond-async",
            },
            params={
                "_type": "Patient",
            },
        ).respond(
            status_code=202,
            headers={"Content-Location": f"{self.fhir_base}/poll"},
        )

        # /poll
        self.respx_mock.get(
            f"{self.fhir_base}/poll",
            headers={
                "Accept": "application/json",
                "Authorization": f"Bearer {self.fhir_bearer}",
            },
        ).respond(
            json={
                "transactionTime": "2015-02-07T13:28:17+02:00",
                "output": [{"type": "Patient", "url": f"{self.fhir_base}/download/patient1"}],
            },
        )

        # /download/patient1
        self.respx_mock.get(
            f"{self.fhir_base}/download/patient1",
            headers={
                "Accept": "application/fhir+ndjson",
                "Authorization": f"Bearer {self.fhir_bearer}",
            },
        ).respond(
            json={  # content doesn't really matter
                "resourceType": "Patient",
                "id": "testPatient1",
            },
        )

        # DELETE /poll
        self.respx_mock.delete(
            f"{self.fhir_base}/poll",
            headers={
                "Accept": "application/fhir+json",
                "Authorization": f"Bearer {self.fhir_bearer}",
            },
        ).respond(
            status_code=202,
        )

    async def test_successful_etl_bulk_export(self):
        """Verify a happy path ETL bulk export, from toe to tip"""
        with tempfile.TemporaryDirectory() as tmpdir:
            self.set_up_requests()

            await cli.main(
                [
                    self.fhir_url,
                    f"{tmpdir}/output",
                    f"{tmpdir}/phi",
                    "--skip-init-checks",
                    "--output-format=ndjson",
                    "--task=patient",
                    f"--smart-client-id={self.fhir_client_id}",
                    f"--smart-key={self.fhir_jwks_path}",
                ]
            )

            self.assertEqual(
                {
                    "id": "4342abf315cf6f243e11f4d460303e36c6c3663a25c91cc6b1a8002476c850dd",
                    "resourceType": "Patient",
                },
                common.read_json(f"{tmpdir}/output/patient/patient.000.ndjson"),
            )

            self.assertEqual(
                {
                    "table_name": "patient",
                    "group_name": "MyGroup",
                    "export_time": "2015-02-07T13:28:17+02:00",
                    "export_url": f"{self.fhir_url}/$export?_type=Patient",
                    "etl_version": "1.0.0+test",
                    "etl_time": "2021-09-14T21:23:45+00:00",
                },
                common.read_json(f"{tmpdir}/output/etl__completion/etl__completion.000.ndjson"),
            )

    async def test_successful_standalone_bulk_export(self):
        """Verify a happy path standalone bulk export, from toe to tip"""
        with tempfile.TemporaryDirectory() as tmpdir:
            self.set_up_requests()

            await cli.main(
                [
                    "export",
                    self.fhir_url,
                    tmpdir,
                    "--task=patient",
                    f"--smart-client-id={self.fhir_client_id}",
                    f"--smart-jwks={self.fhir_jwks_path}",
                ]
            )

            self.assertEqual({"Patient.000.ndjson", "log.ndjson"}, set(os.listdir(tmpdir)))

            self.assertEqual(
                {
                    "id": "testPatient1",
                    "resourceType": "Patient",
                },
                common.read_json(f"{tmpdir}/Patient.000.ndjson"),
            )

            # log should have kickoff, 3 completes, download start, download complete, finished
            self.assertEqual(7, common.read_local_line_count(f"{tmpdir}/log.ndjson"))
