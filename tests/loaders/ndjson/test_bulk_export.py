"""Tests for bulk export support"""

import contextlib
import datetime
import io
import tempfile
from unittest import mock

import ddt
import respx

from cumulus_etl import cli, common, errors, store
from cumulus_etl.loaders.fhir.bulk_export import BulkExporter
from cumulus_etl.loaders.fhir.export_log import BulkExportLogParser
from tests import utils


@ddt.ddt
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

    def assert_log_equals(self, *rows) -> None:
        found_rows = list(common.read_ndjson(f"{self.tmpdir}/log.ndjson"))

        # Do we use the same export ID throughout?
        all_export_ids = {x["exportId"] for x in found_rows}
        self.assertEqual(1, len(all_export_ids))

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
            expected_duration = (complete_datetime - kickoff_datetime).microseconds // 1000
            found_duration = found_rows[complete_index]["eventDetail"]["duration"]
            self.assertEqual(found_duration, expected_duration)

        extracted_details = [(x["eventId"], x["eventDetail"]) for x in found_rows]

        # Match and reorder download requests/completes because those can be async/non-deterministic
        reordered_details = []
        completion = []
        downloads = {}
        for event_id, detail in extracted_details:
            if event_id == "download_request":
                self.assertNotIn(detail["fileUrl"], downloads)
                downloads[detail["fileUrl"]] = [(event_id, detail)]
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

    def mock_kickoff(self, params: str = "?_type=Condition%2CPatient", side_effect: list = None, **kwargs) -> None:
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
        self.respx_mock.get(
            "https://example.com/con1",
            headers={"Accept": "application/fhir+ndjson"},
        ).respond(json={"resourceType": "Condition", "id": "1"})
        self.respx_mock.get(
            "https://example.com/con2",
            headers={"Accept": "application/fhir+ndjson"},
        ).respond(json={"resourceType": "Condition", "id": "2"})
        self.respx_mock.get(
            "https://example.com/pat1",
            headers={"Accept": "application/fhir+ndjson"},
        ).respond(json={"resourceType": "Patient", "id": "P"})

        await self.export()

        self.assertEqual("MyGroup", self.exporter.group_name)
        self.assertEqual("2015-02-07T13:28:17.239000+02:00", self.exporter.export_datetime.isoformat())

        # Ensure we can read back our own log and parse the above values too
        parser = BulkExportLogParser(store.Root(self.tmpdir))
        self.assertEqual("MyGroup", parser.group_name)
        self.assertEqual("2015-02-07T13:28:17.239000+02:00", parser.export_datetime.isoformat())

        self.assertEqual(
            {"resourceType": "Condition", "id": "1"}, common.read_json(f"{self.tmpdir}/Condition.000.ndjson")
        )
        self.assertEqual(
            {"resourceType": "Condition", "id": "2"}, common.read_json(f"{self.tmpdir}/Condition.001.ndjson")
        )
        self.assertEqual({"resourceType": "Patient", "id": "P"}, common.read_json(f"{self.tmpdir}/Patient.000.ndjson"))

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
                    "deletedFileCount": 0,
                    "errorFileCount": 0,
                    "outputFileCount": 3,
                    "transactionTime": "2015-02-07T13:28:17.239+02:00",
                },
            ),
            (
                "download_request",
                {"fileUrl": "https://example.com/con1", "itemType": "output", "resourceType": "Condition"},
            ),
            ("download_complete", {"fileSize": 40, "fileUrl": "https://example.com/con1", "resourceCount": 1}),
            (
                "download_request",
                {"fileUrl": "https://example.com/con2", "itemType": "output", "resourceType": "Condition"},
            ),
            ("download_complete", {"fileSize": 40, "fileUrl": "https://example.com/con2", "resourceCount": 1}),
            (
                "download_request",
                {"fileUrl": "https://example.com/pat1", "itemType": "output", "resourceType": "Patient"},
            ),
            ("download_complete", {"fileSize": 38, "fileUrl": "https://example.com/pat1", "resourceCount": 1}),
            ("export_complete", {"attachments": None, "bytes": 118, "duration": 0, "files": 3, "resources": 3}),
        )

    async def test_since_until(self):
        """Verify that we send since & until parameters correctly to the server"""
        self.mock_kickoff(
            params="?_type=Condition%2CPatient&_since=2000-01-01T00%3A00%3A00%2B00.00&_until=2010",
            status_code=500,  # early exit
        )

        with self.assertRaises(errors.FatalError):
            await self.export(since="2000-01-01T00:00:00+00.00", until="2010")

    async def test_export_error(self):
        """Verify that we download and present any server-reported errors during the bulk export"""
        self.mock_kickoff()
        self.mock_delete()
        self.respx_mock.get(
            "https://example.com/poll",
            headers={"Accept": "application/json"},
        ).respond(
            json={
                "transactionTime": "2015-02-07T13:28:17.239+02:00",
                "error": [
                    {"type": "OperationOutcome", "url": "https://example.com/err1"},
                    {"type": "OperationOutcome", "url": "https://example.com/err2"},
                ],
                "output": [  # include an output too, to confirm we do download it
                    {"type": "Condition", "url": "https://example.com/con1"},
                ],
            },
        )
        self.respx_mock.get(
            "https://example.com/err1",
            headers={"Accept": "application/fhir+ndjson"},
        ).respond(
            json={
                "resourceType": "OperationOutcome",
                "issue": [{"severity": "error", "diagnostics": "err1"}],
            },
        )
        self.respx_mock.get(
            "https://example.com/err2",
            headers={"Accept": "application/fhir+ndjson"},
        ).respond(
            text=(
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
        )
        self.respx_mock.get(
            "https://example.com/con1",
            headers={"Accept": "application/fhir+ndjson"},
        ).respond(
            json={"resourceType": "Condition"},
        )

        with self.assertRaisesRegex(
            errors.FatalError, "Errors occurred during export:\n - err1\n - err2\n - err3\n - err4"
        ):
            await self.export()

        self.assert_log_equals(
            ("kickoff", None),
            (
                "status_complete",
                {
                    "deletedFileCount": 0,
                    "errorFileCount": 2,
                    "outputFileCount": 1,
                    "transactionTime": "2015-02-07T13:28:17.239+02:00",
                },
            ),
            (
                "download_request",
                {"fileUrl": "https://example.com/con1", "itemType": "output", "resourceType": "Condition"},
            ),
            ("download_complete", {"fileSize": 29, "fileUrl": "https://example.com/con1", "resourceCount": 1}),
            (
                "download_request",
                {"fileUrl": "https://example.com/err1", "itemType": "error", "resourceType": "OperationOutcome"},
            ),
            ("download_complete", {"fileSize": 93, "fileUrl": "https://example.com/err1", "resourceCount": 1}),
            (
                "download_request",
                {"fileUrl": "https://example.com/err2", "itemType": "error", "resourceType": "OperationOutcome"},
            ),
            ("download_complete", {"fileSize": 322, "fileUrl": "https://example.com/err2", "resourceCount": 3}),
            ("export_complete", {"attachments": None, "bytes": 444, "duration": 0, "files": 3, "resources": 5}),
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
        self.respx_mock.get("https://example.com/con1").respond(status_code=501, content=b'["error"]')

        with self.assertRaisesRegex(
            errors.FatalError,
            r'An error occurred when connecting to "https://example.com/con1": \["error"\]',
        ):
            await self.export()

        self.assert_log_equals(
            ("kickoff", None),
            ("status_complete", None),
            ("download_request", None),
            (
                "download_error",
                {
                    "fileUrl": "https://example.com/con1",
                    "body": '["error"]',
                    "code": 501,
                    "message": 'An error occurred when connecting to "https://example.com/con1": ["error"]',
                    "responseHeaders": {"content-length": "9"},
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
        )

    @mock.patch("cumulus_etl.loaders.fhir.bulk_export.asyncio.sleep")
    async def test_delay(self, mock_sleep):
        """Verify that we wait the amount of time the server asks us to"""
        self.mock_kickoff(
            side_effect=[
                # Before returning a successful kickoff, pause for an hour
                respx.MockResponse(status_code=429, headers={"Retry-After": "3600"}),
                respx.MockResponse(status_code=202, headers={"Content-Location": "https://example.com/poll"}),
            ]
        )
        self.respx_mock.get("https://example.com/poll").side_effect = [
            # default of one minute
            respx.MockResponse(status_code=429, headers={"X-Progress": "chill"}, content=b"{}"),
            # five hours (though 202 responses will get limited to five min)
            respx.MockResponse(status_code=202, headers={"Retry-After": "18000"}, content=b"..."),
            # 23 hours (putting us over a day)
            respx.MockResponse(status_code=429, headers={"Retry-After": "82800", "X-Progress": "plz wait"}),
        ]

        with self.assertRaisesRegex(errors.FatalError, "Timed out waiting"):
            await self.export()

        # 86760 == 24 hours + six minutes
        self.assertEqual(86760, self.exporter._total_wait_time)  # pylint: disable=protected-access

        self.assertListEqual(
            [
                mock.call(3600),
                mock.call(60),
                mock.call(300),
                mock.call(82800),
            ],
            mock_sleep.call_args_list,
        )

        self.assert_log_equals(
            ("kickoff", None),
            ("status_progress", {"body": {}, "xProgress": "chill", "retryAfter": None}),
            ("status_progress", {"body": "...", "xProgress": None, "retryAfter": "18000"}),
            ("status_progress", {"body": "", "xProgress": "plz wait", "retryAfter": "82800"}),
            (
                "status_error",
                {
                    "body": None,
                    "code": None,
                    "message": "Timed out waiting for the bulk FHIR export to finish.",
                    "responseHeaders": None,
                },
            ),
        )

    async def test_no_delete_if_interrupted(self):
        """Verify that we don't delete the export on the server if we raise an exception during the middle of export"""
        self.mock_kickoff()
        self.respx_mock.get("https://example.com/poll").respond(
            status_code=500,
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
                    "code": 500,
                    "message": (
                        'An error occurred when connecting to "https://example.com/poll": ' "Test Status Call Failed"
                    ),
                    "responseHeaders": {"content-length": "23"},
                },
            ),
        )

    async def test_log_duration(self):
        """Verify that we calculate the correct export duration for the logs"""

        def status_check(request):
            del request
            future = utils.FROZEN_TIME_UTC + datetime.timedelta(milliseconds=192)
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
            (
                "export_complete",
                {"attachments": None, "bytes": 0, "duration": 192, "files": 0, "resources": 0},
            ),
        )


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

    async def test_successful_bulk_export(self):
        """Verify a happy path bulk export, from toe to tip"""
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
                    f"--smart-jwks={self.fhir_jwks_path}",
                    "--write-completion",
                ]
            )

            self.assertEqual(
                {"id": "4342abf315cf6f243e11f4d460303e36c6c3663a25c91cc6b1a8002476c850dd", "resourceType": "Patient"},
                common.read_json(f"{tmpdir}/output/patient/patient.000.ndjson"),
            )

            self.assertEqual(
                {
                    "table_name": "patient",
                    "group_name": "MyGroup",
                    "export_time": "2015-02-07T13:28:17+02:00",
                },
                common.read_json(f"{tmpdir}/output/etl__completion/etl__completion.000.ndjson"),
            )
