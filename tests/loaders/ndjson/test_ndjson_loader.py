"""Tests for ndjson loading"""

import datetime
import os
import tempfile

import cumulus_fhir_support as cfs

from cumulus_etl import cli, common, errors, feedback, loaders
from tests.utils import AsyncTestCase


class TestNdjsonLoader(AsyncTestCase):
    """
    Test case for the etl pipeline and ndjson loader.

    i.e. tests for cli.py & ndjson_loader.py.
    """

    def setUp(self):
        super().setUp()
        self.progress = feedback.Progress()

    @staticmethod
    def _write_log_file(path: str, group: str, timestamp: str) -> None:
        with common.NdjsonWriter(cfs.FsPath(path)) as writer:
            writer.write(
                {
                    "eventId": "kickoff",
                    "exportId": "testing",
                    "eventDetail": {"exportUrl": f"https://host/Group/{group}/$export"},
                }
            )
            writer.write(
                {
                    "eventId": "status_complete",
                    "exportId": "testing",
                    "eventDetail": {"transactionTime": timestamp},
                }
            )

    async def test_local_happy_path(self):
        """Do a full local load from a folder."""
        patient = {"id": "A", "resourceType": "Patient"}

        with tempfile.TemporaryDirectory() as tmpdir:
            self._write_log_file(f"{tmpdir}/log.ndjson", "G", "1999-03-14T14:12:10")
            with common.NdjsonWriter(cfs.FsPath(f"{tmpdir}/Patient.ndjson")) as writer:
                writer.write(patient)

            loader = loaders.FhirNdjsonLoader(cfs.FsPath(tmpdir))
            results = await loader.load_resources({"Patient"}, progress=self.progress)

        self.assertEqual(["Patient.ndjson"], os.listdir(results.path))
        self.assertEqual(patient, cfs.FsPath(f"{results.path}/Patient.ndjson").read_json())
        self.assertEqual("G", results.group_name)
        self.assertEqual(
            datetime.datetime.fromisoformat("1999-03-14T14:12:10"), results.export_datetime
        )

    # At some point, we do want to make this fatal.
    # But not while this feature is still optional.
    async def test_log_parsing_is_non_fatal(self):
        """Do a local load with a bad log setup."""
        with tempfile.TemporaryDirectory() as tmpdir:
            self._write_log_file(f"{tmpdir}/log.1.ndjson", "G1", "2001-01-01")
            self._write_log_file(f"{tmpdir}/log.2.ndjson", "G2", "2002-02-02")

            loader = loaders.FhirNdjsonLoader(cfs.FsPath(tmpdir))
            results = await loader.load_resources(set(), progress=self.progress)

        # We used neither log and didn't error out.
        self.assertIsNone(results.group_name)
        self.assertIsNone(results.export_datetime)

    async def test_fhir_url(self):
        with self.assert_fatal_exit(errors.FEATURE_REMOVED):
            await cli.main(
                [
                    "https://example.com/hello1/Group/1234",
                    "/tmp/output",
                    "/tmp/phi",
                    "--skip-init-checks",
                ]
            )

    async def test_reads_deleted_ids(self):
        """Verify we read in the deleted/ folder"""
        with tempfile.TemporaryDirectory() as tmpdir:
            os.mkdir(f"{tmpdir}/deleted")
            cfs.FsPath(f"{tmpdir}/deleted/deletes.ndjson").write_json(
                {
                    "resourceType": "Bundle",
                    "type": "transaction",
                    "entry": [
                        {"request": {"method": "GET", "url": "Patient/bad-method"}},
                        {"request": {"method": "DELETE", "url": "Patient/pat1"}},
                        {"request": {"method": "DELETE", "url": "Patient/too/many/slashes"}},
                        {"request": {"method": "DELETE", "url": "Condition/con1"}},
                        {"request": {"method": "DELETE", "url": "Condition/con2"}},
                    ],
                },
            )
            # This next bundle will be ignored because of the wrong "type"
            cfs.FsPath(f"{tmpdir}/deleted/messages.ndjson").write_json(
                {
                    "resourceType": "Bundle",
                    "type": "message",
                    "entry": [
                        {
                            "request": {"method": "DELETE", "url": "Patient/wrong-message-type"},
                        }
                    ],
                },
            )
            # This next file will be ignored because of the wrong "resourceType"
            cfs.FsPath(f"{tmpdir}/deleted/conditions-for-some-reason.ndjson").write_json(
                {
                    "resourceType": "Condition",
                    "recordedDate": "2024-09-04",
                },
            )
            loader = loaders.FhirNdjsonLoader(cfs.FsPath(tmpdir))
            results = await loader.load_resources({"Patient"}, progress=self.progress)

        self.assertEqual(results.deleted_ids, {"Patient": {"pat1"}, "Condition": {"con1", "con2"}})

    async def test_detect_resources(self):
        """Verify we can inspect a folder and find all resources."""
        with tempfile.TemporaryDirectory() as tmpdir:
            cfs.FsPath(f"{tmpdir}/p.ndjson").write_json({"id": "A", "resourceType": "Patient"})
            cfs.FsPath(f"{tmpdir}/unrelated.ndjson").write_json({"num_cats": 5})
            cfs.FsPath(f"{tmpdir}/c.ndjson").write_json({"id": "A", "resourceType": "Condition"})

            loader = loaders.FhirNdjsonLoader(cfs.FsPath(tmpdir))
            resources = await loader.detect_resources(progress=self.progress)

        self.assertEqual(resources, {"Condition", "Patient"})
