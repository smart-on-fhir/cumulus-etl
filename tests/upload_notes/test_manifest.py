"""Tests for upload_notes/manifest.py"""

import cumulus_fhir_support as cfs

from cumulus_etl import common
from cumulus_etl.upload_notes import manifest
from cumulus_etl.upload_notes.labelstudio import LabelStudioNote
from tests.utils import AsyncTestCase


class TestUploadManifest(AsyncTestCase):
    """Tests for the uploaded-notes manifest writer."""

    @staticmethod
    def make_note(**kwargs) -> LabelStudioNote:
        defaults = {
            "unique_id": "Encounter/23",
            "patient_id": "P1",
            "anon_patient_id": "anonP1",
            "encounter_id": "23",
            "anon_encounter_id": "anon23",
        }
        defaults.update(kwargs)
        return LabelStudioNote(**defaults)

    def read_manifest(self, folder: str) -> list[dict]:
        with common.read_csv(cfs.FsPath(f"{folder}/{manifest.MANIFEST_FILENAME}")) as reader:
            return list(reader)

    ##################
    # Actual Tests
    def test_writes_one_row_per_real_note(self):
        """A grouped chart expands to one manifest row per contained note"""
        tmpdir = self.make_tempdir()
        note = self.make_note(
            doc_mappings={
                "DocumentReference/43": "DocumentReference/anon43",
                "DiagnosticReport/us": "DiagnosticReport/anonus",
            },
        )

        manifest.write_upload_manifest([note], cfs.FsPath(tmpdir))

        rows = self.read_manifest(tmpdir)
        self.assertEqual(manifest.MANIFEST_COLUMNS, list(rows[0].keys()))
        self.assertEqual(
            {"DocumentReference/43", "DiagnosticReport/us"}, {row["note_ref"] for row in rows}
        )
        row = next(r for r in rows if r["note_ref"] == "DocumentReference/43")
        self.assertEqual("DocumentReference/anon43", row["anon_note_ref"])
        self.assertEqual("P1", row["patient_id"])
        self.assertEqual("Encounter/23", row["unique_id"])

    def test_no_export_dir_is_a_noop(self):
        """Without an export folder, nothing is written and no error is raised"""
        # Should simply do nothing (the export folder is a temp dir deleted after use).
        manifest.write_upload_manifest([self.make_note(doc_mappings={"a": "b"})], None)

    def test_missing_encounter_is_blank(self):
        """Notes without an encounter get empty encounter columns, not the string 'None'"""
        tmpdir = self.make_tempdir()
        note = self.make_note(
            encounter_id=None,
            anon_encounter_id=None,
            doc_mappings={"DocumentReference/43": "DocumentReference/anon43"},
        )

        manifest.write_upload_manifest([note], cfs.FsPath(tmpdir))

        rows = self.read_manifest(tmpdir)
        self.assertEqual("", rows[0]["encounter_id"])
        self.assertEqual("", rows[0]["anon_encounter_id"])
