"""Writing a manifest of which notes were uploaded to Label Studio"""

import csv
from collections.abc import Collection

import cumulus_fhir_support as cfs

from cumulus_etl.upload_notes.labelstudio import LabelStudioNote

MANIFEST_FILENAME = "uploaded_notes.csv"

# Columns written to the manifest. `note_ref` matches what --select-by-csv expects, so the
# manifest can be fed straight back in as a selection input.
MANIFEST_COLUMNS = [
    "note_ref",
    "anon_note_ref",
    "patient_id",
    "anon_patient_id",
    "encounter_id",
    "anon_encounter_id",
    "unique_id",
]


def write_upload_manifest(notes: Collection[LabelStudioNote], export_to: cfs.FsPath | None) -> None:
    """
    Writes a CSV recording which real notes were uploaded, into the export folder.

    One row per real note (a grouped chart can contain several). Only written when the user asked
    to keep the exported documents via --export-to; otherwise the export folder is a temp dir that
    gets deleted after use.
    """
    if not export_to:
        return

    manifest_path = export_to.joinpath(MANIFEST_FILENAME)
    with manifest_path.open("w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(MANIFEST_COLUMNS)
        for note in notes:
            for note_ref, anon_note_ref in note.doc_mappings.items():
                writer.writerow(
                    [
                        note_ref,
                        anon_note_ref,
                        note.patient_id,
                        note.anon_patient_id,
                        note.encounter_id or "",
                        note.anon_encounter_id or "",
                        note.unique_id,
                    ]
                )
