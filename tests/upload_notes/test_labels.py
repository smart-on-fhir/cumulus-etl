"""Tests for upload_notes/labels.py"""

import contextlib
import io
import os

import cumulus_fhir_support as cfs

from cumulus_etl import common
from cumulus_etl.upload_notes import labels
from cumulus_etl.upload_notes.labelstudio import Highlight, LabelStudioNote
from tests.utils import AsyncTestCase


class TestLabelFiles(AsyncTestCase):
    """Tests for the Chart Review label file writer."""

    @staticmethod
    def make_note(**kwargs) -> LabelStudioNote:
        defaults = {
            "unique_id": "Encounter/23",
            "patient_id": "P1",
            "anon_patient_id": "anonP1",
            "encounter_id": "23",
            "anon_encounter_id": "anon23",
            "text": "sore throat and a headache",
            "doc_mappings": {"DocumentReference/43": "DocumentReference/anon43"},
            "doc_spans": {"DocumentReference/43": (0, 26)},
        }
        defaults.update(kwargs)
        return LabelStudioNote(**defaults)

    @staticmethod
    def make_highlight(label: str, span: tuple[int, int], **kwargs) -> Highlight:
        defaults = {"origin": "Cumulus"}
        defaults.update(kwargs)
        return Highlight(label, span, **defaults)

    def read_labels(self, folder: str, filename: str = "labels-cumulus.csv") -> list[dict]:
        with common.read_csv(cfs.FsPath(f"{folder}/{filename}")) as reader:
            return list(reader)

    ##################
    # Actual Tests
    def test_basic_label_file(self):
        """Each label becomes a row, keyed by the real note ID"""
        tmpdir = self.make_tempdir()
        note = self.make_note(
            highlights=[
                self.make_highlight("Sore throat", (0, 11)),
                self.make_highlight("Headache", (18, 26)),
            ],
        )

        labels.write_label_files([note], cfs.FsPath(tmpdir))

        rows = self.read_labels(tmpdir)
        self.assertEqual(["note_ref", "label"], list(rows[0].keys()))
        self.assertEqual(
            [
                {"note_ref": "DocumentReference/43", "label": "Headache"},
                {"note_ref": "DocumentReference/43", "label": "Sore throat"},
            ],
            rows,
        )

    def test_no_export_dir_is_a_noop(self):
        """Without an export folder, nothing is written and no error is raised"""
        note = self.make_note(highlights=[self.make_highlight("Sore throat", (0, 11))])
        labels.write_label_files([note], None)

    def test_no_labels_writes_nothing(self):
        """Without any labels at all, we have no annotator to write a file for"""
        tmpdir = self.make_tempdir()

        labels.write_label_files([self.make_note()], cfs.FsPath(tmpdir))

        self.assertEqual([], os.listdir(tmpdir))

    def test_unlabeled_notes_get_a_blank_row(self):
        """Chart Review reads a blank label as 'reviewed, found nothing'"""
        tmpdir = self.make_tempdir()
        labeled = self.make_note(highlights=[self.make_highlight("Sore throat", (0, 11))])
        unlabeled = self.make_note(
            doc_mappings={"DocumentReference/44": "DocumentReference/anon44"},
            doc_spans={"DocumentReference/44": (0, 26)},
        )

        labels.write_label_files([labeled, unlabeled], cfs.FsPath(tmpdir))

        rows = self.read_labels(tmpdir)
        self.assertEqual(
            [
                {"note_ref": "DocumentReference/43", "label": "Sore throat"},
                {"note_ref": "DocumentReference/44", "label": ""},
            ],
            rows,
        )

    def test_one_file_per_origin(self):
        """Each origin is a separate Chart Review annotator, so it gets its own file"""
        tmpdir = self.make_tempdir()
        note = self.make_note(
            highlights=[
                self.make_highlight("Sore throat", (0, 11), origin="gpt-5.4"),
                self.make_highlight("Headache", (18, 26), origin="Claude Sonnet 4.5"),
            ],
        )

        labels.write_label_files([note], cfs.FsPath(tmpdir))

        self.assertEqual(
            [{"note_ref": "DocumentReference/43", "label": "Sore throat"}],
            self.read_labels(tmpdir, "labels-gpt_5_4.csv"),
        )
        self.assertEqual(
            [{"note_ref": "DocumentReference/43", "label": "Headache"}],
            self.read_labels(tmpdir, "labels-claude_sonnet_4_5.csv"),
        )

    def test_origin_covers_all_notes(self):
        """An origin's file lists notes it never labeled, so denominators stay right"""
        tmpdir = self.make_tempdir()
        first = self.make_note(highlights=[self.make_highlight("Fever", (0, 5), origin="gpt")])
        second = self.make_note(
            doc_mappings={"DocumentReference/44": "DocumentReference/anon44"},
            doc_spans={"DocumentReference/44": (0, 26)},
            highlights=[self.make_highlight("Cough", (0, 5), origin="claude")],
        )

        labels.write_label_files([first, second], cfs.FsPath(tmpdir))

        self.assertEqual(
            [
                {"note_ref": "DocumentReference/43", "label": "Fever"},
                {"note_ref": "DocumentReference/44", "label": ""},
            ],
            self.read_labels(tmpdir, "labels-gpt.csv"),
        )
        self.assertEqual(
            [
                {"note_ref": "DocumentReference/43", "label": ""},
                {"note_ref": "DocumentReference/44", "label": "Cough"},
            ],
            self.read_labels(tmpdir, "labels-claude.csv"),
        )

    def test_sublabels(self):
        """Sublabel columns show up as a pair, and only when the origin uses them"""
        tmpdir = self.make_tempdir()
        note = self.make_note(
            highlights=[
                self.make_highlight(
                    "Fever", (0, 5), sublabel_name="Severity", sublabel_value="Mild"
                ),
                self.make_highlight("Headache", (18, 26)),
            ],
        )

        labels.write_label_files([note], cfs.FsPath(tmpdir))

        rows = self.read_labels(tmpdir)
        self.assertEqual(
            ["note_ref", "label", "sublabel_name", "sublabel_value"], list(rows[0].keys())
        )
        self.assertEqual(
            [
                {
                    "note_ref": "DocumentReference/43",
                    "label": "Fever",
                    "sublabel_name": "Severity",
                    "sublabel_value": "Mild",
                },
                {
                    "note_ref": "DocumentReference/43",
                    "label": "Headache",
                    "sublabel_name": "",
                    "sublabel_value": "",
                },
            ],
            rows,
        )

    def test_half_a_sublabel_is_dropped(self):
        """Chart Review's Label() rejects a sublabel name with no value, so don't emit one"""
        # Our NLP sources read sublabel_name and sublabel_value as independent columns, so a
        # partly-filled row can reach us. Degrade to the bare label rather than write a file
        # that Chart Review refuses to load.
        tmpdir = self.make_tempdir()
        note = self.make_note(
            highlights=[self.make_highlight("Rash", (0, 5), sublabel_name="Severity")],
        )

        labels.write_label_files([note], cfs.FsPath(tmpdir))

        self.assertEqual(
            [{"note_ref": "DocumentReference/43", "label": "Rash"}], self.read_labels(tmpdir)
        )

    def test_mixed_sublabels_warn(self):
        """A label emitted both bare and sublabeled gets dropped by Chart Review, so warn"""
        tmpdir = self.make_tempdir()
        note = self.make_note(
            highlights=[
                self.make_highlight(
                    "Fever", (0, 5), origin="gpt", sublabel_name="Severity", sublabel_value="Mild"
                ),
                self.make_highlight("Fever", (18, 26), origin="claude"),
            ],
        )

        stdout = io.StringIO()
        with contextlib.redirect_stdout(stdout):
            labels.write_label_files([note], cfs.FsPath(tmpdir))

        self.assertIn("appear both with and without a sublabel: Fever", stdout.getvalue())

    def test_consistent_sublabels_do_not_warn(self):
        """Only the mixed case is a problem - don't cry wolf on normal sublabel use"""
        tmpdir = self.make_tempdir()
        note = self.make_note(
            highlights=[
                self.make_highlight(
                    "Fever", (0, 5), sublabel_name="Severity", sublabel_value="Mild"
                ),
                self.make_highlight("Headache", (18, 26)),
            ],
        )

        stdout = io.StringIO()
        with contextlib.redirect_stdout(stdout):
            labels.write_label_files([note], cfs.FsPath(tmpdir))

        self.assertNotIn("sublabel", stdout.getvalue())

    def test_pipe_in_label_is_dropped(self):
        """Chart Review reserves '|' and won't load a file containing one, so leave it out"""
        tmpdir = self.make_tempdir()
        note = self.make_note(
            highlights=[
                self.make_highlight("Fever | chills", (0, 5)),
                self.make_highlight("Headache", (18, 26)),
            ],
        )

        stdout = io.StringIO()
        with contextlib.redirect_stdout(stdout):
            labels.write_label_files([note], cfs.FsPath(tmpdir))

        self.assertIn("1 mention across 1 note, dropped: label contains a '|'", stdout.getvalue())
        self.assertIn("Fever | chills", stdout.getvalue())
        self.assertEqual(
            [{"note_ref": "DocumentReference/43", "label": "Headache"}], self.read_labels(tmpdir)
        )

    def test_unplaceable_span_is_reported(self):
        """A span that lands outside every note in the chart is dropped, and said so"""
        tmpdir = self.make_tempdir()
        note = self.make_note(
            highlights=[
                self.make_highlight("Fever", (0, 5)),
                self.make_highlight("Ghost", (500, 505)),  # past the end of the note text
            ],
        )

        stdout = io.StringIO()
        with contextlib.redirect_stdout(stdout):
            labels.write_label_files([note], cfs.FsPath(tmpdir))

        self.assertIn("span falls outside the text of any note", stdout.getvalue())
        self.assertIn("Ghost", stdout.getvalue())
        self.assertEqual(
            [{"note_ref": "DocumentReference/43", "label": "Fever"}], self.read_labels(tmpdir)
        )

    def test_skip_summary_counts_mentions_and_notes(self):
        """The summary counts mentions and the notes they came from, not just label names"""
        tmpdir = self.make_tempdir()
        first = self.make_note(
            highlights=[
                self.make_highlight("A|B", (0, 5)),
                self.make_highlight("A|B", (10, 15)),  # same label, second mention
            ],
        )
        second = self.make_note(
            doc_mappings={"DocumentReference/44": "DocumentReference/anon44"},
            doc_spans={"DocumentReference/44": (0, 26)},
            highlights=[self.make_highlight("C|D", (0, 5))],
        )

        stdout = io.StringIO()
        with contextlib.redirect_stdout(stdout):
            labels.write_label_files([first, second], cfs.FsPath(tmpdir))

        self.assertIn("3 mentions across 2 notes", stdout.getvalue())

    def test_half_sublabel_is_reported(self):
        """Degrading a half-populated sublabel is worth mentioning too"""
        tmpdir = self.make_tempdir()
        note = self.make_note(
            highlights=[self.make_highlight("Rash", (0, 5), sublabel_name="Severity")],
        )

        stdout = io.StringIO()
        with contextlib.redirect_stdout(stdout):
            labels.write_label_files([note], cfs.FsPath(tmpdir))

        self.assertIn("kept as a bare label", stdout.getvalue())

    def test_pipe_in_sublabel_name_is_dropped(self):
        """Same rule applies to sublabel names - but not to sublabel values"""
        tmpdir = self.make_tempdir()
        note = self.make_note(
            highlights=[
                self.make_highlight("Fever", (0, 5), sublabel_name="A|B", sublabel_value="Mild"),
                self.make_highlight(
                    "Rash", (18, 26), sublabel_name="Severity", sublabel_value="A|B"
                ),
            ],
        )

        with contextlib.redirect_stdout(io.StringIO()):
            labels.write_label_files([note], cfs.FsPath(tmpdir))

        # The sublabel *value* keeps its pipe - Chart Review allows that one.
        self.assertEqual(
            [
                {
                    "note_ref": "DocumentReference/43",
                    "label": "Rash",
                    "sublabel_name": "Severity",
                    "sublabel_value": "A|B",
                }
            ],
            self.read_labels(tmpdir),
        )

    def test_duplicate_labels_are_collapsed(self):
        """The same label at several spans is one row - Chart Review compares label sets"""
        tmpdir = self.make_tempdir()
        note = self.make_note(
            highlights=[
                self.make_highlight("Fever", (0, 5)),
                self.make_highlight("Fever", (18, 26)),
            ],
        )

        labels.write_label_files([note], cfs.FsPath(tmpdir))

        self.assertEqual(
            [{"note_ref": "DocumentReference/43", "label": "Fever"}], self.read_labels(tmpdir)
        )

    def test_grouped_chart_attributes_labels_to_the_right_note(self):
        """Spans in a grouped chart still resolve back to the note they came from"""
        tmpdir = self.make_tempdir()
        note = self.make_note(
            doc_mappings={
                "DocumentReference/43": "DocumentReference/anon43",
                "DocumentReference/44": "DocumentReference/anon44",
            },
            doc_spans={"DocumentReference/43": (0, 26), "DocumentReference/44": (30, 56)},
            highlights=[
                self.make_highlight("Sore throat", (0, 11)),
                self.make_highlight("Headache", (48, 56)),
            ],
        )

        labels.write_label_files([note], cfs.FsPath(tmpdir))

        self.assertEqual(
            [
                {"note_ref": "DocumentReference/43", "label": "Sore throat"},
                {"note_ref": "DocumentReference/44", "label": "Headache"},
            ],
            self.read_labels(tmpdir),
        )

    def test_both_note_types_share_one_file(self):
        """Fully-qualified refs let both resource types live in a single file"""
        # Chart Review takes a ref with a "/" verbatim and only guesses a resource type from the
        # column name for bare IDs, so mixing the two is fine - and they resolve to the same
        # chart anyway, since a Chart Review chart is a resource-agnostic Label Studio note ID.
        tmpdir = self.make_tempdir()
        note = self.make_note(
            doc_mappings={
                "DocumentReference/43": "DocumentReference/anon43",
                "DiagnosticReport/us": "DiagnosticReport/anonus",
            },
            doc_spans={"DocumentReference/43": (0, 26), "DiagnosticReport/us": (30, 56)},
            highlights=[
                self.make_highlight("Sore throat", (0, 11)),
                self.make_highlight("Fever", (30, 35)),
            ],
        )

        labels.write_label_files([note], cfs.FsPath(tmpdir))

        self.assertEqual(["labels-cumulus.csv"], os.listdir(tmpdir))
        self.assertEqual(
            [
                {"note_ref": "DocumentReference/43", "label": "Sore throat"},
                {"note_ref": "DiagnosticReport/us", "label": "Fever"},
            ],
            self.read_labels(tmpdir),
        )
