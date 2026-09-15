"""Writing chart-review label files for the notes we upload"""

import csv
from collections.abc import Collection, Iterable

import cumulus_fhir_support as cfs
import rich

from cumulus_etl.upload_notes.labelstudio import NON_ALPHANUM, UNDERSCORES, LabelStudioNote

# We write fully-qualified refs ("DocumentReference/abc"), which Chart Review takes verbatim.
# It only falls back to guessing a resource type from the column's name for bare IDs, so one file
# can hold both DocumentReferences and DiagnosticReports - they resolve to the same chart anyway,
# since a Chart Review chart is a Label Studio note ID and is resource-agnostic.
# See https://docs.smarthealthit.org/cumulus/chart-review/config.html
#
# "note_ref" is the column name that works in both directions. Chart Review recognizes it, and so
# does cumulus_fhir_support, which we use to read these files back in (--select-by-csv). A "..._id"
# name would satisfy Chart Review but not cfs, which treats "..._id" columns as holding *bare* IDs
# and prepends a resource type - turning our value into "DocumentReference/DocumentReference/abc".
ID_COLUMN = "note_ref"

LABEL_COLUMNS = ["label"]
SUBLABEL_COLUMNS = ["sublabel_name", "sublabel_value"]

# Every way a label mention can fail to survive the trip, and what we tell the user about it.
# These keys are the only ones _gather_rows() may record a skip under.
SKIP_REASONS = {
    "delimiter": "dropped: label contains a '|', which Chart Review reserves as its delimiter",
    "unplaceable": "dropped: span falls outside the text of any note in the chart",
    "half_sublabel": "kept as a bare label: sublabel had a name but no value",
}


def write_label_files(
    notes: Collection[LabelStudioNote], export_labels_to: cfs.FsPath | None
) -> None:
    """
    Writes Chart Review external annotator CSVs, one per label origin.

    Each origin (i.e. each NLP model or table that gave us labels) becomes its own file, because
    Chart Review scores one annotator per file. Every uploaded note gets a row in every file -
    with an empty label if that origin had nothing to say about it - because Chart Review reads a
    blank label as "reviewed, found nothing" but a missing note as "not reviewed at all".
    """
    if not export_labels_to:
        return

    rows, skipped = _gather_rows(notes)
    _warn_about_skips(skipped)
    if not rows:
        rich.print("No labels were found, so no label files were written.")
        return

    export_labels_to.makedirs()
    _warn_about_mixed_sublabels(rows)

    for origin, origin_rows in sorted(rows.items()):
        filename = f"labels-{_slugify(origin)}.csv"
        _write_one_file(export_labels_to.joinpath(filename), origin_rows)
        rich.print(f"Wrote {len(origin_rows):,} label rows to {filename}")


def _gather_rows(
    notes: Collection[LabelStudioNote],
) -> tuple[dict[str, list[tuple]], dict[str, dict]]:
    """
    Gathers up the label rows to write, and a record of anything we couldn't write as-is.

    Returns ({origin: [(note ref, label, sublabel name, sublabel value)]}, skips)
    """
    skipped = {}  # reason -> {"mentions": count, "notes": set of IDs, "labels": set of labels}

    def record_skip(reason: str, note_id: str, label: str) -> None:
        entry = skipped.setdefault(reason, {"mentions": 0, "notes": set(), "labels": set()})
        entry["mentions"] += 1
        entry["notes"].add(note_id)
        entry["labels"].add(label)

    # Which notes did we upload, in upload order? A grouped chart holds several notes, and can
    # mix resource types - which is fine, since they all go in the same file.
    note_refs = [ref for note in notes for ref in note.doc_mappings]

    # Now gather up the labels each origin gave to each of those notes.
    labels = {}  # origin -> note ref -> {(label, sublabel name, sublabel value)}
    for note in notes:
        for highlight in note.highlights:
            ref = _ref_for_span(note, highlight.span)
            if ref is None:
                # Normally can't happen, since highlights are built from doc-relative spans - but
                # a source whose spans don't match the note text we loaded can land outside them.
                record_skip("unplaceable", note.unique_id, highlight.label)
                continue
            # Chart Review rejects a sublabel name that has no value, so only count a sublabel
            # when we have both halves - our NLP sources read the two columns independently.
            if highlight.sublabel_name and highlight.sublabel_value:
                sublabel = (highlight.sublabel_name, highlight.sublabel_value)
            else:
                if highlight.sublabel_name:
                    record_skip("half_sublabel", ref, highlight.label)
                sublabel = ("", "")
            # "|" is Chart Review's own label/sublabel delimiter and it refuses to load a file
            # containing one, so a label from an NLP model can render the whole file unreadable.
            # Drop just that mention rather than the file. (Sublabel values may contain "|".)
            if "|" in highlight.label or "|" in sublabel[0]:
                record_skip("delimiter", ref, highlight.label)
                continue
            found = labels.setdefault(highlight.origin, {})
            # A label often appears at several spans in one note, but Chart Review compares
            # per-note label sets, so collapse those down to one row.
            found.setdefault(ref, set()).add((highlight.label, *sublabel))

    rows = {}
    for origin, found in labels.items():
        origin_rows = []
        for ref in note_refs:
            if note_labels := found.get(ref):
                origin_rows.extend((ref, *label) for label in sorted(note_labels))
            else:
                origin_rows.append((ref, "", "", ""))
        rows[origin] = origin_rows

    return rows, skipped


def _warn_about_skips(skipped: dict[str, dict]) -> None:
    """Summarizes any label mentions we couldn't write out as-is"""
    if not skipped:
        return

    rich.print("Warning: some label mentions did not make it into the label files as-is:")
    for reason, description in SKIP_REASONS.items():  # a stable, sensible order
        if not (entry := skipped.get(reason)):
            continue
        mentions = entry["mentions"]
        notes = len(entry["notes"])
        labels = sorted(entry["labels"])
        shown = ", ".join(labels[:3]) + (", …" if len(labels) > 3 else "")
        rich.print(
            f"  {mentions:,} mention{'' if mentions == 1 else 's'} "
            f"across {notes:,} note{'' if notes == 1 else 's'}, "
            f"{description} ({shown})"
        )


def _warn_about_mixed_sublabels(rows: dict[str, list[tuple]]) -> None:
    """
    Warns about labels we emit both bare and with a sublabel.

    Once any annotator uses a sublabel for a label, Chart Review treats every bare mention of
    that label as invalid and drops it - across every annotator, including the human reviewers.
    That silently deflates a score, so it's worth flagging while we can still point at the label.
    """
    bare = set()
    sublabeled = set()
    for origin_rows in rows.values():
        for _note_ref, label, _sublabel_name, sublabel_value in origin_rows:
            if not label:
                continue
            elif sublabel_value:
                sublabeled.add(label)
            else:
                bare.add(label)

    if mixed := bare & sublabeled:
        rich.print(
            "Warning: these labels appear both with and without a sublabel: "
            f"{', '.join(sorted(mixed))}. "
            "Chart Review drops the bare mentions of such a label from every annotator "
            "(human reviewers included), so those will not be scored."
        )


def _ref_for_span(note: LabelStudioNote, span: tuple[int, int]) -> str | None:
    """Which of the notes inside this (possibly grouped) chart does this highlight sit in?"""
    # Grouping offsets highlight spans and doc spans by the same amount, so this still holds
    # after group_notes_by_unique_id() has concatenated several notes together.
    for ref, (start, stop) in note.doc_spans.items():
        if start <= span[0] < stop:
            return ref
    return None  # pragma: no cover - highlights are built from doc-relative spans


def _write_one_file(path: cfs.FsPath, rows: Iterable[tuple]) -> None:
    rows = list(rows)
    columns = [ID_COLUMN, *LABEL_COLUMNS]
    # Chart Review errors out on a sublabel_name column without a sublabel_value column, and empty
    # sublabel columns are just noise for origins that don't use them - so only write the pair
    # when this origin actually has sublabels.
    has_sublabels = any(row[2] or row[3] for row in rows)
    if has_sublabels:
        columns += SUBLABEL_COLUMNS

    with path.open("w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(columns)
        for row in rows:
            # Dropping the sublabel columns here loses nothing to warn about: we only drop them
            # when no row in this file had a sublabel in the first place. Mentions we did have to
            # alter or discard are reported by _warn_about_skips().
            writer.writerow(row if has_sublabels else row[:2])


def _slugify(origin: str) -> str:
    """Converts a label origin into something safe to put in a filename"""
    # Same treatment we give sublabel data column names in labelstudio.py
    slug = NON_ALPHANUM.sub("_", origin.casefold())
    slug = UNDERSCORES.sub("_", slug).strip("_")
    return slug or "unknown"
