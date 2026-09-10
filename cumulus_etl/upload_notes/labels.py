"""Writing chart-review label files for the notes we upload"""

import csv
from collections.abc import Collection, Iterable

import cumulus_fhir_support as cfs
import rich

from cumulus_etl.upload_notes.labelstudio import NON_ALPHANUM, UNDERSCORES, LabelStudioNote

# Chart Review picks one ID column per file and prepends a resource type to every value in it,
# based on the column's name. So DocumentReference and DiagnosticReport rows can't share a file -
# we write one file per resource type, with the column name that Chart Review looks for.
# See https://docs.smarthealthit.org/cumulus/chart-review/config.html
ID_COLUMNS = {
    "DocumentReference": "docref_id",
    "DiagnosticReport": "diagnosticreport_id",
}

# Only the DiagnosticReport file gets a suffix, so that the common (DocumentReference-only) case
# keeps a stable, predictable filename to point a Chart Review config at.
FILENAME_SUFFIXES = {
    "DocumentReference": "",
    "DiagnosticReport": "-dxreport",
}

LABEL_COLUMNS = ["label"]
SUBLABEL_COLUMNS = ["sublabel_name", "sublabel_value"]


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

    rows = _gather_rows(notes)
    if not rows:
        rich.print("No labels were found, so no label files were written.")
        return

    export_labels_to.makedirs()
    _warn_about_mixed_sublabels(rows)

    for (origin, res_type), origin_rows in sorted(rows.items()):
        if res_type not in ID_COLUMNS:  # pragma: no cover - we only ever read the two note types
            continue
        filename = f"labels-{_slugify(origin)}{FILENAME_SUFFIXES[res_type]}.csv"
        _write_one_file(export_labels_to.joinpath(filename), res_type, origin_rows)
        rich.print(f"Wrote {len(origin_rows):,} label rows to {filename}")


def _gather_rows(notes: Collection[LabelStudioNote]) -> dict[tuple[str, str], list[tuple]]:
    """Returns {(origin, resource type): [(id, label, sublabel name, sublabel value)]}"""
    # Which notes did we upload, in upload order, split up by resource type?
    # (A grouped chart holds several notes, and can even mix resource types.)
    ids_by_type = {}
    for note in notes:
        for ref in note.doc_mappings:
            res_type, res_id = ref.split("/", 1)
            ids_by_type.setdefault(res_type, []).append(res_id)

    # Now gather up the labels each origin gave to each of those notes.
    labels = {}  # (origin, resource type) -> res_id -> {(label, sublabel name, sublabel value)}
    dropped = set()  # labels we can't represent at all (see the "|" note below)
    for note in notes:
        for highlight in note.highlights:
            ref = _ref_for_span(note, highlight.span)
            if ref is None:  # pragma: no cover - highlights are built from doc-relative spans
                continue
            res_type, res_id = ref.split("/", 1)
            # Chart Review rejects a sublabel name that has no value, so only count a sublabel
            # when we have both halves - our NLP sources read the two columns independently.
            if highlight.sublabel_name and highlight.sublabel_value:
                sublabel = (highlight.sublabel_name, highlight.sublabel_value)
            else:
                sublabel = ("", "")
            # "|" is Chart Review's own label/sublabel delimiter and it refuses to load a file
            # containing one, so a label from an NLP model can render the whole file unreadable.
            # Drop just that mention rather than the file. (Sublabel values may contain "|".)
            if "|" in highlight.label or "|" in sublabel[0]:
                dropped.add(highlight.label if "|" in highlight.label else sublabel[0])
                continue
            found = labels.setdefault((highlight.origin, res_type), {})
            # A label often appears at several spans in one note, but Chart Review compares
            # per-note label sets, so collapse those down to one row.
            found.setdefault(res_id, set()).add((highlight.label, *sublabel))

    rows = {}
    for key, found in labels.items():
        res_type = key[1]
        origin_rows = []
        for res_id in ids_by_type[res_type]:
            if note_labels := found.get(res_id):
                origin_rows.extend((res_id, *label) for label in sorted(note_labels))
            else:
                origin_rows.append((res_id, "", "", ""))
        rows[key] = origin_rows

    if dropped:
        rich.print(
            "Warning: these labels contain a '|', which Chart Review reserves as its "
            f"label/sublabel delimiter: {', '.join(sorted(dropped))}. "
            "They have been left out of the label files - Chart Review would refuse to load a "
            "file containing one - so those mentions will not be scored."
        )

    return rows


def _warn_about_mixed_sublabels(rows: dict[tuple[str, str], list[tuple]]) -> None:
    """
    Warns about labels we emit both bare and with a sublabel.

    Once any annotator uses a sublabel for a label, Chart Review treats every bare mention of
    that label as invalid and drops it - across every annotator, including the human reviewers.
    That silently deflates a score, so it's worth flagging while we can still point at the label.
    """
    bare = set()
    sublabeled = set()
    for origin_rows in rows.values():
        for _res_id, label, _sublabel_name, sublabel_value in origin_rows:
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


def _write_one_file(path: cfs.FsPath, res_type: str, rows: Iterable[tuple]) -> None:
    rows = list(rows)
    columns = [ID_COLUMNS[res_type], *LABEL_COLUMNS]
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
            writer.writerow(row if has_sublabels else row[:2])


def _slugify(origin: str) -> str:
    """Converts a label origin into something safe to put in a filename"""
    # Same treatment we give sublabel data column names in labelstudio.py
    slug = NON_ALPHANUM.sub("_", origin.casefold())
    slug = UNDERSCORES.sub("_", slug).strip("_")
    return slug or "unknown"
