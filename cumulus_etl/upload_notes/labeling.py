"""Methods for labeling notes"""

import argparse
from collections.abc import Collection

import cumulus_fhir_support as cfs

from cumulus_etl import cli_utils, common, deid, errors, nlp
from cumulus_etl.upload_notes import labelstudio

DEFAULT_ORIGIN = "Cumulus"
DEFAULT_LABEL = "Tag"


async def add_labels(
    codebook: deid.Codebook,
    notes: Collection[labelstudio.LabelStudioNote],
    args: argparse.Namespace,
) -> None:
    # Confirm we don't have conflicting arguments. Which we could maybe combine, as a future
    # improvement, but is too much hassle right now)
    has_csv = bool(args.label_by_csv)
    has_anon_csv = bool(args.label_by_anon_csv)
    has_word = bool(args.highlight_by_word)
    has_regex = bool(args.highlight_by_regex)
    has_athena_table = bool(args.label_by_athena_table)
    arg_count = (
        int(has_csv) + int(has_anon_csv) + int(has_word or has_regex) + int(has_athena_table)
    )
    if arg_count > 1:
        errors.fatal(
            "Multiple labeling arguments provided. Please specify just one.",
            errors.MULTIPLE_LABELING_ARGS,
        )
    elif arg_count == 0:
        return

    common.print_header("Labeling notes...")

    if has_athena_table:
        _label_by_csv(
            codebook, notes, nlp.query_athena_table(args.label_by_athena_table, args), is_anon=True
        )
    elif has_anon_csv:
        _label_by_csv(codebook, notes, args.label_by_anon_csv, is_anon=True)
    elif has_csv:
        _label_by_csv(codebook, notes, args.label_by_csv, is_anon=False)
    elif has_word:
        _highlight_words(notes, args.highlight_by_word, args.highlight_by_regex)


def _check_matches(
    res_type: str, res_id: str, patient_id: str, refs: cfs.RefSet, *, codebook: deid.Codebook | None
) -> set[tuple]:
    # Refs will only have patients if there weren't other resource ID columns
    if refs.has_type("Patient"):
        res_type = "Patient"
        res_id = patient_id

    if codebook:
        res_id = codebook.fake_id(res_type, res_id, caching_allowed=False)

    return refs.get_data_for_id(res_type, res_id)


def _label_by_csv(
    codebook: deid.Codebook,
    notes: Collection[labelstudio.LabelStudioNote],
    csv_file: str,
    *,
    is_anon: bool,
) -> None:
    refs = nlp.get_refs_from_csv(
        csv_file,
        is_anon=is_anon,
        extra_fields=[
            "label",
            "span",
            "sublabel_name",
            "sublabel_value",
            "origin",
        ],
    )

    codebook = codebook if is_anon else None

    for note in notes:
        patient_id = note.patient_id
        for ref, doc_span in note.doc_spans.items():
            res_type, res_id = ref.split("/", 1)
            if matches := _check_matches(res_type, res_id, patient_id, refs, codebook=codebook):
                for match in sorted(matches):
                    label = match[0]
                    span = match[1]
                    sublabel_name = match[2] or None
                    sublabel_value = match[3] or None
                    origin = match[4] or DEFAULT_ORIGIN
                    if "__" in origin:  # if it looks like a table name, chop it down
                        origin = origin.split("__", 1)[-1].removeprefix("nlp_")
                    if label and span and ":" in span:
                        begin, end = span.split(":", 1)
                        span = (int(begin) + doc_span[0], int(end) + doc_span[0])
                        note.highlights.append(
                            labelstudio.Highlight(
                                label,
                                span,
                                origin=origin,
                                sublabel_name=sublabel_name,
                                sublabel_value=sublabel_value,
                            )
                        )


def _highlight_words(
    notes: Collection[labelstudio.LabelStudioNote],
    words: list[str] | None,
    regexes: list[str] | None,
) -> None:
    patterns = []
    if words:
        words = cli_utils.expand_comma_list_arg(words)
        patterns.extend(cli_utils.user_term_to_pattern(term) for term in words)
    if regexes:
        patterns.extend(cli_utils.user_regex_to_pattern(regex) for regex in regexes)

    for note in notes:
        for pattern in patterns:
            for match in pattern.finditer(note.text):
                note.highlights.append(
                    labelstudio.Highlight(
                        # We use a generic default label to cause Label Studio to highlight it
                        label=DEFAULT_LABEL,
                        # Look at group 2 (the middle term group, ignoring the edge groups)
                        span=(match.start(2), match.end(2)),
                        origin=DEFAULT_ORIGIN,
                    )
                )
