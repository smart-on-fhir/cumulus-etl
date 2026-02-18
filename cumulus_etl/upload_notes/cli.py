"""Aid manual chart review by sending docs to Label Studio"""

import argparse
import dataclasses
import datetime
from collections.abc import Callable, Collection

from cumulus_etl import cli_utils, common, deid, errors, fhir, nlp, store
from cumulus_etl.upload_notes import labeling, selector
from cumulus_etl.upload_notes.labelstudio import LabelStudioClient, LabelStudioNote

PHILTER_DISABLE = "disable"
PHILTER_REDACT = "redact"
PHILTER_LABEL = "label"


def init_checks(args: argparse.Namespace):
    """Do any external service checks necessary at the start"""
    if args.skip_init_checks:
        return

    if not cli_utils.is_url_available(args.label_studio_url, retry=False):
        errors.fatal(
            f"A running Label Studio server was not found at:\n    {args.label_studio_url}",
            errors.LABEL_STUDIO_MISSING,
        )


async def gather_resources(
    root_input: store.Root,
    codebook: deid.Codebook,
    args: argparse.Namespace,
) -> common.Directory:
    """Selects and downloads just the docrefs we need to an export folder."""
    common.print_header("Gathering documents...")

    note_filter = nlp.get_note_filter(args)

    return await selector.select_resources_from_files(
        root_input,
        codebook,
        note_filter=note_filter,
        export_to=args.export_to,
    )


def _get_encounter_id(resource: dict) -> str | None:
    encounter_ref = None
    if resource["resourceType"] == "DiagnosticReport":
        encounter_ref = resource.get("encounter")
    elif resource["resourceType"] == "DocumentReference":
        encounter_refs = resource.get("context", {}).get("encounter", [])
        encounter_ref = encounter_refs and encounter_refs[0]  # just use first encounter

    try:
        return fhir.unref_resource(encounter_ref)[1]
    except ValueError:
        return None


def _get_unique_id_for_encounter_grouping(resource: dict) -> str:
    """Prefers encounter ID, but falls back to resource ref if no encounter"""
    if encounter_id := _get_encounter_id(resource):
        return encounter_id
    else:
        return f"{resource['resourceType']}/{resource['id']}"


def _get_unique_id_for_no_grouping(resource: dict) -> str:
    """Uses resource ref to keep all notes separated"""
    return f"{resource['resourceType']}/{resource['id']}"


async def read_notes_from_ndjson(
    dirname: str,
    codebook: deid.Codebook,
    *,
    src_dir: str,
    get_unique_id: Callable[[dict], str],
) -> list[LabelStudioNote]:
    common.print_header("Downloading note text...")

    # Grab notes
    root = store.Root(dirname)
    resources = common.read_resource_ndjson(root, {"DiagnosticReport", "DocumentReference"})

    # Now bundle each note together with some metadata and ID mappings
    notes = []
    for resource in resources:
        note_ref = f"{resource['resourceType']}/{resource['id']}"

        try:
            text = fhir.get_clinical_note(resource)
        except Exception:
            print(f"Skipping {note_ref}: no text found.")
            continue

        # Grab a bunch of the metadata
        unique_id = get_unique_id(resource)
        patient_id = resource.get("subject", {}).get("reference", "").removeprefix("Patient/")
        anon_patient_id = patient_id and codebook.fake_id("Patient", patient_id)
        encounter_id = _get_encounter_id(resource)
        anon_encounter_id = encounter_id and codebook.fake_id("Encounter", encounter_id)
        anon_note_id = codebook.fake_id(resource["resourceType"], resource["id"])
        doc_mappings = {note_ref: f"{resource['resourceType']}/{anon_note_id}"}
        doc_spans = {note_ref: (0, len(text))}
        date = nlp.get_note_date(resource)

        # Prepare header
        header = "########################################\n"

        # Header: resource type
        if resource["resourceType"] == "DiagnosticReport":
            header += "# Diagnostic Report\n"
        else:
            header += "# Document\n"  # keep it generic (and no "Reference" - we have the note!)

        # Header: type
        doctype = "unknown"
        if resource["resourceType"] == "DiagnosticReport":
            doctype = fhir.get_concept_user_text(resource.get("code")) or doctype
        elif resource["resourceType"] == "DocumentReference":
            doctype = fhir.get_concept_user_text(resource.get("type")) or doctype
        header += f"# Type: {doctype}\n"

        # Header: date
        if not date:
            date_string = "unknown"
        elif date.tzinfo:
            # aware datetime, with hours/minutes (using original timezone, not local)
            date_string = f"{date:%x %X}"  # locale-based date + time
        else:
            # non-aware datetime, only show the date (fhir spec says times must have timezones)
            date_string = f"{date:%x}"  # locale-based date
        header += f"# Date: {date_string}\n"

        # Header: roles
        roles = fhir.get_clinical_note_role_info(resource, src_dir)
        header += "\n".join(f"# {line}" for line in roles.splitlines()) + "\n"

        header += "########################################"

        notes.append(
            LabelStudioNote(
                unique_id,
                patient_id,
                anon_patient_id,
                encounter_id,
                anon_encounter_id,
                doc_mappings=doc_mappings,
                doc_spans=doc_spans,
                header=header,
                text=text,
                date=date,
            )
        )

    return notes


def philter_notes(notes: Collection[LabelStudioNote], args: argparse.Namespace) -> None:
    if args.philter == PHILTER_DISABLE:
        return

    common.print_header("Running philter...")
    philter = deid.Philter()
    for note in notes:
        if args.philter == PHILTER_LABEL:
            note.philter_map = philter.detect_phi(note.text).get_complement(note.text)
        else:
            note.text = philter.scrub_text(note.text)


def sort_notes(notes: Collection[LabelStudioNote]) -> Collection[LabelStudioNote]:
    """Ensure notes are ordered in a convenient way for chart review"""
    # Sort notes by date (putting Nones last)
    notes = sorted(notes, key=lambda x: (x.date or datetime.datetime.max).timestamp())

    # Now gather the first time each patient and encounter appear in the date-sorted list.
    # This will help us sort the groups and patients by time, so that a chart reviewer will
    # have a more natural reading of encounters.
    def gather_firsts(field: str) -> dict[str, int]:
        firsts = {}
        for index, note in enumerate(notes):
            value = getattr(note, field)
            adjusted_index = index if value else len(notes)  # put None values last
            firsts.setdefault(value, adjusted_index)
        return firsts

    encounter_firsts = gather_firsts("encounter_id")
    patient_firsts = gather_firsts("patient_id")

    # Group up by encounter (and order groups among themselves by earliest)
    # (This only matters if we aren't merging notes by encounter later down the line.)
    notes = sorted(notes, key=lambda x: (encounter_firsts[x.encounter_id], x.encounter_id))

    # Group up by patient (and order patients among themselves by earliest)
    notes = sorted(notes, key=lambda x: (patient_firsts[x.patient_id], x.patient_id))

    return notes


def group_notes_by_unique_id(notes: Collection[LabelStudioNote]) -> list[LabelStudioNote]:
    """
    Gather all notes with the same unique ID together into one note.
    """
    grouped_notes = []

    # Group up docs & notes by their unique IDs
    by_unique_id = {}
    for note in notes:
        by_unique_id.setdefault(note.unique_id, []).append(note)

    # Group up the text into one big note
    for unique_id, group_notes in by_unique_id.items():
        grouped_text = ""
        grouped_highlights = []
        grouped_philter_map = {}
        grouped_doc_mappings = {}
        grouped_doc_spans = {}

        for note in group_notes:
            grouped_doc_mappings.update(note.doc_mappings)

            if grouped_text:
                grouped_text += "\n\n\n"
            grouped_text += f"{note.header}\n\n\n"
            offset = len(grouped_text)
            grouped_text += note.text

            offset_doc_spans = {
                k: (v[0] + offset, v[1] + offset) for k, v in note.doc_spans.items()
            }
            grouped_doc_spans.update(offset_doc_spans)

            for highlight in note.highlights:
                span = highlight.span
                new_span = (span[0] + offset, span[1] + offset)
                grouped_highlights.append(dataclasses.replace(highlight, span=new_span))

            for start, stop in note.philter_map.items():
                grouped_philter_map[start + offset] = stop + offset

        grouped_notes.append(
            LabelStudioNote(
                unique_id,
                group_notes[0].patient_id,
                group_notes[0].anon_patient_id,
                group_notes[0].encounter_id,
                group_notes[0].anon_encounter_id,
                text=grouped_text,
                date=group_notes[0].date,
                doc_mappings=grouped_doc_mappings,
                doc_spans=grouped_doc_spans,
                highlights=grouped_highlights,
                philter_map=grouped_philter_map,
            )
        )

    return grouped_notes


async def push_to_label_studio(
    notes: Collection[LabelStudioNote], access_token: str, args: argparse.Namespace
) -> None:
    common.print_header(f"Pushing {len(notes):,} charts to Label Studio.")
    ls_client = LabelStudioClient(args.label_studio_url, access_token, args.ls_project)
    await ls_client.push_tasks(notes, overwrite=args.overwrite)


#####################################################################################################################
#
# Main
#
#####################################################################################################################


def define_upload_notes_parser(parser: argparse.ArgumentParser) -> None:
    parser.usage = "cumulus-etl upload-notes [OPTION]... INPUT LS_URL PHI"

    parser.add_argument("dir_input", metavar="/path/to/input")
    parser.add_argument("label_studio_url", metavar="https://example.com/labelstudio")
    parser.add_argument("dir_phi", metavar="/path/to/phi")

    parser.add_argument(
        "--export-to",
        metavar="PATH",
        help="Where to put exported documents (default is to delete after use)",
    )

    parser.add_argument(
        "--philter",
        choices=[PHILTER_DISABLE, PHILTER_REDACT, PHILTER_LABEL],
        default=PHILTER_REDACT,
        help="Whether to use philter to redact/tag PHI (default is redact)",
    )
    # Old, simpler version of the above (feel free to remove after May 2024)
    parser.add_argument(
        "--no-philter",
        action="store_const",
        const=PHILTER_DISABLE,
        dest="philter",
        help=argparse.SUPPRESS,
    )

    parser.add_argument(
        "--grouping",
        choices=["encounter", "none"],
        default="encounter",
        help="how to group together notes into one Label Studio task (default is encounter)",
    )

    cli_utils.add_aws(parser, athena=True)
    cli_utils.add_auth(parser)

    group = parser.add_argument_group("labeling")
    group.add_argument(
        "--highlight-by-word",
        "--highlight",  # old alias, let's keep around whynot
        action="append",
        metavar="WORD",
        help="annotate the provided word (can be specified multiple times or comma separated, "
        "will only match whole words/phrases)",
    )
    group.add_argument(
        "--highlight-by-regex",
        action="append",
        metavar="REGEX",
        help="annotate the provided word regex (can be specified multiple times, "
        "will only match whole words/phrases)",
    )
    group.add_argument(
        "--label-by-csv",
        metavar="FILE",
        help="path to a .csv file with annotations (must have note ID, label, and span columns)",
    )
    group.add_argument(
        "--label-by-anon-csv",
        metavar="FILE",
        help="path to a .csv file with annotations "
        "(must have anonymized note ID, label, and span columns)",
    )
    group.add_argument(
        "--label-by-athena-table",
        metavar="DB.TABLE",
        help="name of an Athena table with annotations "
        "(must have note ID, label, and span columns)",
    )

    group = nlp.add_note_selection(parser)
    # Add some deprecated aliases for some note selection options. Deprecated since Sep 2025.
    group.add_argument("--anon-docrefs", dest="select_by_anon_csv", help=argparse.SUPPRESS)
    group.add_argument("--docrefs", dest="select_by_csv", help=argparse.SUPPRESS)

    group = parser.add_argument_group("Label Studio")
    group.add_argument(
        "--ls-token", metavar="PATH", help="token file for Label Studio access", required=True
    )
    group.add_argument(
        "--ls-project",
        metavar="ID",
        type=int,
        help="Label Studio project ID to update",
        required=True,
    )
    group.add_argument(
        "--overwrite", action="store_true", help="whether to overwrite an existing task for a note"
    )

    cli_utils.add_debugging(parser)


async def upload_notes_main(args: argparse.Namespace) -> None:
    """
    Prepare for chart review by uploading some documents to Label Studio.

    There are three major steps:
    1. Gather requested resources, reverse-engineering the original IDs if necessary
    2. Run Philter
    3. Upload to Label Studio
    """
    init_checks(args)

    # record filesystem options like --s3-region before creating Roots
    store.set_user_fs_options(vars(args))

    args.dir_input = cli_utils.process_input_dir(args.dir_input)
    root_input = store.Root(args.dir_input)
    store.Root(args.dir_phi, create=True)  # create PHI if needed (very edge case)

    # Read token file early for quick error feedback
    access_token = common.read_text(args.ls_token).strip()

    match args.grouping:
        case "encounter":
            get_unique_id = _get_unique_id_for_encounter_grouping
        case _:
            get_unique_id = _get_unique_id_for_no_grouping

    common.print_header()
    print("Preparing metadata…")  # i.e. the codebook

    with deid.Codebook(args.dir_phi) as codebook:
        ndjson_folder = await gather_resources(root_input, codebook, args)
        notes = await read_notes_from_ndjson(
            ndjson_folder.name,
            codebook,
            get_unique_id=get_unique_id,
            src_dir=args.dir_input,
        )

        await labeling.add_labels(codebook, notes, args)

    # It's safe to philter notes after labeling because philter does not change character counts
    philter_notes(notes, args)
    notes = sort_notes(notes)
    notes = group_notes_by_unique_id(notes)
    await push_to_label_studio(notes, access_token, args)


async def run_upload_notes(parser: argparse.ArgumentParser, argv: list[str]) -> None:
    """Parses an upload-notes CLI"""
    define_upload_notes_parser(parser)
    args = parser.parse_args(argv)
    await upload_notes_main(args)
