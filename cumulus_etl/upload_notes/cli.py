"""Aid manual chart review by sending docs to Label Studio"""

import argparse
import asyncio
import datetime
import re
import sys
from collections.abc import Collection

import ctakesclient
from ctakesclient.typesystem import Polarity

from cumulus_etl import cli_utils, common, deid, errors, fhir, nlp, store
from cumulus_etl.upload_notes import downloader, selector
from cumulus_etl.upload_notes.labelstudio import LabelStudioClient, LabelStudioNote

PHILTER_DISABLE = "disable"
PHILTER_REDACT = "redact"
PHILTER_LABEL = "label"


def init_checks(args: argparse.Namespace):
    """Do any external service checks necessary at the start"""
    if args.skip_init_checks:
        return

    if args.nlp:
        nlp.check_ctakes()
        nlp.check_negation_cnlpt()

    if not cli_utils.is_url_available(args.label_studio_url, retry=False):
        errors.fatal(
            f"A running Label Studio server was not found at:\n    {args.label_studio_url}",
            errors.LABEL_STUDIO_MISSING,
        )


async def gather_resources(
    client: fhir.FhirClient,
    root_input: store.Root,
    codebook: deid.Codebook,
    args: argparse.Namespace,
) -> common.Directory:
    """Selects and downloads just the docrefs we need to an export folder."""
    common.print_header("Gathering documents...")

    # There are three possibilities: we have real IDs, fake IDs, or neither.
    # Note that we don't support providing both real & fake IDs right now. It's not clear that would be useful.
    if args.docrefs and args.anon_docrefs:
        errors.fatal(
            "You cannot use both --docrefs and --anon-docrefs at the same time.",
            errors.ARGS_CONFLICT,
        )

    if root_input.protocol == "https":  # is this a FHIR server?
        return await downloader.download_resources_from_fhir_server(
            client,
            root_input,
            codebook,
            id_file=args.docrefs,
            anon_id_file=args.anon_docrefs,
            export_to=args.export_to,
        )
    else:
        return selector.select_resources_from_files(
            root_input,
            codebook,
            id_file=args.docrefs,
            anon_id_file=args.anon_docrefs,
            export_to=args.export_to,
        )


def datetime_from_resource(resource: dict) -> datetime.datetime | None:
    """Returns the date of a resource - preferring clinical dates, then administrative ones"""
    if resource["resourceType"] == "DiagnosticReport":
        if time := fhir.parse_datetime(resource.get("effectiveDateTime")):
            return time
        if time := fhir.parse_datetime(resource.get("effectivePeriod", {}).get("start")):
            return time
        if time := fhir.parse_datetime(resource.get("issued")):
            return time
    elif resource["resourceType"] == "DocumentReference":
        if time := fhir.parse_datetime(resource.get("context", {}).get("period", {}).get("start")):
            return time
        if time := fhir.parse_datetime(resource.get("date")):
            return time
    return None


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


async def read_notes_from_ndjson(
    client: fhir.FhirClient, dirname: str, codebook: deid.Codebook
) -> list[LabelStudioNote]:
    common.print_header("Downloading note text...")

    # Download all the doc notes (and save some metadata about each)
    encounter_ids = []
    resources = []
    coroutines = []

    # Grab notes
    root = store.Root(dirname)
    for resource in common.read_resource_ndjson(root, {"DiagnosticReport", "DocumentReference"}):
        encounter_id = _get_encounter_id(resource)
        if not encounter_id:
            # If a note doesn't have an encounter - we can't group it with other docs
            print(f"Skipping {resource['resourceType']}/{resource['id']}: No linked encounter.")
            continue
        encounter_ids.append(encounter_id)
        resources.append(resource)
        coroutines.append(fhir.get_clinical_note(client, resource))

    note_texts = await asyncio.gather(*coroutines, return_exceptions=True)

    # Now bundle each note together with some metadata and ID mappings
    notes = []
    for i, text in enumerate(note_texts):
        resource = resources[i]
        note_ref = f"{resource['resourceType']}/{resource['id']}"

        if isinstance(text, Exception):
            print(f"Skipping {note_ref}: {text}")
            continue

        default_title = "Document"
        codings = []
        if resource["resourceType"] == "DiagnosticReport":
            codings = resource.get("code", {}).get("coding", [])
        elif resource["resourceType"] == "DocumentReference":
            codings = resource.get("type", {}).get("coding", [])
        title = codings[0].get("display", default_title) if codings else default_title

        patient_id = resource.get("subject", {}).get("reference", "").removeprefix("Patient/")
        anon_patient_id = codebook.fake_id("Patient", patient_id)
        encounter_id = encounter_ids[i]
        anon_encounter_id = codebook.fake_id("Encounter", encounter_id)
        anon_note_id = codebook.fake_id(resource["resourceType"], resource["id"])
        doc_mappings = {note_ref: f"{resource['resourceType']}/{anon_note_id}"}
        doc_spans = {note_ref: (0, len(text))}

        notes.append(
            LabelStudioNote(
                patient_id,
                anon_patient_id,
                encounter_id,
                anon_encounter_id,
                doc_mappings=doc_mappings,
                doc_spans=doc_spans,
                title=title,
                text=text,
                date=datetime_from_resource(resource),
            )
        )

    return notes


async def run_nlp(notes: Collection[LabelStudioNote], args: argparse.Namespace) -> None:
    if not args.nlp:
        return

    common.print_header("Running notes through cTAKES...")
    if not nlp.restart_ctakes_with_bsv(args.ctakes_overrides, args.symptoms_bsv):
        sys.exit(errors.CTAKES_OVERRIDES_INVALID)

    http_client = nlp.ctakes_httpx_client()

    # Run each note through cTAKES then the cNLP transformer for negation
    for note in notes:
        ctakes_json = await ctakesclient.client.extract(note.text, client=http_client)
        matches = ctakes_json.list_match(polarity=Polarity.pos)
        spans = ctakes_json.list_spans(matches)
        cnlpt_results = await ctakesclient.transformer.list_polarity(
            note.text,
            spans,
            client=http_client,
            model=nlp.TransformerModel.NEGATION,
        )
        note.ctakes_matches = [
            match for i, match in enumerate(matches) if cnlpt_results[i] == Polarity.pos
        ]


def add_highlights(notes: Collection[LabelStudioNote], args: argparse.Namespace) -> None:
    highlights = list(cli_utils.expand_comma_list_arg(args.highlight))
    if not highlights:
        return

    common.print_header("Highlighting notes...")

    # Make a custom version of \b that allows non-word characters to be on edge of the term too.
    # For example:
    #   This misses: re.match(r"\ba\+\b", "a+")
    #   But this hits: re.match(r"\ba\+", "a+")
    # So to work around that, we look for the word boundary ourselves.
    edge = r"(\W|$|^)"
    escaped_terms = [re.escape(term) for term in highlights]
    re_terms = [re.compile(f"{edge}({term}){edge}", re.IGNORECASE) for term in escaped_terms]

    for note in notes:
        for term in re_terms:
            for match in term.finditer(note.text):
                # Look at group 2 (the middle term group, ignoring the edge groups)
                note.highlights.append(ctakesclient.typesystem.Span(match.start(2), match.end(2)))


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
            firsts.setdefault(value, index)
        return firsts

    encounter_firsts = gather_firsts("encounter_id")
    patient_firsts = gather_firsts("patient_id")

    # Group up by encounter (and order groups among themselves by earliest)
    # (This only matters in case we ever make the later encounter-bundling step optional.)
    notes = sorted(notes, key=lambda x: (encounter_firsts[x.encounter_id], x.encounter_id))

    # Group up by patient (and order patients among themselves by earliest)
    notes = sorted(notes, key=lambda x: (patient_firsts[x.patient_id], x.patient_id))

    return notes


def group_notes_by_encounter(notes: Collection[LabelStudioNote]) -> list[LabelStudioNote]:
    """
    Gather all notes with the same encounter ID together into one note.

    Reviewers seem to prefer that.
    """
    grouped_notes = []

    # Group up docs & notes by encounter
    by_encounter_id = {}
    for note in notes:
        by_encounter_id.setdefault(note.encounter_id, []).append(note)

    # Group up the text into one big note
    for enc_id, enc_notes in by_encounter_id.items():
        grouped_text = ""
        grouped_ctakes_matches = []
        grouped_highlights = []
        grouped_philter_map = {}
        grouped_doc_mappings = {}
        grouped_doc_spans = {}

        for note in enc_notes:
            grouped_doc_mappings.update(note.doc_mappings)

            if not note.date:
                date_string = "Unknown time"
            elif note.date.tzinfo:
                # aware datetime, with hours/minutes (using original timezone, not local)
                date_string = f"{note.date:%x %X}"  # locale-based date + time
            else:
                # non-aware datetime, only show the date (fhir spec says times must have timezones)
                date_string = f"{note.date:%x}"  # locale-based date

            if grouped_text:
                grouped_text += "\n\n\n"
                grouped_text += "########################################\n########################################\n"
            grouped_text += f"{note.title}\n"
            grouped_text += f"{date_string}\n"
            grouped_text += "########################################\n########################################\n\n\n"
            offset = len(grouped_text)
            grouped_text += note.text

            offset_doc_spans = {
                k: (v[0] + offset, v[1] + offset) for k, v in note.doc_spans.items()
            }
            grouped_doc_spans.update(offset_doc_spans)

            for match in note.ctakes_matches:
                match.begin += offset
                match.end += offset
                grouped_ctakes_matches.append(match)

            for span in note.highlights:
                new_span = ctakesclient.typesystem.Span(span.begin + offset, span.end + offset)
                grouped_highlights.append(new_span)

            for start, stop in note.philter_map.items():
                grouped_philter_map[start + offset] = stop + offset

        grouped_notes.append(
            LabelStudioNote(
                enc_notes[0].patient_id,
                enc_notes[0].anon_patient_id,
                enc_id,
                enc_notes[0].anon_encounter_id,
                text=grouped_text,
                doc_mappings=grouped_doc_mappings,
                doc_spans=grouped_doc_spans,
                ctakes_matches=grouped_ctakes_matches,
                highlights=grouped_highlights,
                philter_map=grouped_philter_map,
            )
        )

    return grouped_notes


async def push_to_label_studio(
    notes: Collection[LabelStudioNote], access_token: str, labels: dict, args: argparse.Namespace
) -> None:
    common.print_header(f"Pushing {len(notes)} charts to Label Studio...")
    ls_client = LabelStudioClient(args.label_studio_url, access_token, args.ls_project, labels)
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
        "--highlight",
        action="append",
        help="annotate the provided word (can be specified multiple times or comma separated)",
    )

    cli_utils.add_aws(parser)
    cli_utils.add_auth(parser)

    docs = parser.add_argument_group("document selection")
    docs.add_argument(
        "--anon-docrefs",
        metavar="PATH",
        help="CSV file with anonymized patient_id,docref_id columns",
    )
    docs.add_argument(
        "--docrefs", metavar="PATH", help="CSV file with a docref_id column of original IDs"
    )

    group = cli_utils.add_nlp(parser)
    group.add_argument(
        "--symptoms-bsv",
        metavar="PATH",
        help="BSV file with concept CUIs (defaults to Covid)",
        default=ctakesclient.filesystem.covid_symptoms_path(),
    )
    group.add_argument(
        "--nlp",
        action=argparse.BooleanOptionalAction,
        default=False,
        help="whether to run NLP on notes (disabled by default)",
    )

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
    2. Run NLP
    3. Run Philter
    4. Upload to Label Studio
    """
    init_checks(args)

    # record filesystem options like --s3-region before creating Roots
    store.set_user_fs_options(vars(args))

    root_input = store.Root(args.dir_input)

    # Auth & read files early for quick error feedback
    client = fhir.create_fhir_client_for_cli(
        args,
        root_input,
        {"DiagnosticReport", "DocumentReference"},
    )
    access_token = common.read_text(args.ls_token).strip()
    labels = ctakesclient.filesystem.map_cui_pref(args.symptoms_bsv)

    async with client:
        with deid.Codebook(args.dir_phi) as codebook:
            ndjson_folder = await gather_resources(client, root_input, codebook, args)
            notes = await read_notes_from_ndjson(client, ndjson_folder.name, codebook)

    await run_nlp(notes, args)
    add_highlights(notes, args)
    # It's safe to philter notes after NLP because philter does not change character counts
    philter_notes(notes, args)
    notes = sort_notes(notes)
    notes = group_notes_by_encounter(notes)
    await push_to_label_studio(notes, access_token, labels, args)


async def run_upload_notes(parser: argparse.ArgumentParser, argv: list[str]) -> None:
    """Parses an upload-notes CLI"""
    define_upload_notes_parser(parser)
    args = parser.parse_args(argv)
    await upload_notes_main(args)
