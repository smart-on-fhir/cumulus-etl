"""Aid manual chart review by sending docs to Label Studio"""

import argparse
import asyncio
import datetime
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
            f"A running Label Studio server was not found at:\n    {args.label_studio_url}", errors.LABEL_STUDIO_MISSING
        )


async def gather_docrefs(
    client: fhir.FhirClient, root_input: store.Root, codebook: deid.Codebook, args: argparse.Namespace
) -> common.Directory:
    """Selects and downloads just the docrefs we need to an export folder."""
    common.print_header("Gathering documents...")

    # There are three possibilities: we have real IDs, fake IDs, or neither.
    # Note that we don't support providing both real & fake IDs right now. It's not clear that would be useful.
    if args.docrefs and args.anon_docrefs:
        errors.fatal("You cannot use both --docrefs and --anon-docrefs at the same time.", errors.ARGS_CONFLICT)

    if root_input.protocol == "https":  # is this a FHIR server?
        return await downloader.download_docrefs_from_fhir_server(
            client, root_input, codebook, docrefs=args.docrefs, anon_docrefs=args.anon_docrefs, export_to=args.export_to
        )
    else:
        return selector.select_docrefs_from_files(
            root_input, codebook, docrefs=args.docrefs, anon_docrefs=args.anon_docrefs, export_to=args.export_to
        )


def datetime_from_docref(docref: dict) -> datetime.datetime | None:
    """Returns the date of a docref - preferring `context.period.start`, then `date`"""
    if start := fhir.parse_datetime(docref.get("context", {}).get("period", {}).get("start")):
        return start
    if date := fhir.parse_datetime(docref.get("date")):
        return date
    return None


async def read_notes_from_ndjson(
    client: fhir.FhirClient, dirname: str, codebook: deid.Codebook
) -> list[LabelStudioNote]:
    common.print_header("Downloading note text...")

    # Download all the doc notes (and save some metadata about each)
    encounter_ids = []
    docrefs = []
    coroutines = []
    for docref in common.read_resource_ndjson(store.Root(dirname), "DocumentReference"):
        encounter_refs = docref.get("context", {}).get("encounter", [])
        if not encounter_refs:
            # If a note doesn't have an encounter - we can't group it with other docs
            print(f"Skipping DocumentReference {docref['id']} as it lacks a linked encounter.")
            continue

        encounter_ids.append(fhir.unref_resource(encounter_refs[0])[1])  # just use first encounter
        docrefs.append(docref)
        coroutines.append(fhir.get_docref_note(client, docref))
    note_texts = await asyncio.gather(*coroutines)

    # Now bundle each note together with some metadata and ID mappings
    notes = []
    for i, text in enumerate(note_texts):
        default_title = "Document"
        codings = docrefs[i].get("type", {}).get("coding", [])
        title = codings[0].get("display", default_title) if codings else default_title

        enc_id = encounter_ids[i]
        anon_enc_id = codebook.fake_id("Encounter", enc_id)
        doc_id = docrefs[i]["id"]
        doc_mappings = {doc_id: codebook.fake_id("DocumentReference", doc_id)}
        doc_spans = {doc_id: (0, len(text))}

        notes.append(
            LabelStudioNote(
                enc_id,
                anon_enc_id,
                doc_mappings=doc_mappings,
                doc_spans=doc_spans,
                title=title,
                text=text,
                date=datetime_from_docref(docrefs[i]),
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
        note.ctakes_matches = [match for i, match in enumerate(matches) if cnlpt_results[i] == Polarity.pos]


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


def group_notes_by_encounter(notes: Collection[LabelStudioNote]) -> list[LabelStudioNote]:
    """
    Gather all notes with the same encounter ID together into one note.

    Reviewers seem to prefer that.
    """
    grouped_notes = []

    # Group up docs & notes by encounter
    by_encounter_id = {}
    for note in notes:
        by_encounter_id.setdefault(note.enc_id, []).append(note)

    # Group up the text into one big note
    for enc_id, enc_notes in by_encounter_id.items():
        grouped_text = ""
        grouped_ctakes_matches = []
        grouped_philter_map = {}
        grouped_doc_mappings = {}
        grouped_doc_spans = {}

        # Sort notes by date (putting Nones last)
        enc_notes = sorted(enc_notes, key=lambda x: (x.date or datetime.datetime.max).timestamp())

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

            offset_doc_spans = {k: (v[0] + offset, v[1] + offset) for k, v in note.doc_spans.items()}
            grouped_doc_spans.update(offset_doc_spans)

            for match in note.ctakes_matches:
                match.begin += offset
                match.end += offset
                grouped_ctakes_matches.append(match)

            for start, stop in note.philter_map.items():
                grouped_philter_map[start + offset] = stop + offset

        grouped_notes.append(
            LabelStudioNote(
                enc_id,
                enc_notes[0].anon_id,
                text=grouped_text,
                doc_mappings=grouped_doc_mappings,
                doc_spans=grouped_doc_spans,
                ctakes_matches=grouped_ctakes_matches,
                philter_map=grouped_philter_map,
            )
        )

    return grouped_notes


def push_to_label_studio(
    notes: Collection[LabelStudioNote], access_token: str, labels: dict, args: argparse.Namespace
) -> None:
    common.print_header("Pushing notes to Label Studio...")
    ls_client = LabelStudioClient(args.label_studio_url, access_token, args.ls_project, labels)
    ls_client.push_tasks(notes, overwrite=args.overwrite)


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
        "--export-to", metavar="PATH", help="Where to put exported documents (default is to delete after use)"
    )

    parser.add_argument(
        "--philter",
        choices=[PHILTER_DISABLE, PHILTER_REDACT, PHILTER_LABEL],
        default=PHILTER_REDACT,
        help="Whether to use philter to redact/tag PHI",
    )
    # Old, simpler version of the above (feel free to remove after May 2024)
    parser.add_argument(
        "--no-philter", action="store_const", const=PHILTER_DISABLE, dest="philter", help=argparse.SUPPRESS
    )

    cli_utils.add_aws(parser)
    cli_utils.add_auth(parser)

    docs = parser.add_argument_group("document selection")
    docs.add_argument("--anon-docrefs", metavar="PATH", help="CSV file with anonymized patient_id,docref_id columns")
    docs.add_argument("--docrefs", metavar="PATH", help="CSV file with a docref_id column of original IDs")

    group = cli_utils.add_nlp(parser)
    group.add_argument(
        "--symptoms-bsv",
        metavar="PATH",
        help="BSV file with concept CUIs (defaults to Covid)",
        default=ctakesclient.filesystem.covid_symptoms_path(),
    )
    group.add_argument("--no-nlp", action="store_false", dest="nlp", default=True, help="Don’t run NLP on notes")

    group = parser.add_argument_group("Label Studio")
    group.add_argument("--ls-token", metavar="PATH", help="Token file for Label Studio access", required=True)
    group.add_argument("--ls-project", metavar="ID", type=int, help="Label Studio project ID to update", required=True)
    group.add_argument("--overwrite", action="store_true", help="Whether to overwrite an existing task for a note")

    cli_utils.add_debugging(parser)


async def upload_notes_main(args: argparse.Namespace) -> None:
    """
    Prepare for chart review by uploading some documents to Label Studio.

    There are three major steps:
    1. Gather requested DocumentReference resources, reverse-engineering the original docref IDs if necessary
    2. Run NLP
    3. Run Philter
    4. Upload to Label Studio
    """
    init_checks(args)

    store.set_user_fs_options(vars(args))  # record filesystem options like --s3-region before creating Roots
    root_input = store.Root(args.dir_input)
    codebook = deid.Codebook(args.dir_phi)

    # Auth & read files early for quick error feedback
    client = fhir.create_fhir_client_for_cli(args, root_input, ["DocumentReference"])
    access_token = common.read_text(args.ls_token).strip()
    labels = ctakesclient.filesystem.map_cui_pref(args.symptoms_bsv)

    async with client:
        ndjson_folder = await gather_docrefs(client, root_input, codebook, args)
        notes = await read_notes_from_ndjson(client, ndjson_folder.name, codebook)

    await run_nlp(notes, args)
    philter_notes(notes, args)  # safe to do after NLP because philter does not change character counts
    notes = group_notes_by_encounter(notes)
    push_to_label_studio(notes, access_token, labels, args)


async def run_upload_notes(parser: argparse.ArgumentParser, argv: list[str]) -> None:
    """Parses an upload-notes CLI"""
    define_upload_notes_parser(parser)
    args = parser.parse_args(argv)
    await upload_notes_main(args)
