"""Aid manual chart review by sending docs to Label Studio"""

import argparse
import asyncio
import sys
from collections.abc import Collection

import ctakesclient
from ctakesclient.typesystem import Polarity

from cumulus_etl import cli_utils, common, deid, errors, fhir_client, fhir_common, loaders, nlp, store
from cumulus_etl.chart_review import downloader, selector
from cumulus_etl.chart_review.labelstudio import LabelStudioClient, LabelStudioNote


def init_checks(args: argparse.Namespace):
    """Do any external service checks necessary at the start"""
    if args.skip_init_checks:
        return

    if args.nlp:
        nlp.check_ctakes()
        nlp.check_cnlpt()

    if not cli_utils.is_url_available(args.label_studio_url, retry=False):
        errors.fatal(
            f"A running Label Studio server was not found at:\n    {args.label_studio_url}", errors.LABEL_STUDIO_MISSING
        )


async def gather_docrefs(
    client: fhir_client.FhirClient, root_input: store.Root, root_phi: store.Root, args: argparse.Namespace
) -> loaders.Directory:
    """Selects and downloads just the docrefs we need to an export folder."""
    common.print_header("Gathering documents...")

    # There are three possibilities: we have real IDs, fake IDs, or neither.
    # Note that we don't support providing both real & fake IDs right now. It's not clear that would be useful.
    if args.docrefs and args.anon_docrefs:
        errors.fatal("You cannot use both --docrefs and --anon-docrefs at the same time.", errors.ARGS_CONFLICT)

    if root_input.protocol == "https":  # is this a FHIR server?
        return await downloader.download_docrefs_from_fhir_server(
            client, root_input, root_phi, docrefs=args.docrefs, anon_docrefs=args.anon_docrefs, export_to=args.export_to
        )
    else:
        return selector.select_docrefs_from_files(
            root_input, root_phi, docrefs=args.docrefs, anon_docrefs=args.anon_docrefs, export_to=args.export_to
        )


async def read_notes_from_ndjson(client: fhir_client.FhirClient, dirname: str) -> list[LabelStudioNote]:
    common.print_header("Downloading note text...")
    docref_ids = []
    coroutines = []
    for docref in common.read_resource_ndjson(store.Root(dirname), "DocumentReference"):
        docref_ids.append(docref["id"])
        coroutines.append(fhir_common.get_docref_note(client, docref))
    note_texts = await asyncio.gather(*coroutines)

    notes = []
    for i, text in enumerate(note_texts):
        notes.append(LabelStudioNote(docref_ids[i], text))

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
        cnlpt_results = await ctakesclient.transformer.list_polarity(note.text, spans, client=http_client)
        note.matches = [match for i, match in enumerate(matches) if cnlpt_results[i] == Polarity.pos]


def philter_notes(notes: Collection[LabelStudioNote], args: argparse.Namespace) -> None:
    if not args.philter:
        return

    common.print_header("Running philter...")
    philter = deid.Philter()
    for note in notes:
        note.text = philter.scrub_text(note.text)


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


def define_chart_review_parser(parser: argparse.ArgumentParser) -> None:
    parser.usage = "%(prog)s [OPTION]... INPUT LS_URL PHI"

    parser.add_argument("dir_input", metavar="/path/to/input")
    parser.add_argument("label_studio_url", metavar="https://example.com/labelstudio")
    parser.add_argument("dir_phi", metavar="/path/to/phi")

    parser.add_argument(
        "--export-to", metavar="PATH", help="Where to put exported documents (default is to delete after use)"
    )
    parser.add_argument(
        "--no-philter", action="store_false", dest="philter", default=True, help="Don’t run philter on notes"
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


async def chart_review_main(args: argparse.Namespace) -> None:
    """
    Prepare for chart review by uploading some documents to Label Studio.

    There are three major steps:
    1. Gather requested DocumentReference resources, reverse-engineering the original docref IDs if necessary
    2. Run NLP
    3. Run Philter
    4. Upload to Label Studio
    """
    init_checks(args)

    common.set_user_fs_options(vars(args))  # record filesystem options like --s3-region before creating Roots
    root_input = store.Root(args.dir_input)
    root_phi = store.Root(args.dir_phi, create=True)

    # Auth & read files early for quick error feedback
    client = fhir_client.create_fhir_client_for_cli(args, root_input, ["DocumentReference"])
    access_token = common.read_text(args.ls_token).strip()
    labels = ctakesclient.filesystem.map_cui_pref(args.symptoms_bsv)

    async with client:
        ndjson_folder = await gather_docrefs(client, root_input, root_phi, args)
        notes = await read_notes_from_ndjson(client, ndjson_folder.name)

    await run_nlp(notes, args)
    philter_notes(notes, args)  # safe to do after NLP because philter does not change character counts
    push_to_label_studio(notes, access_token, labels, args)


async def run_chart_review(parser: argparse.ArgumentParser, argv: list[str]) -> None:
    """Parses a chart review CLI"""
    define_chart_review_parser(parser)
    args = parser.parse_args(argv)
    await chart_review_main(args)
