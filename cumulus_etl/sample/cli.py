"""Find a random sample of clinical notes"""

import argparse
import contextlib
import json
import random
import sys
from collections.abc import AsyncIterable, Iterable, Iterator

import rich

from cumulus_etl import cli_utils, common, deid, errors, fhir, nlp, store


class MultiResourceWriter:
    def __init__(self, export_path: str | None):
        self.export_path = export_path
        self.stack = contextlib.ExitStack()
        self.writers = {}

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.stack.close()

    def write(self, resource: dict) -> None:
        if not self.export_path:
            return

        res_type = resource["resourceType"]
        writer = self.writers.get(res_type)
        if not writer:
            ndjson_path = f"{self.export_path}/{res_type}.ndjson.gz"
            writer = common.NdjsonWriter(ndjson_path, compressed=True)
            self.writers[res_type] = writer
            self.stack.push(writer)

        writer.write(resource)


def define_sample_parser(parser: argparse.ArgumentParser) -> None:
    parser.usage = "cumulus-etl sample [OPTION]... INPUT"

    parser.add_argument("dir_input", metavar="/path/to/input")

    parser.add_argument(
        "--phi-dir",
        metavar="DIR",
        help="if selecting by anonymous or Athena IDs, the path to your Cumulus PHI folder",
    )

    group = parser.add_argument_group("output")
    group.add_argument(
        "--output",
        metavar="PATH",
        default="-",
        help="where to print the sampled IDs (default is stdout)",
    )
    group.add_argument(
        "--export-to",
        metavar="DIR",
        help="put a copy of all sampled documents in this empty dir",
    )
    group.add_argument(
        "--columns",
        default="note",
        help="which columns to print (options are note, subject, encounter; default is just note)",
    )

    cli_utils.add_aws(parser, athena=True)
    nlp.add_note_selection(parser)

    group = parser.add_argument_group("sampling")
    group.add_argument(
        "--count",
        required=True,
        type=int,
        help="sample at most this many notes",
    )
    group.add_argument(
        "--seed",
        type=int,
        help="random number generator seed, for consistent results",
    )
    group.add_argument(
        "--type",
        default="DiagnosticReport,DocumentReference",
        help="which FHIR types to consider, default is DiagnosticReport and DocumentReference",
    )


def csv_header(columns: list[str]) -> str:
    line = []
    if "note" in columns:
        line.append("note_ref")
    if "subject" in columns:
        line.append("subject_ref")
    if "encounter" in columns:
        line.append("encounter_id")
    return ",".join(line)


def csv_row(resource: dict, columns: list[str]) -> str:
    line = []
    if "note" in columns:
        line.append(f"{resource['resourceType']}/{resource['id']}")
    if "subject" in columns:
        line.append(nlp.get_note_subject_ref(resource) or "")
    if "encounter" in columns:
        line.append(nlp.get_note_encounter_id(resource) or "")
    return ",".join(line)


async def sample(source: AsyncIterable, count: int) -> tuple[list, int]:
    """
    Randomly pick 'count' elements from 'source'

    This is the "Algorithm R" from https://en.wikipedia.org/wiki/Reservoir_sampling,
    picked for its simplicity.

    Returns the list of sampled items, and a total count of examined items.
    """
    reservoir = []
    idx = 0
    async for item in source:
        if len(reservoir) < count:
            reservoir.append(item)
        else:
            j = random.randint(0, idx)  # noqa: S311
            if j < count:
                reservoir[j] = item
        idx += 1
    return reservoir, idx


async def scan_notes(
    root_input: store.Root,
    *,
    args: argparse.Namespace,
    codebook: deid.Codebook | None = None,
    res_types: set[str] | None = None,
) -> Iterator[tuple[str, int]]:
    """Returns (path, byte offset) for each file that matches our criteria"""
    details = common.read_resource_ndjson_with_details(root_input, res_types)
    filter_func = nlp.get_note_filter(None, args)

    total_count = 0
    text_count = 0
    filtered_count = 0
    for path, data in details:
        resource = data["json"]
        total_count += 1

        # First, make sure it has available text. We only want to sample notes with inlined text.
        try:
            attachment = fhir.get_clinical_note_attachment(resource)
            if attachment.get("data") is None:
                continue
        except Exception:  # noqa: S112
            continue

        text_count += 1
        if not filter_func or await filter_func(codebook, resource):
            yield path, data["byte_offset"]
            filtered_count += 1

    if not total_count:
        errors.fatal(
            "No notes found. "
            "Make sure you’re pointing at a folder with DiagnosticReport or DocumentReference "
            "resource files inside it.",
            errors.NOTES_NOT_FOUND,
        )
    if not text_count:
        errors.fatal(
            "No notes with text attachment data found. "
            "Make sure that you’ve first downloaded and inlined any URL-based attachments with "
            "the SMART Fetch tool, like so:\n\n"
            "  smart-fetch hydrate --tasks inline …\n\n"
            "See https://docs.smarthealthit.org/cumulus/fetch/ for more information.",
            errors.NOTES_NOT_FOUND_WITH_TEXT,
        )
    if not filtered_count:
        errors.fatal(
            "No notes were selected by the provided --select-by-* arguments. "
            "Make sure that you have the right folder and the right arguments.",
            errors.NOTES_NOT_FOUND_WITH_FILTER,
        )


def read_notes(
    root: store.Root,
    locations: Iterable[tuple[str, int]],
) -> Iterator[dict]:
    """Returns resources for each provided path/offset location"""
    # First, group up and sort locations, so that we don't need to decompress the same file a bunch
    file_offsets = {}
    for path, offset in sorted(locations):
        file_offsets.setdefault(path, []).append(offset)

    for path, offsets in file_offsets.items():
        with root.fs.open(path, compression="infer") as f:
            for offset in offsets:
                f.seek(offset)
                yield json.loads(f.readline())


async def prep_and_sample(args: argparse.Namespace) -> tuple[Iterator[dict], int]:
    # Normalize the various CLI arguments
    args.dir_input = cli_utils.process_input_dir(args.dir_input)
    root_input = store.Root(args.dir_input)

    res_types = set(args.type.split(",")) & {"DiagnosticReport", "DocumentReference"}
    if not res_types:
        errors.fatal("No valid types selected", errors.ARGS_INVALID)

    codebook = args.phi_dir and deid.Scrubber(args.phi_dir).codebook

    # Prepare note location iterator (storing only locations to save memory)
    locations = scan_notes(root_input, args=args, res_types=res_types, codebook=codebook)

    # Do the actual random sample
    random.seed(args.seed)
    sampled, selected_count = await sample(locations, args.count)

    # Read the notes back from the saved locations
    return read_notes(root_input, sampled), selected_count


async def sample_main(args: argparse.Namespace) -> None:
    # record filesystem options like --s3-region before creating Roots
    store.set_user_fs_options(vars(args))

    # Check CLI args
    if args.count <= 0:
        errors.fatal(f"Count must be a positive number, not '{args.count}'.", errors.ARGS_INVALID)

    if args.output == "-":
        output = sys.stdout
    else:
        output = open(args.output, "w", encoding="utf8")

    if args.export_to:
        export_dir = cli_utils.make_export_dir(export_to=args.export_to)
        export_path = export_dir.name
    else:
        export_path = None

    columns = set(args.columns.split(",")) & {"note", "subject", "encounter"}
    if not columns:
        errors.fatal("No valid columns selected", errors.ARGS_INVALID)

    # Do the sampling
    with rich.get_console().status("Sampling…"):
        notes, selected_count = await prep_and_sample(args)

    # Print the CSV and write out exports
    found_count = 0
    with MultiResourceWriter(export_path) as writer:
        print(csv_header(columns), file=output)
        for resource in notes:
            found_count += 1
            print(csv_row(resource, csv_header(columns)), file=output)
            writer.write(resource)
        output.flush()

    if output == sys.stdout and sys.stdout.isatty():
        # If we are showing the CSV direct to the console, create a little visual space before the
        # following warnings.
        rich.print("", file=sys.stderr)  # pragma: no cover

    if found_count < args.count:
        note_phrase = cli_utils.plural("%d note was", "%d notes were", found_count)
        errors.fatal(
            f"Warning: only {note_phrase} found to sample, rather than {args.count:,}.",
            errors.NOTES_TOO_FEW,
        )

    note_word = cli_utils.plural("note", "notes", found_count)
    rich.print(f"Sampled {found_count:,} (of {selected_count:,}) {note_word}.", file=sys.stderr)


async def run_sample(parser: argparse.ArgumentParser, argv: list[str]) -> None:
    define_sample_parser(parser)
    args = parser.parse_args(argv)
    await sample_main(args)
