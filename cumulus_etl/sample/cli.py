"""Find a random sample of clinical notes"""

import argparse
import json
import random
import sys
from collections.abc import AsyncIterable, Iterable, Iterator

import cumulus_fhir_support as cfs
import rich

from cumulus_etl import cli_utils, common, deid, errors, fhir, nlp, store


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


def add_to_export_dir(resource: dict, export_dir: str | None) -> None:
    if not export_dir:
        return

    path = f"{export_dir}/{resource['resourceType']}.ndjson.gz"
    with common.NdjsonWriter(path, append=True, compressed=True) as writer:
        writer.write(resource)


async def sample(source: AsyncIterable, count: int) -> list:
    """
    Randomly pick 'count' elements from 'source'

    This is the "Algorithm R" from https://en.wikipedia.org/wiki/Reservoir_sampling,
    picked for its simplicity.
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
    return reservoir


async def scan_notes(
    root_input: store.Root,
    *,
    args: argparse.Namespace,
    codebook: deid.Codebook | None = None,
    res_types: set[str] | None = None,
) -> Iterator[tuple[str, int]]:
    """Returns (path, byte offset) for each file that matches our criteria"""
    details = common.read_resource_ndjson_with_details(root_input, res_types, warn_if_empty=True)
    filter_func = nlp.get_note_filter(None, args)
    for path, data in details:
        resource = data["json"]

        # First, make sure it has available text. We only want to sample notes with text.
        try:
            fhir.get_clinical_note_attachment(resource)
        except Exception:  # noqa: S112
            continue

        if not filter_func or await filter_func(codebook, resource):
            yield path, data["byte_offset"]


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


async def prep_and_sample(args: argparse.Namespace) -> Iterator[dict]:
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
    sampled = await sample(locations, args.count)

    # Read the notes back from the saved locations
    return read_notes(root_input, sampled)


async def sample_main(args: argparse.Namespace) -> None:
    # record filesystem options like --s3-region before creating Roots
    store.set_user_fs_options(vars(args))

    # Check CLI args
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
        notes = await prep_and_sample(args)

    # Print the CSV
    print(csv_header(columns), file=output)
    for resource in notes:
        print(csv_row(resource, csv_header(columns)), file=output)
        add_to_export_dir(resource, export_path)
    output.flush()


async def run_sample(parser: argparse.ArgumentParser, argv: list[str]) -> None:
    define_sample_parser(parser)
    args = parser.parse_args(argv)
    await sample_main(args)
