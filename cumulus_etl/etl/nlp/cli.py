"""
Similar to a normal ETL task, but with an extra NLP focus.

Some differences:
- Runs only the NLP targeted tasks
- No completion tracking
- No bulk de-identification (i.e. no MS tool)
- Has NLP specific arguments
"""

import argparse

import cumulus_fhir_support as cfs
import rich
import rich.prompt

from cumulus_etl import cli_utils, common, deid, loaders, nlp, store
from cumulus_etl.etl import pipeline


def define_nlp_parser(parser: argparse.ArgumentParser) -> None:
    """Fills out an argument parser with all the ETL options."""
    parser.usage = "%(prog)s [OPTION]... INPUT OUTPUT PHI"

    pipeline.add_common_etl_args(parser)
    cli_utils.add_ctakes_override(parser)
    cli_utils.add_task_selection(parser, etl_mode=False)
    cli_utils.add_aws(parser, athena=True)
    nlp.add_note_selection(parser)


async def check_input_size(
    codebook: deid.Codebook, folder: str, res_filter: deid.FilterFunc
) -> int:
    root = store.Root(folder)
    count = 0
    for resource in common.read_resource_ndjson(root, {"DiagnosticReport", "DocumentReference"}):
        if not res_filter or await res_filter(codebook, resource):
            count += 1
    return count


async def nlp_main(args: argparse.Namespace) -> None:
    async def prep_scrubber(
        client: cfs.FhirClient, results: loaders.LoaderResults
    ) -> tuple[deid.Scrubber, dict]:
        res_filter = nlp.get_note_filter(client, args)
        scrubber = deid.Scrubber(args.dir_phi)

        # Let the user know how many documents got selected, so there are no big cost surprises.
        # But skip it if we aren't in a TTY or the user provided an override flag.
        should_confirm = rich.get_console().is_interactive and not args.allow_large_selection
        count = await check_input_size(scrubber.codebook, results.path, res_filter)
        if should_confirm:
            if not rich.prompt.Confirm.ask(f"Run NLP on {count:,} notes?", default=False):
                raise SystemExit
        else:
            rich.print(f"Running NLP on {count:,} notes…")

        config_args = {"ctakes_overrides": args.ctakes_overrides, "resource_filter": res_filter}
        return scrubber, config_args

    await pipeline.run_pipeline(args, prep_scrubber=prep_scrubber, nlp=True)


async def run_nlp(parser: argparse.ArgumentParser, argv: list[str]) -> None:
    """Parses an etl CLI"""
    define_nlp_parser(parser)
    args = parser.parse_args(argv)
    await nlp_main(args)
