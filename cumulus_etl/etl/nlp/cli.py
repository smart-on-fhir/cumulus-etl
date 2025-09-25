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

    # Smaller default batch size than normal ETL because we might keep notes in memory during batch
    pipeline.add_common_etl_args(parser, batch_size=50_000)
    cli_utils.add_ctakes_override(parser)
    cli_utils.add_aws(parser, athena=True)
    nlp.add_note_selection(parser)

    parser.add_argument(
        "--task",
        action="append",
        help="run this NLP task (comma separated, use '--task help' to see full list)",
        required=True,
    )
    parser.add_argument(
        "--provider",
        choices=["azure", "bedrock", "local"],
        default="local",
        help="which model provider to use (default is local)",
    )
    parser.add_argument(
        "--azure-deployment",
        help="what Azure deployment name to use (default is model name)",
    )
    parser.add_argument(
        "--batch",
        action="store_true",
        help="use batching mode (saves money, might take longer, not all models support it)",
    )


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
    nlp.set_nlp_config(args)

    async def prep_scrubber(
        client: cfs.FhirClient, results: loaders.LoaderResults
    ) -> tuple[deid.Scrubber, dict]:
        res_filter = nlp.get_note_filter(client, args)
        scrubber = deid.Scrubber(args.dir_phi)

        # Let the user know how many documents got selected, so there are no big cost surprises.
        count = await check_input_size(scrubber.codebook, results.path, res_filter)
        response = cli_utils.prompt(
            f"Run NLP on {count:,} notes?", override=args.allow_large_selection
        )
        if response in cli_utils.PromptResponse.SKIPPED:
            rich.print(f"Running NLP on {count:,} notes…")

        config_args = {"ctakes_overrides": args.ctakes_overrides, "resource_filter": res_filter}
        return scrubber, config_args

    await pipeline.run_pipeline(args, prep_scrubber=prep_scrubber, nlp=True)


async def run_nlp(parser: argparse.ArgumentParser, argv: list[str]) -> None:
    """Parses an etl CLI"""
    define_nlp_parser(parser)
    args = parser.parse_args(argv)
    await nlp_main(args)
