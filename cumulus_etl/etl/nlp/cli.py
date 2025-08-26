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

from cumulus_etl import cli_utils, deid, loaders, nlp
from cumulus_etl.etl import pipeline


def define_nlp_parser(parser: argparse.ArgumentParser) -> None:
    """Fills out an argument parser with all the ETL options."""
    parser.usage = "%(prog)s [OPTION]... INPUT OUTPUT PHI"

    pipeline.add_common_etl_args(parser)
    cli_utils.add_ctakes_override(parser)
    cli_utils.add_task_selection(parser, etl_mode=False)
    cli_utils.add_aws(parser, athena=True)
    nlp.add_note_selection(parser)


async def nlp_main(args: argparse.Namespace) -> None:
    async def prep_scrubber(
        client: cfs.FhirClient, _results: loaders.LoaderResults
    ) -> tuple[deid.Scrubber, dict]:
        res_filter = nlp.get_note_filter(client, args)
        config_args = {"ctakes_overrides": args.ctakes_overrides, "resource_filter": res_filter}
        return deid.Scrubber(args.dir_phi), config_args

    await pipeline.run_pipeline(args, prep_scrubber=prep_scrubber, nlp=True)


async def run_nlp(parser: argparse.ArgumentParser, argv: list[str]) -> None:
    """Parses an etl CLI"""
    define_nlp_parser(parser)
    args = parser.parse_args(argv)
    await nlp_main(args)
