"""
Similar to a normal ETL task, but with an extra NLP focus.
"""

import argparse

from cumulus_etl import cli_utils, deid, loaders
from cumulus_etl.etl import pipeline


def define_nlp_parser(parser: argparse.ArgumentParser) -> None:
    """Fills out an argument parser with all the ETL options."""
    parser.usage = "%(prog)s [OPTION]... INPUT OUTPUT PHI"

    pipeline.add_common_etl_args(parser)
    cli_utils.add_nlp(parser)


async def nlp_main(args: argparse.Namespace) -> None:
    async def prep_scrubber(_results: loaders.LoaderResults) -> tuple[deid.Scrubber, dict]:
        return deid.Scrubber(args.dir_phi), {"ctakes_overrides": args.ctakes_overrides}

    await pipeline.run_pipeline(args, prep_scrubber=prep_scrubber, nlp=True)


async def run_nlp(parser: argparse.ArgumentParser, argv: list[str]) -> None:
    """Parses an etl CLI"""
    define_nlp_parser(parser)
    args = parser.parse_args(argv)
    await nlp_main(args)
