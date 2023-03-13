"""The command line interface to cumulus-etl"""

import argparse
import asyncio
import sys
from typing import List, Optional

from cumulus import chart_review, etl

CHART_REVIEW = "chart-review"
ETL = "etl"
KNOWN_COMMANDS = {CHART_REVIEW, ETL}


async def run_chart_review(parser: argparse.ArgumentParser, argv: List[str]) -> None:
    """Parses a chart review CLI"""
    chart_review.define_chart_review_parser(parser)
    args = parser.parse_args(argv)
    await chart_review.chart_review_main(args)


async def run_etl(parser: argparse.ArgumentParser, argv: List[str]) -> None:
    """Parses an etl CLI"""
    etl.define_etl_parser(parser)
    args = parser.parse_args(argv)
    await etl.etl_main(args)


def get_subcommand(argv: List[str]) -> Optional[str]:
    """
    Determines which subcommand was requested by the given command line.

    Python's argparse has no good way of setting a default sub-parser.
    (i.e. one that parses the command line if no sub parser subcommand is specified)
    So instead, this method inspects the first positional argument.
    If it's a recognized command, we return it. Else None.
    """
    for i, arg in enumerate(argv):
        if arg in KNOWN_COMMANDS:
            return argv.pop(i)  # remove it to make later parsers' jobs easier
        elif not arg.startswith("-"):
            return None  # first positional arg did not match a known command, assume default command


async def main(argv: List[str]) -> None:
    subcommand = get_subcommand(argv)

    prog = "cumulus-etl"
    if subcommand:
        prog += f" {subcommand}"  # to make --help look nicer
    parser = argparse.ArgumentParser(prog=prog)

    if subcommand == CHART_REVIEW:
        await run_chart_review(parser, argv)
    else:
        await run_etl(parser, argv)


def main_cli():
    asyncio.run(main(sys.argv[1:]))


if __name__ == "__main__":
    main_cli()
