"""The command line interface to cumulus-etl"""

import argparse
import asyncio
import sys
from typing import List

from cumulus import etl


def add_auth(parser: argparse.ArgumentParser) -> None:
    group = parser.add_argument_group("authentication")
    group.add_argument("--smart-client-id", metavar="ID", help="Client ID for SMART authentication")
    group.add_argument("--smart-jwks", metavar="PATH", help="JWKS file for SMART authentication")
    group.add_argument("--basic-user", metavar="USER", help="Username for Basic authentication")
    group.add_argument("--basic-passwd", metavar="PATH", help="Password file for Basic authentication")
    group.add_argument("--bearer-token", metavar="PATH", help="Token file for Bearer authentication")
    group.add_argument("--fhir-url", metavar="URL", help="FHIR server base URL, only needed if you exported separately")


def add_aws(parser: argparse.ArgumentParser) -> None:
    group = parser.add_argument_group("AWS")
    group.add_argument("--s3-region", metavar="REGION", help="If using S3 paths (s3://...), this is their region")
    group.add_argument(
        "--s3-kms-key", metavar="KEY", help="If using S3 paths (s3://...), this is the KMS key ID to use"
    )


def make_parser() -> argparse.ArgumentParser:
    """Creates an ArgumentParser for Cumulus ETL"""
    parser = argparse.ArgumentParser(prog="cumulus-etl", usage="%(prog)s [OPTION]... INPUT OUTPUT PHI")
    etl.define_etl_parser(parser, add_auth=add_auth, add_aws=add_aws)
    return parser


async def main(argv: List[str]) -> None:
    parser = make_parser()
    args = parser.parse_args(argv)
    await etl.etl_main(args)


def main_cli():
    asyncio.run(main(sys.argv[1:]))


if __name__ == "__main__":
    main_cli()
