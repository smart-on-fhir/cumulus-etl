"""Inline attachments inside NDJSON by downloading the URLs"""

import argparse

from cumulus_etl import cli_utils, common, errors, fhir, inliner, store


def define_inline_parser(parser: argparse.ArgumentParser) -> None:
    parser.usage = "cumulus-etl inline [OPTION]... DIR FHIR_URL"

    parser.add_argument("src", metavar="/path/to/input")
    parser.add_argument("url_input", metavar="https://fhir.example.com/")

    parser.add_argument(
        "--resource",
        metavar="RESOURCES",
        action="append",
        help="only consider this resource (default is all supported inline targets: "
        "DiagnosticReport and DocumentReference)",
    )
    parser.add_argument(
        "--mimetype",
        metavar="MIMETYPES",
        action="append",
        help="only inline this attachment mimetype (default is text, HTML, and XHTML)",
    )

    cli_utils.add_auth(parser, use_fhir_url=False)
    cli_utils.add_aws(parser)


async def inline_main(args: argparse.Namespace) -> None:
    """Exports data from an EHR to a folder."""
    # record filesystem options before creating Roots
    store.set_user_fs_options(vars(args))

    src_root = store.Root(args.src)
    fhir_root = store.Root(args.url_input)

    # Help the user in case they got the order of the arguments wrong.
    if fhir_root.protocol not in {"http", "https"}:
        errors.fatal(
            f"Provided URL {args.url_input} does not look like a URL. "
            "See --help for argument usage.",
            errors.ARGS_INVALID,
        )

    resources = cli_utils.expand_inline_resources(args.resource)
    mimetypes = cli_utils.expand_inline_mimetypes(args.mimetype)

    common.print_header()

    async with fhir.create_fhir_client_for_cli(args, fhir_root, resources) as client:
        await inliner.inliner(client, src_root, resources, mimetypes)


async def run_inline(parser: argparse.ArgumentParser, argv: list[str]) -> None:
    """Parses an inline CLI"""
    define_inline_parser(parser)
    args = parser.parse_args(argv)
    await inline_main(args)
