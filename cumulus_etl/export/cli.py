"""Do a standalone bulk export from an EHR"""

import argparse
import sys

from cumulus_etl import cli_utils, common, errors, fhir, loaders, store
from cumulus_etl.etl.tasks import task_factory
from cumulus_etl.loaders.fhir.bulk_export import BulkExporter


def define_export_parser(parser: argparse.ArgumentParser) -> None:
    parser.usage = "cumulus-etl export [OPTION]... FHIR_URL DIR"

    parser.add_argument("url_input", metavar="https://fhir.example.com/Group/ABC")
    parser.add_argument("export_to", metavar="/path/to/output")
    group = cli_utils.add_bulk_export(parser, as_subgroup=False)
    group.add_argument("--type", action="append", help="resource types to export (_type)")
    group.add_argument(
        "--type-filter", action="append", help="search filters to apply to the export (_typeFilter)"
    )
    group.add_argument(
        "--cancel", action="store_true", help="cancel an interrupted export, use with --resume"
    )

    cli_utils.add_auth(parser, use_fhir_url=False)
    cli_utils.add_task_selection(parser)


async def export_main(args: argparse.Namespace) -> None:
    """Exports data from an EHR to a folder."""
    # record filesystem options before creating Roots
    store.set_user_fs_options(vars(args))

    selected_tasks = task_factory.get_selected_tasks(args.task, args.task_filter)
    required_resources = {t.resource for t in selected_tasks}
    using_default_tasks = not args.task and not args.task_filter

    # Fold in manually specified --type args (very similar to --task, but more familiar to folks
    # used to the bulk export spec)
    if args.type:
        types = set(cli_utils.expand_comma_list_arg(args.type))
        if using_default_tasks:
            required_resources = types
            using_default_tasks = False
        else:
            required_resources |= types

    inline_resources = cli_utils.expand_inline_resources(args.inline_resource)
    inline_mimetypes = cli_utils.expand_inline_mimetypes(args.inline_mimetype)

    fhir_root = store.Root(args.url_input)
    client = fhir.create_fhir_client_for_cli(args, fhir_root, required_resources)

    common.print_header()

    async with client:
        if args.cancel:
            if not args.resume:
                errors.fatal(
                    "You provided --cancel without a --resume URL, but you must provide both.",
                    errors.ARGS_CONFLICT,
                )
            exporter = BulkExporter(client, set(), fhir_root.path, None, resume=args.resume)
            if not await exporter.cancel():
                sys.exit(1)
            print("Export cancelled.")
            return

        loader = loaders.FhirNdjsonLoader(
            fhir_root,
            client=client,
            export_to=args.export_to,
            since=args.since,
            until=args.until,
            type_filter=list(cli_utils.expand_comma_list_arg(args.type_filter)),
            resume=args.resume,
            inline=args.inline,
            inline_resources=inline_resources,
            inline_mimetypes=inline_mimetypes,
        )
        await loader.load_from_bulk_export(
            sorted(required_resources), prefer_url_resources=using_default_tasks
        )


async def run_export(parser: argparse.ArgumentParser, argv: list[str]) -> None:
    """Parses an export CLI"""
    define_export_parser(parser)
    args = parser.parse_args(argv)
    await export_main(args)
