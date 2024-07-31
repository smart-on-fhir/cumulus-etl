"""Do a standalone bulk export from an EHR"""

import argparse

from cumulus_etl import cli_utils, fhir, loaders, store
from cumulus_etl.etl.tasks import task_factory


def define_export_parser(parser: argparse.ArgumentParser) -> None:
    parser.usage = "cumulus-etl export [OPTION]... FHIR_URL DIR"

    parser.add_argument("url_input", metavar="https://fhir.example.com/Group/ABC")
    parser.add_argument("export_to", metavar="/path/to/output")
    cli_utils.add_bulk_export(parser, as_subgroup=False)

    cli_utils.add_auth(parser, use_fhir_url=False)
    cli_utils.add_task_selection(parser)


async def export_main(args: argparse.Namespace) -> None:
    """Exports data from an EHR to a folder."""
    # record filesystem options before creating Roots
    store.set_user_fs_options(vars(args))

    selected_tasks = task_factory.get_selected_tasks(args.task, args.task_filter)
    required_resources = {t.resource for t in selected_tasks}
    using_default_tasks = not args.task and not args.task_filter

    fhir_root = store.Root(args.url_input)
    client = fhir.create_fhir_client_for_cli(args, fhir_root, required_resources)

    async with client:
        loader = loaders.FhirNdjsonLoader(
            fhir_root,
            client=client,
            export_to=args.export_to,
            since=args.since,
            until=args.until,
        )
        await loader.load_from_bulk_export(
            sorted(required_resources), prefer_url_resources=using_default_tasks
        )


async def run_export(parser: argparse.ArgumentParser, argv: list[str]) -> None:
    """Parses an export CLI"""
    define_export_parser(parser)
    args = parser.parse_args(argv)
    await export_main(args)
