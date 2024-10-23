"""
Initializes basic resource tables.

Creates the tables if they don't exist and pushes up a basic schema.
"""

import argparse
from collections.abc import Iterable

from cumulus_etl import cli_utils, formats, store
from cumulus_etl.etl import tasks
from cumulus_etl.etl.tasks import task_factory


def define_init_parser(parser: argparse.ArgumentParser) -> None:
    parser.usage = "%(prog)s [OPTION]... OUTPUT"
    parser.description = (
        "Initialize all basic output tables. "
        "After this command is run, you will be ready to set up Cumulus Library. "
        "This command is safe to run multiple times on the same folder, "
        "or even on an existing folder with data already in it."
    )

    parser.add_argument("dir_output", metavar="/path/to/output")
    cli_utils.add_output_format(parser)

    cli_utils.add_aws(parser)


def get_task_tables() -> Iterable[tuple[type[tasks.EtlTask], tasks.OutputTable]]:
    for task_class in task_factory.get_default_tasks():
        for output in task_class.outputs:
            if not output.get_name(task_class).startswith("etl__"):
                yield task_class, output


async def init_main(args: argparse.Namespace) -> None:
    """Main logic for initialization"""
    # record filesystem options like --s3-region before creating Roots
    store.set_user_fs_options(vars(args))

    output_root = store.Root(args.dir_output)

    with cli_utils.make_progress_bar() as progress:
        # Set up progress bar
        total_steps = len(list(get_task_tables())) + 1  # extra 1 is initializing the formatter
        task = progress.add_task("Initializing tables", total=total_steps)

        # Initialize formatter (which can take a moment with deltalake)
        format_class = formats.get_format_class(args.output_format)
        format_class.initialize_class(output_root)
        progress.update(task, advance=1)

        # Create an empty JobConfig/ folder, so that the 'convert' command will recognize this
        # folder as an ETL folder.
        output_root.makedirs(output_root.joinpath("JobConfig"))

        # Now iterate through, pushing to each output table
        for task_class, output in get_task_tables():
            batch = task_class.make_batch_from_rows(output.get_resource_type(task_class), [])
            formatter = format_class(output_root, output.get_name(task_class))
            formatter.write_records(batch)
            progress.update(task, advance=1)


async def run_init(parser: argparse.ArgumentParser, argv: list[str]) -> None:
    """Parse arguments and do the work"""
    define_init_parser(parser)
    args = parser.parse_args(argv)
    await init_main(args)
