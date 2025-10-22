"""
Initializes basic resource tables.

Creates the tables if they don't exist and pushes up a basic schema.
"""

import argparse
from collections.abc import Generator

import pyarrow

from cumulus_etl import cli_utils, completion, formats, store
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


def get_task_tables() -> Generator[tuple[dict, pyarrow.Schema]]:
    """Returns (formatter kwargs, table_schema)"""
    for task_class in task_factory.get_default_tasks():
        for output in task_class.outputs:
            kwargs = {
                "dbname": output.get_name(task_class),
                "group_field": output.group_field,
                "uniqueness_fields": output.uniqueness_fields,
                "update_existing": output.update_existing,
            }
            schema = task_class.get_schema(output.get_resource_type(task_class), [])
            yield kwargs, schema

    # Add the general ETL completion table, just to be nice to any possible consumers
    yield completion.completion_format_args(), completion.completion_schema()


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
        for format_kwargs, schema in get_task_tables():
            formatter = format_class(output_root, **format_kwargs)
            formatter.write_records(formats.Batch([], schema=schema))
            progress.update(task, advance=1)


async def run_init(parser: argparse.ArgumentParser, argv: list[str]) -> None:
    """Parse arguments and do the work"""
    define_init_parser(parser)
    args = parser.parse_args(argv)
    await init_main(args)
