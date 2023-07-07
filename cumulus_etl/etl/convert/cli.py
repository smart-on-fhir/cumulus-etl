"""
Convert one output format to another.

Usually used for ndjson -> deltalake conversions, after the ndjson has been manually reviewed.
"""

import argparse
import os
import tempfile

import pandas
import rich.progress

from cumulus_etl import cli_utils, common, errors, formats, store
from cumulus_etl.etl import tasks


def make_progress_bar() -> rich.progress.Progress:
    # The default columns don't change to elapsed time when finished.
    columns = [
        rich.progress.TextColumn("[progress.description]{task.description}"),
        rich.progress.BarColumn(),
        rich.progress.TaskProgressColumn(),
        rich.progress.TimeRemainingColumn(elapsed_when_finished=True),
    ]
    return rich.progress.Progress(*columns)


def convert_task_table(
    task: type[tasks.EtlTask],
    table: tasks.OutputTable,
    input_root: store.Root,
    output_root: store.Root,
    formatter_class: type[formats.Format],
    progress: rich.progress.Progress,
) -> None:
    """Converts a task's output folder (like output/observation/ or output/covid_symptom__nlp_results/)"""
    # Does the task dir even exist?
    task_input_dir = input_root.joinpath(table.get_name(task))
    if not input_root.exists(task_input_dir):
        # Don't error out in this case -- it's not the user's fault if the folder doesn't exist.
        # We're just checking all task folders.
        return

    # Grab all the files in the task dir
    all_paths = store.Root(task_input_dir).ls()
    ndjson_paths = list(filter(lambda x: x.endswith(".ndjson"), all_paths))
    if not ndjson_paths:
        # Again, don't error out in this case -- if the ETL made an empty dir, it's not a user-visible error
        return

    # Let's convert! Make the formatter and chew through the files
    formatter = formatter_class(
        output_root,
        table.get_name(task),
        group_field=table.group_field,
        resource_type=table.get_schema(task),
    )

    count = len(ndjson_paths) + 1  # add one for finalize step
    progress_task = progress.add_task(table.get_name(task), total=count)

    for index, ndjson_path in enumerate(ndjson_paths):
        rows = common.read_ndjson(ndjson_path)
        df = pandas.DataFrame(rows)
        df.drop_duplicates("id", inplace=True)
        formatter.write_records(df, index)
        progress.update(progress_task, advance=1)

    formatter.finalize()
    progress.update(progress_task, advance=1)


def copy_job_configs(input_root: store.Root, output_root: store.Root) -> None:
    with tempfile.TemporaryDirectory() as tmpdir:
        job_config_path = input_root.joinpath("JobConfig")

        # Download input dir if it's not local
        if input_root.protocol != "file":
            input_root.get(job_config_path, tmpdir, recursive=True)
            job_config_path = os.path.join(tmpdir, "JobConfig")

        output_root.put(job_config_path, output_root.path, recursive=True)


def walk_tree(input_root: store.Root, output_root: store.Root, formatter_class: type[formats.Format]) -> None:
    all_tasks = tasks.get_all_tasks()

    with make_progress_bar() as progress:
        for task in all_tasks:
            for table in task.outputs:
                convert_task_table(task, table, input_root, output_root, formatter_class, progress)

        # Copy JobConfig files over too.
        # To consider: Marking the job_config.json file in these JobConfig directories as "converted" in some way.
        #              They already will be detectable by having "output_format: ndjson", but maybe we could do more.
        #              But there is also an elegance to not touching them at all. :shrug:
        copy_job_configs(input_root, output_root)


def validate_input_dir(input_root: store.Root) -> None:
    if not input_root.exists(input_root.path):
        errors.fatal(f"Original folder '{input_root.path}' does not exist!", errors.ARGS_INVALID)
    if not input_root.exists(input_root.joinpath("JobConfig")):
        errors.fatal(
            f"Original folder '{input_root.path}' does not look like a Cumulus ETL output folder. "
            f"It is missing the JobConfig folder.",
            errors.ARGS_INVALID,
        )


#####################################################################################################################
#
# Main
#
#####################################################################################################################


def define_convert_parser(parser: argparse.ArgumentParser) -> None:
    parser.usage = "%(prog)s [OPTION]... ORIGINAL TARGET"
    parser.description = (
        "Convert an ndjson ETL output folder into a new or existing Delta Lake output folder. "
        "This is useful if you have manually reviewed the ndjson result of the ETL first, "
        "and now want to push the output to S3."
    )

    # A future addition could be adding format arguments (to allow converting multiple ways).
    # But at the time of writing, they were unnecessary, so we'll start with just ndjson -> deltalake.

    parser.add_argument("input_dir", metavar="/path/to/original")
    parser.add_argument("output_dir", metavar="/path/to/target")

    cli_utils.add_aws(parser)


async def convert_main(args: argparse.Namespace) -> None:
    """Main logic for converting"""
    store.set_user_fs_options(vars(args))  # record filesystem options like --s3-region before creating Roots

    input_root = store.Root(args.input_dir)
    validate_input_dir(input_root)

    output_root = store.Root(args.output_dir)
    formatter_class = formats.get_format_class("deltalake")
    formatter_class.initialize_class(output_root)

    walk_tree(input_root, output_root, formatter_class)


async def run_convert(parser: argparse.ArgumentParser, argv: list[str]) -> None:
    """Parse arguments and do the work"""
    define_convert_parser(parser)
    args = parser.parse_args(argv)
    await convert_main(args)
