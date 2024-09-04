"""
Convert one output format to another.

Usually used for ndjson -> deltalake conversions, after the ndjson has been manually reviewed.
"""

import argparse
import os
import tempfile
from collections.abc import Callable
from functools import partial

import pyarrow
import rich.progress

from cumulus_etl import cli_utils, common, completion, errors, formats, store
from cumulus_etl.etl import tasks
from cumulus_etl.etl.tasks import task_factory


def make_batch(
    root: store.Root,
    path: str,
    schema_func: Callable[[list[dict]], pyarrow.Schema],
) -> formats.Batch:
    metadata_path = path.removesuffix(".ndjson") + ".meta"
    try:
        metadata = common.read_json(metadata_path)
    except FileNotFoundError:
        metadata = {}

    rows = list(common.read_ndjson(root, path))
    groups = set(metadata.get("groups", []))
    schema = schema_func(rows)

    return formats.Batch(rows, groups=groups, schema=schema)


def convert_table_metadata(
    meta_path: str,
    formatter: formats.Format,
) -> None:
    try:
        meta = common.read_json(meta_path)
    except (FileNotFoundError, PermissionError):
        return

    # Only one metadata field currently: deleted IDs
    deleted = meta.get("deleted", [])
    formatter.delete_records(set(deleted))


def convert_folder(
    input_root: store.Root,
    *,
    table_name: str,
    schema_func: Callable[[list[dict]], pyarrow.Schema],
    formatter: formats.Format,
    progress: rich.progress.Progress,
) -> None:
    table_input_dir = input_root.joinpath(table_name)
    if not input_root.exists(table_input_dir):
        # Don't error out in this case -- it's not the user's fault if the folder doesn't exist.
        # We're just checking all task folders.
        return

    # Grab all the files in the task dir
    all_paths = store.Root(table_input_dir).ls()
    ndjson_paths = sorted(filter(lambda x: x.endswith(".ndjson"), all_paths))
    if not ndjson_paths:
        # Again, don't error out in this case -- if the ETL made an empty dir, it's not a user-visible error
        return

    # Let's convert! Start chewing through the files
    count = len(ndjson_paths) + 1  # add one for finalize step
    progress_task = progress.add_task(table_name, total=count)

    for ndjson_path in ndjson_paths:
        batch = make_batch(input_root, ndjson_path, schema_func)
        formatter.write_records(batch)
        progress.update(progress_task, advance=1)

    convert_table_metadata(f"{table_input_dir}/{table_name}.meta", formatter)
    formatter.finalize()
    progress.update(progress_task, advance=1)


def convert_task_table(
    task: type[tasks.EtlTask],
    table: tasks.OutputTable,
    input_root: store.Root,
    output_root: store.Root,
    formatter_class: type[formats.Format],
    progress: rich.progress.Progress,
) -> None:
    """Converts a task's output folder (like output/observation/ or output/covid_symptom__nlp_results/)"""

    # Start with a formatter
    formatter = formatter_class(
        output_root,
        table.get_name(task),
        group_field=table.group_field,
        uniqueness_fields=table.uniqueness_fields,
        update_existing=table.update_existing,
    )

    # And then run the conversion
    convert_folder(
        input_root,
        table_name=table.get_name(task),
        schema_func=partial(task.get_schema, table.get_resource_type(task)),
        formatter=formatter,
        progress=progress,
    )


def convert_completion(
    input_root: store.Root,
    output_root: store.Root,
    formatter_class: type[formats.Format],
    progress: rich.progress.Progress,
) -> None:
    """Converts the etl__completion metadata table"""
    convert_folder(
        input_root,
        table_name=completion.COMPLETION_TABLE,
        schema_func=lambda rows: completion.completion_schema(),
        formatter=formatter_class(output_root, **completion.completion_format_args()),
        progress=progress,
    )


def copy_job_configs(input_root: store.Root, output_root: store.Root) -> None:
    with tempfile.TemporaryDirectory() as tmpdir:
        job_config_path = input_root.joinpath("JobConfig/")

        # Download input dir if it's not local
        if input_root.protocol != "file":
            new_location = os.path.join(tmpdir, "JobConfig/")
            input_root.get(job_config_path, new_location, recursive=True)
            job_config_path = new_location

        output_root.put(job_config_path, output_root.joinpath("JobConfig/"), recursive=True)


def walk_tree(
    input_root: store.Root, output_root: store.Root, formatter_class: type[formats.Format]
) -> None:
    all_tasks = task_factory.get_all_tasks()

    with cli_utils.make_progress_bar() as progress:
        for task in all_tasks:
            for table in task.outputs:
                convert_task_table(task, table, input_root, output_root, formatter_class, progress)

        # And aftward, copy over the completion metadata tables
        convert_completion(input_root, output_root, formatter_class, progress)

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
    # record filesystem options like --s3-region before creating Roots
    store.set_user_fs_options(vars(args))

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
