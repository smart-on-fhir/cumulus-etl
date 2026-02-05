"""
Similar to a normal ETL task, but with an extra NLP focus.

Some differences:
- Runs only the NLP targeted tasks
- No completion tracking
- Leaves attachment data in place during scrubbing
- Has NLP specific arguments
"""

import argparse
import os

import pyathena
import rich
import rich.prompt

from cumulus_etl import cli_utils, common, deid, errors, nlp, store
from cumulus_etl.etl import pipeline
from cumulus_etl.etl.config import JobConfig


def define_nlp_parser(parser: argparse.ArgumentParser) -> None:
    """Fills out an argument parser with all the ETL options."""
    parser.usage = "%(prog)s [OPTION]... INPUT OUTPUT PHI"

    parser.add_argument("dir_input", metavar="/path/to/input")
    parser.add_argument("dir_output", metavar="/path/to/output")
    parser.add_argument("dir_phi", metavar="/path/to/phi", nargs="?")

    # Smaller default batch size than normal ETL because we might keep notes in memory during batch
    pipeline.add_common_etl_args(
        parser, batch_size=50_000, outputs=["parquet", "ndjson", "deltalake"]
    )
    cli_utils.add_ctakes_override(parser)
    cli_utils.add_aws(parser, athena=True)
    nlp.add_note_selection(parser)

    parser.add_argument(
        "--task",
        action="append",
        help="run this NLP task (comma separated, use '--task help' to see full list)",
        required=True,
    )
    parser.add_argument(
        "--provider",
        choices=["azure", "bedrock", "local"],
        default="local",
        help="which model provider to use (default is local)",
    )
    parser.add_argument(
        "--azure-deployment",
        help="what Azure deployment name to use (default is model name)",
    )
    parser.add_argument(
        "--batch",
        action="store_true",
        help="use batching mode (saves money, might take longer, not all models support it)",
    )
    parser.add_argument(
        "--clean",
        action="store_true",
        help="previous NLP task results will be deleted before uploading the new ones",
    )


async def check_input_size(
    codebook: deid.Codebook, folder: str, res_filter: deid.FilterFunc
) -> int:
    root = store.Root(folder)
    count = 0
    for resource in common.read_resource_ndjson(root, {"DiagnosticReport", "DocumentReference"}):
        if not res_filter or await res_filter(codebook, resource):
            count += 1
    return count


def get_athena_connection(args: argparse.Namespace) -> "pyathena.Connection":
    return pyathena.connect(
        region_name=args.athena_region,
        work_group=args.athena_workgroup,
        schema_name=args.athena_database,
    )


def set_up_output_folder(args: argparse.Namespace) -> tuple[str, bool]:
    """
    Returns the root output folder base path to use, and whether we should register with Athena.

    This is not the full path. We'll later add {study}/{task} folders onto it.
    """
    # There are three modes we want to support:
    # 1. Athena: upload to an S3 connected to your Athena workgroup, register table for you.
    # 2. Non-Athena: write to a folder, likely with NDJSON, to test the flow and/or review
    #    the output before running `cumulus-etl convert` on the folder.
    # 3. Delta Lake: traditional Delta Lake uploading, to ease transition to the above.
    # The three modes need different kinds of information (workgroup vs output folder) so we do a
    # fair bit of checking and error reporting in the two flows below.

    if args.dir_phi:
        if args.athena_workgroup:
            # All three folders were provided, but also an Athena workgroup. Intention is unclear.
            errors.fatal(
                "Both an output folder and an Athena workgroup were provided. An output folder is "
                "only needed in local, non-Athena mode. Otherwise, we upload directly to the "
                "workgroup’s results folder in S3. Please provide an output folder or a workgroup, "
                "but not both.",
                errors.ARGS_CONFLICT,
            )

        if args.output_format != "deltalake":
            # If we aren't doing the old-school delta lake upload, require an empty output folder,
            # to avoid the user thinking they can just point at their normal ETL output folder.
            # This "non-Athena" code path is mostly for just checking that the output is sane, so
            # it's not a burden to require an empty folder.
            cli_utils.confirm_dir_is_empty(
                store.Root(args.dir_output),
                message="If you are pointing at your normal ETL output folder, you will need to "
                "either transition to the --athena-workgroup flavor of NLP uploading (without "
                "specifying an output folder at all) or pass --output-format=deltalake, which "
                "will continue to work for a little bit.",
            )

        if args.output_format == "parquet":
            rich.get_console().print(
                "An output folder was provided, instead of an Athena workgroup. "
                "The resulting results will not be registered as an Athena table. "
                "If you want to use these results with the rest of the Cumulus pipeline, "
                "provide --athena-workgroup and --athena-database flags instead of "
                "an output folder.",
                style="bold red",
            )

        dir_output = args.dir_output
        register_table = False
    else:
        # Swap these, because the PHI folder is normally the third folder.
        args.dir_phi = args.dir_output
        args.dir_output = None

        if args.output_format == "deltalake":
            errors.fatal(
                "You cannot use the deltalake output format with --athena-workgroup.",
                errors.ARGS_CONFLICT,
            )

        try:
            connection = get_athena_connection(args)
        except pyathena.ProgrammingError:
            # raised when workgroup/staging-dir could not be discovered. i.e. no output folder
            errors.fatal(
                "No Athena workgroup provided. Please pass --athena-workgroup with the name of "
                "your Athena workgroup (and also --athena-database).",
                errors.ARGS_INVALID,
            )

        if not connection.schema_name:
            errors.fatal(
                "No Athena database provided. Please pass --athena-database with the name of "
                "your Athena database.",
                errors.ARGS_INVALID,
            )

        workgroup_name = connection.work_group  # could be specified via env vars

        # This bit here is copied from cumulus-library. It's small enough (and hopefully
        # temporary enough - until the NLP flow lives in Library) that I'm just reproducing it
        # here.
        workgroup = connection.client.get_work_group(WorkGroup=workgroup_name)
        wg_conf = workgroup["WorkGroup"]["Configuration"]["ResultConfiguration"]
        s3_path = wg_conf["OutputLocation"]
        encryption_type = wg_conf.get("EncryptionConfiguration", {}).get("EncryptionOption")
        if encryption_type is None:
            bucket = "/".join(s3_path.split("/")[2:3])
            errors.fatal(
                f"The workgroup {workgroup_name} is not reporting an encrypted status. "
                f"Either bucket {bucket} is not encrypted, or the workgroup is overriding the "
                "bucket's default encryption. Please confirm the encryption status of both. "
                "Cumulus requires encryption as part of ensuring safety for limited data sets",
                errors.ATHENA_NOT_ENCRYPTED,
            )

        dir_output = f"{s3_path}cumulus_user_uploads/{args.athena_database}"
        register_table = True

    return dir_output, register_table


def validate_athena_results_folder(workgroup: str, output_dir: str, codebook_id: str) -> None:
    """
    Confirm the user isn't trying to use different PHI folders for the same Athena workgroup.

    If they did that, they would end up with anonymized IDs that don't match.
    This doesn't really validate that they are using the **right** PHI folder, just that they
    haven't accidentally used different PHI folders on subsequent runs. Which is worth protecting
    against. Confirming they are using the **right** PHI folder would require some linking between
    the PHI folder and Athena, which doesn't yet exist.

    It's safe to have multiple output folders all using the same PHI folder. But not the other way
    around.
    """
    id_path = os.path.join(output_dir, "codebook.id")
    try:
        saved_codebook_id = common.read_text(id_path).strip()
    except (FileNotFoundError, PermissionError):
        common.write_text(id_path, codebook_id)
        return

    if saved_codebook_id != codebook_id:
        errors.fatal(
            f"The Athena workgroup '{workgroup}' is already associated with another PHI folder. "
            "You must always use the same PHI folder for a given Athena workgroup.",
            errors.WRONG_PHI_FOLDER,
        )


async def nlp_main(args: argparse.Namespace) -> None:
    dir_output_base, register_table = set_up_output_folder(args)

    # record CLI options for NLP models
    nlp.set_nlp_config(args)

    job_datetime = common.datetime_now()
    scrubber, loader_results, selected_tasks = await pipeline.prepare_pipeline(
        args, job_datetime, nlp=True
    )

    connection = get_athena_connection(args) if register_table else None

    if connection:
        validate_athena_results_folder(
            args.athena_workgroup, dir_output_base, scrubber.codebook.get_codebook_id()
        )

    # Let the user know how many documents got selected, so there are no big cost surprises.
    res_filter = nlp.get_note_filter(args)
    count = await check_input_size(scrubber.codebook, loader_results.path, res_filter)
    response = cli_utils.prompt(f"Run NLP on {count:,} notes?", override=args.allow_large_selection)
    if response in cli_utils.PromptResponse.SKIPPED:
        rich.print(f"Running NLP on {count:,} notes…")

    # Go through each task and do the actual NLP!
    summaries = []
    for task_class in selected_tasks:
        schema = task_class.get_schema(None, [])

        if args.output_format == "deltalake":
            output_format = args.output_format
            format_kwargs = {}
            if args.clean:
                errors.fatal(
                    "Cannot provide --clean with the deltalake output format.", errors.ARGS_CONFLICT
                )
        else:
            # Use special NLP versions of these output formats, which will handle our special
            # Athena logic. Unless we're still using the old deltalake formatter, for backwards
            # compatibility.
            output_format = "nlp-" + args.output_format
            format_kwargs = {
                "connection": connection,
                "schema": schema,
                "version": task_class.task_version,
                "clean": args.clean,
            }

        config = JobConfig(
            args.dir_input,
            loader_results.path,
            dir_output_base,
            args.dir_phi,
            args.input_format,
            output_format,
            format_kwargs=format_kwargs,
            codebook_id=scrubber.codebook.get_codebook_id(),
            batch_size=args.batch_size,
            dir_errors=args.errors_to,
            ctakes_overrides=args.ctakes_overrides,
            resource_filter=res_filter,
        )

        task = task_class(config, scrubber)
        task_summaries = await task.run()
        summaries.extend(task_summaries)

    pipeline.print_summary(summaries)


async def run_nlp(parser: argparse.ArgumentParser, argv: list[str]) -> None:
    """Parses an etl CLI"""
    define_nlp_parser(parser)
    args = parser.parse_args(argv)
    await nlp_main(args)
