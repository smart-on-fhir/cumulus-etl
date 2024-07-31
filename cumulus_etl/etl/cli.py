"""Load, transform, and write out input data to deidentified FHIR"""

import argparse
import datetime
import os
import shutil
import sys
from collections.abc import Iterable

import rich
import rich.table

from cumulus_etl import cli_utils, common, deid, errors, fhir, loaders, store
from cumulus_etl.etl import context, tasks
from cumulus_etl.etl.config import JobConfig, JobSummary
from cumulus_etl.etl.tasks import task_factory

###############################################################################
#
# Main Pipeline (run all tasks)
#
###############################################################################


async def etl_job(
    config: JobConfig, selected_tasks: list[type[tasks.EtlTask]], use_philter: bool = False
) -> list[JobSummary]:
    """
    :param config: job config
    :param selected_tasks: the tasks to run
    :param use_philter: whether to run text through philter
    :return: a list of job summaries
    """
    summary_list = []

    scrubber = deid.Scrubber(config.dir_phi, use_philter=use_philter)
    for task_class in selected_tasks:
        task = task_class(config, scrubber)
        task_summaries = await task.run()
        summary_list.extend(task_summaries)

        for summary in task_summaries:
            path = os.path.join(config.dir_job_config(), f"{summary.label}.json")
            common.write_json(path, summary.as_json(), indent=4)

    return summary_list


###############################################################################
#
# External requirements (like cTAKES)
#
###############################################################################


def check_mstool() -> None:
    """
    Verifies that the MS anonymizer tool is installed in PATH.
    """
    if not shutil.which(deid.MSTOOL_CMD):
        print(
            f"No executable found for {deid.MSTOOL_CMD}.\n\n"
            "Please see https://github.com/microsoft/Tools-for-Health-Data-Anonymization\n"
            "and install it into your PATH.",
            file=sys.stderr,
        )
        raise SystemExit(errors.MSTOOL_MISSING)


async def check_requirements(selected_tasks: Iterable[type[tasks.EtlTask]]) -> None:
    """
    Verifies that all external services and programs are ready

    May block while waiting a bit for them.
    """
    check_mstool()
    for task in selected_tasks:
        await task.init_check()


###############################################################################
#
# Main
#
###############################################################################


def define_etl_parser(parser: argparse.ArgumentParser) -> None:
    """Fills out an argument parser with all the ETL options."""
    parser.usage = "%(prog)s [OPTION]... INPUT OUTPUT PHI"

    parser.add_argument("dir_input", metavar="/path/to/input")
    parser.add_argument("dir_output", metavar="/path/to/output")
    parser.add_argument("dir_phi", metavar="/path/to/phi")
    parser.add_argument(
        "--input-format",
        default="ndjson",
        choices=["i2b2", "ndjson"],
        help="input format (default is ndjson)",
    )
    parser.add_argument(
        "--output-format",
        default="deltalake",
        choices=["deltalake", "ndjson"],
        help="output format (default is deltalake)",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        metavar="SIZE",
        default=200000,
        help="how many entries to process at once and thus "
        "how many to put in one output file (default is 200k)",
    )
    parser.add_argument("--comment", help="add the comment to the log file")
    parser.add_argument(
        "--philter", action="store_true", help="run philter on all freeform text fields"
    )
    parser.add_argument(
        "--errors-to", metavar="DIR", help="where to put resources that could not be processed"
    )

    cli_utils.add_aws(parser)
    cli_utils.add_auth(parser)

    export = cli_utils.add_bulk_export(parser)
    export.add_argument(
        "--export-to",
        metavar="DIR",
        help="where to put exported files (default is to delete after use)",
    )

    group = parser.add_argument_group("external export identification")
    group.add_argument("--export-group", help=argparse.SUPPRESS)
    group.add_argument("--export-timestamp", help=argparse.SUPPRESS)
    # Temporary explicit opt-in flag during the development of the completion-tracking feature
    group.add_argument(
        "--write-completion", action="store_true", default=False, help=argparse.SUPPRESS
    )

    cli_utils.add_nlp(parser)
    cli_utils.add_task_selection(parser)
    cli_utils.add_debugging(parser)


def print_config(
    args: argparse.Namespace, job_datetime: datetime.datetime, all_tasks: Iterable[tasks.EtlTask]
) -> None:
    """
    Prints the ETL configuration to the console.

    This is often redundant with command line arguments, but it's helpful if you are looking at a docker container's
    logs after the fact.

    This is different from printing a JobConfig's json to the console, because on a broad level, this here is to inform
    the user at the console, while JobConfig's purpose is to inform task code how to operate.

    But more specifically:
    (A) we want to print this as early as possible and a JobConfig needs some computed config (notably, it needs
        to be constructed after the bulk export target dir is ready)
    (B) we may want to print some options here that don't particularly need to go into the JobConfig/**.json file in
        the output folder (e.g. export dir)
    (C) this formatting can be very user-friendly, while the other should be more machine-processable (e.g. skipping
        some default-value prints, or formatting time)
    """
    common.print_header("Configuration:")
    table = rich.table.Table("", rich.table.Column(overflow="fold"), box=None, show_header=False)
    table.add_row("Input path:", args.dir_input)
    if args.input_format != "ndjson":
        table.add_row(" Format:", args.input_format)
    if args.since:
        table.add_row(" Since:", args.since)
    if args.until:
        table.add_row(" Until:", args.until)
    table.add_row("Output path:", args.dir_output)
    table.add_row(" Format:", args.output_format)
    table.add_row("PHI/Build path:", args.dir_phi)
    if args.export_to:
        table.add_row("Export path:", args.export_to)
    if args.errors_to:
        table.add_row("Errors path:", args.errors_to)
    table.add_row("Current time:", f"{common.timestamp_datetime(job_datetime)} UTC")
    table.add_row("Batch size:", f"{args.batch_size:,}")
    table.add_row("Tasks:", ", ".join(sorted(t.name for t in all_tasks)))
    if args.comment:
        table.add_row("Comment:", args.comment)
    rich.get_console().print(table)


def handle_completion_args(
    args: argparse.Namespace, loader: loaders.Loader
) -> (str, datetime.datetime):
    """Returns (group_name, datetime)"""
    # Grab completion options from CLI or loader
    export_group_name = args.export_group or loader.group_name
    export_datetime = (
        datetime.datetime.fromisoformat(args.export_timestamp)
        if args.export_timestamp
        else loader.export_datetime
    )

    # Disable entirely if asked to
    if not args.write_completion:
        export_group_name = None
        export_datetime = None

    # Error out if we have mismatched args
    has_group_name = export_group_name is not None
    has_datetime = bool(export_datetime)
    if has_group_name and not has_datetime:
        errors.fatal("Missing --export-datetime argument.", errors.COMPLETION_ARG_MISSING)
    elif not has_group_name and has_datetime:
        errors.fatal("Missing --export-group argument.", errors.COMPLETION_ARG_MISSING)

    return export_group_name, export_datetime


async def etl_main(args: argparse.Namespace) -> None:
    # Set up some common variables

    # record filesystem options like --s3-region before creating Roots
    store.set_user_fs_options(vars(args))

    root_input = store.Root(args.dir_input)
    root_phi = store.Root(args.dir_phi, create=True)

    job_context = context.JobContext(root_phi.joinpath("context.json"))
    job_datetime = common.datetime_now()  # grab timestamp before we do anything

    selected_tasks = task_factory.get_selected_tasks(args.task, args.task_filter)

    # Print configuration
    print_config(args, job_datetime, selected_tasks)
    common.print_header()  # all "prep" comes in this next section, like connecting to server, bulk export, and de-id

    if args.errors_to:
        cli_utils.confirm_dir_is_empty(store.Root(args.errors_to, create=True))

    # Check that cTAKES is running and any other services or binaries we require
    if not args.skip_init_checks:
        await check_requirements(selected_tasks)

    # Grab a list of all required resource types for the tasks we are running
    required_resources = set(t.resource for t in selected_tasks)

    # Create a client to talk to a FHIR server.
    # This is useful even if we aren't doing a bulk export, because some resources like DocumentReference can still
    # reference external resources on the server (like the document text).
    # If we don't need this client (e.g. we're using local data and don't download any attachments), this is a no-op.
    client = fhir.create_fhir_client_for_cli(args, root_input, required_resources)

    async with client:
        if args.input_format == "i2b2":
            config_loader = loaders.I2b2Loader(root_input, export_to=args.export_to)
        else:
            config_loader = loaders.FhirNdjsonLoader(
                root_input,
                client=client,
                export_to=args.export_to,
                since=args.since,
                until=args.until,
            )

        # Pull down resources from any remote location (like s3), convert from i2b2, or do a bulk export
        loaded_dir = await config_loader.load_all(list(required_resources))

        # Establish the group name and datetime of the loaded dataset (from CLI args or Loader)
        export_group_name, export_datetime = handle_completion_args(args, config_loader)

        # If *any* of our tasks need bulk MS de-identification, run it
        if any(t.needs_bulk_deid for t in selected_tasks):
            loaded_dir = await deid.Scrubber.scrub_bulk_data(loaded_dir.name)
        else:
            print("Skipping bulk de-identification.")
            print("These selected tasks will de-identify resources as they are processed.")

        # Prepare config for jobs
        config = JobConfig(
            args.dir_input,
            loaded_dir.name,
            args.dir_output,
            args.dir_phi,
            args.input_format,
            args.output_format,
            client,
            comment=args.comment,
            batch_size=args.batch_size,
            timestamp=job_datetime,
            ctakes_overrides=args.ctakes_overrides,
            dir_errors=args.errors_to,
            tasks=[t.name for t in selected_tasks],
            export_group_name=export_group_name,
            export_datetime=export_datetime,
        )
        common.write_json(config.path_config(), config.as_json(), indent=4)

        # Finally, actually run the meat of the pipeline! (Filtered down to requested tasks)
        summaries = await etl_job(config, selected_tasks, use_philter=args.philter)

    # Update job context for future runs
    job_context.last_successful_datetime = job_datetime
    job_context.last_successful_input_dir = args.dir_input
    job_context.last_successful_output_dir = args.dir_output
    job_context.save()

    # Flag final status to user
    common.print_header()
    if any(s.had_errors for s in summaries):
        print("🚨 One or more tasks above did not 100% complete! 🚨", file=sys.stderr)
        raise SystemExit(errors.TASK_FAILED)
    else:
        print("⭐ All tasks completed successfully! ⭐", file=sys.stderr)


async def run_etl(parser: argparse.ArgumentParser, argv: list[str]) -> None:
    """Parses an etl CLI"""
    define_etl_parser(parser)
    args = parser.parse_args(argv)
    await etl_main(args)
