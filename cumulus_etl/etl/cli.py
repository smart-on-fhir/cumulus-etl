"""Load, transform, and write out input data to deidentified FHIR"""

import argparse
import datetime
import logging
import os
import shutil
import sys

import rich
import rich.table

import cumulus_etl
from cumulus_etl import cli_utils, common, deid, errors, fhir, loaders, store
from cumulus_etl.etl import context, tasks
from cumulus_etl.etl.config import JobConfig, JobSummary
from cumulus_etl.etl.tasks import task_factory

TaskList = list[type[tasks.EtlTask]]


###############################################################################
#
# Main Pipeline (run all tasks)
#
###############################################################################


async def etl_job(
    config: JobConfig, selected_tasks: TaskList, scrubber: deid.Scrubber
) -> list[JobSummary]:
    """
    :param config: job config
    :param selected_tasks: the tasks to run
    :param scrubber: de-id scrubber to use for jobs
    :return: a list of job summaries
    """
    summary_list = []

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


async def check_requirements(selected_tasks: TaskList) -> None:
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
        "--version", action="version", version=f"cumulus-etl {cumulus_etl.__version__}"
    )
    parser.add_argument(
        "--input-format",
        default="ndjson",
        choices=["i2b2", "ndjson"],
        help="input format (default is ndjson)",
    )
    cli_utils.add_output_format(parser)
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
    parser.add_argument(
        "--allow-missing-resources",
        action="store_true",
        help="run tasks even if their resources are not present",
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
    group.add_argument(
        "--export-group",
        metavar="NAME",
        help="name of the FHIR Group that was exported (default is to grab this from an "
        "export log file in the input folder, but you can also use this to assign a "
        "nickname as long as you consistently set the same nickname)",
    )
    group.add_argument(
        "--export-timestamp",
        metavar="TIMESTAMP",
        help="when the data was exported from the FHIR Group (default is to grab this from an "
        "export log file in the input folder)",
    )

    cli_utils.add_nlp(parser)
    cli_utils.add_task_selection(parser)
    cli_utils.add_debugging(parser)


def print_config(
    args: argparse.Namespace, job_datetime: datetime.datetime, all_tasks: TaskList
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
    args: argparse.Namespace, loader_results: loaders.LoaderResults
) -> (str, datetime.datetime):
    """Returns (group_name, datetime)"""
    # Grab completion options from CLI or loader
    export_group_name = args.export_group or loader_results.group_name
    export_datetime = (
        datetime.datetime.fromisoformat(args.export_timestamp)
        if args.export_timestamp
        else loader_results.export_datetime
    )

    # Error out if we have missing args
    missing_group_name = export_group_name is None
    missing_datetime = not export_datetime
    if missing_group_name and missing_datetime:
        errors.fatal(
            "Missing Group name and timestamp export information for the input data.",
            errors.COMPLETION_ARG_MISSING,
            extra="This is likely because you don’t have an export log in your input folder.\n"
            "This log file (log.ndjson) is generated by some bulk export tools.\n"
            "Instead, please manually specify the Group name and timestamp of the export "
            "with the --export-group and --export-timestamp options.\n"
            "These options are necessary to track whether all the required data from "
            "a Group has been imported and is ready to be used.\n"
            "See https://docs.smarthealthit.org/cumulus/etl/bulk-exports.html for more "
            "information.\n",
        )
    # These next two errors can be briefer because the user clearly knows about the args.
    elif missing_datetime:
        errors.fatal("Missing --export-datetime argument.", errors.COMPLETION_ARG_MISSING)
    elif missing_group_name:
        errors.fatal("Missing --export-group argument.", errors.COMPLETION_ARG_MISSING)

    return export_group_name, export_datetime


async def check_available_resources(
    loader: loaders.Loader,
    *,
    requested_resources: set[str],
    args: argparse.Namespace,
    is_default_tasks: bool,
) -> set[str]:
    # Here we try to reconcile which resources the user requested and which resources are actually
    # available in the input root.
    # - If the user didn't specify a specific task, we'll scope down the requested resources to
    #   what is actually present in the input.
    # - If they did, we'll complain if their required resources are not available.
    #
    # Reconciling is helpful for performance reasons (don't need to finalize untouched tables),
    # UX reasons (can tell user if they made a CLI mistake), and completion tracking (don't
    # mark a resource as complete if we didn't even export it)
    if args.allow_missing_resources:
        return requested_resources

    detected = await loader.detect_resources()
    if detected is None:
        return requested_resources  # likely we haven't run bulk export yet

    if missing_resources := requested_resources - detected:
        for resource in sorted(missing_resources):
            # Log the same message we would print if in common.py if we ran tasks anyway
            logging.warning("No %s files found in %s", resource, loader.root.path)

        if is_default_tasks:
            requested_resources -= missing_resources  # scope down to detected resources
            if not requested_resources:
                errors.fatal(
                    "No supported resources found.",
                    errors.MISSING_REQUESTED_RESOURCES,
                )
        else:
            msg = "Required resources not found.\n"
            msg += "Add --allow-missing-resources to run related tasks anyway with no input."
            errors.fatal(msg, errors.MISSING_REQUESTED_RESOURCES)

    return requested_resources


async def etl_main(args: argparse.Namespace) -> None:
    # Set up some common variables

    # record filesystem options like --s3-region before creating Roots
    store.set_user_fs_options(vars(args))

    root_input = store.Root(args.dir_input)
    root_phi = store.Root(args.dir_phi, create=True)

    job_context = context.JobContext(root_phi.joinpath("context.json"))
    job_datetime = common.datetime_now()  # grab timestamp before we do anything

    selected_tasks = task_factory.get_selected_tasks(args.task, args.task_filter)
    is_default_tasks = not args.task and not args.task_filter

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

    inline_resources = cli_utils.expand_inline_resources(args.inline_resource)
    inline_mimetypes = cli_utils.expand_inline_mimetypes(args.inline_mimetype)

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
                resume=args.resume,
                inline=args.inline,
                inline_resources=inline_resources,
                inline_mimetypes=inline_mimetypes,
            )

        required_resources = await check_available_resources(
            config_loader,
            args=args,
            is_default_tasks=is_default_tasks,
            requested_resources=required_resources,
        )
        # Drop any tasks that we didn't find resources for
        selected_tasks = [t for t in selected_tasks if t.resource in required_resources]

        # Pull down resources from any remote location (like s3), convert from i2b2, or do a bulk export
        loader_results = await config_loader.load_resources(required_resources)

        # Establish the group name and datetime of the loaded dataset (from CLI args or Loader)
        export_group_name, export_datetime = handle_completion_args(args, loader_results)

        # If *any* of our tasks need bulk MS de-identification, run it
        if any(t.needs_bulk_deid for t in selected_tasks):
            loader_results.directory = await deid.Scrubber.scrub_bulk_data(loader_results.path)
        else:
            print("Skipping bulk de-identification.")
            print("These selected tasks will de-identify resources as they are processed.")

        # Prepare config for jobs
        config = JobConfig(
            args.dir_input,
            loader_results.path,
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
            export_url=loader_results.export_url,
            deleted_ids=loader_results.deleted_ids,
        )
        common.write_json(config.path_config(), config.as_json(), indent=4)

        # Finally, actually run the meat of the pipeline! (Filtered down to requested tasks)
        scrubber = deid.Scrubber(config.dir_phi, use_philter=args.philter)
        summaries = await etl_job(config, selected_tasks, scrubber)

    # Update job context for future runs
    job_context.last_successful_datetime = job_datetime
    job_context.last_successful_input_dir = args.dir_input
    job_context.last_successful_output_dir = args.dir_output
    job_context.save()

    # Report out any stripped extensions or dropped resources due to modiferExtensions
    scrubber.print_extension_report()

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
