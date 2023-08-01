"""Load, transform, and write out input data to deidentified FHIR"""

import argparse
import datetime
import itertools
import os
import shutil
import sys
import tempfile
from collections.abc import Iterable

import rich
import rich.table

from cumulus_etl import cli_utils, common, deid, errors, fhir, loaders, nlp, store
from cumulus_etl.etl import context, tasks
from cumulus_etl.etl.config import JobConfig, JobSummary


###############################################################################
#
# Main Pipeline (run all tasks)
#
###############################################################################


async def load_and_deidentify(loader: loaders.Loader, resources: Iterable[str]) -> tempfile.TemporaryDirectory:
    """
    Loads the input directory and does a first-pass de-identification

    Code outside this method should never see the original input files.

    :returns: a temporary directory holding the de-identified files in FHIR ndjson format
    """
    # First step is loading all the data into a local ndjson format
    loaded_dir = await loader.load_all(list(resources))

    # Second step is de-identifying that data (at a bulk level)
    return await deid.Scrubber.scrub_bulk_data(loaded_dir.name)


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


def check_requirements() -> None:
    """
    Verifies that all external services and programs are ready

    May block while waiting a bit for them.
    """
    nlp.check_ctakes()
    nlp.check_cnlpt()
    check_mstool()


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
        "--input-format", default="ndjson", choices=["i2b2", "ndjson"], help="input format (default is ndjson)"
    )
    parser.add_argument(
        "--output-format",
        default="deltalake",
        choices=["deltalake", "ndjson", "parquet"],
        help="output format (default is deltalake)",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        metavar="SIZE",
        default=200000,
        help="how many entries to process at once and thus " "how many to put in one output file (default is 200k)",
    )
    parser.add_argument("--comment", help="add the comment to the log file")
    parser.add_argument("--philter", action="store_true", help="run philter on all freeform text fields")
    parser.add_argument("--errors-to", metavar="DIR", help="where to put resources that could not be processed")

    cli_utils.add_aws(parser)
    cli_utils.add_auth(parser)

    export = parser.add_argument_group("bulk export")
    export.add_argument(
        "--export-to", metavar="DIR", help="Where to put exported files (default is to delete after use)"
    )
    export.add_argument("--since", help="Start date for export from the FHIR server")
    export.add_argument("--until", help="End date for export from the FHIR server")

    cli_utils.add_nlp(parser)

    task = parser.add_argument_group("task selection")
    task.add_argument("--task", action="append", help="Only update the given output tables (comma separated)")
    task.add_argument(
        "--task-filter",
        action="append",
        choices=["covid_symptom", "cpu", "gpu"],
        help="Restrict tasks to only the given sets (comma separated)",
    )

    cli_utils.add_debugging(parser)


def print_config(args: argparse.Namespace, job_datetime: datetime.datetime, all_tasks: Iterable[tasks.EtlTask]) -> None:
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
    table.add_row("Batch size:", str(args.batch_size))
    table.add_row("Tasks:", ", ".join(sorted(t.name for t in all_tasks)))
    if args.comment:
        table.add_row("Comment:", args.comment)
    rich.get_console().print(table)


async def etl_main(args: argparse.Namespace) -> None:
    # Set up some common variables
    store.set_user_fs_options(vars(args))  # record filesystem options like --s3-region before creating Roots

    root_input = store.Root(args.dir_input)
    root_phi = store.Root(args.dir_phi, create=True)

    job_context = context.JobContext(root_phi.joinpath("context.json"))
    job_datetime = common.datetime_now()  # grab timestamp before we do anything

    # Check which tasks are being run, allowing comma-separated values
    task_names = args.task and set(itertools.chain.from_iterable(t.split(",") for t in args.task))
    task_filters = args.task_filter and list(itertools.chain.from_iterable(t.split(",") for t in args.task_filter))
    selected_tasks = tasks.get_selected_tasks(task_names, task_filters)

    # Print configuration
    print_config(args, job_datetime, selected_tasks)
    common.print_header()  # all "prep" comes in this next section, like connecting to server, bulk export, and de-id

    if args.errors_to:
        cli_utils.confirm_dir_is_empty(args.errors_to)

    # Check that cTAKES is running and any other services or binaries we require
    if not args.skip_init_checks:
        check_requirements()

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
                root_input, client=client, export_to=args.export_to, since=args.since, until=args.until
            )

        # Pull down resources and run the MS tool on them
        deid_dir = await load_and_deidentify(config_loader, required_resources)

        # Prepare config for jobs
        config = JobConfig(
            args.dir_input,
            deid_dir.name,
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
