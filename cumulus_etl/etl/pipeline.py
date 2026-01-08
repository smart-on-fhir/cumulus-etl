import argparse
import datetime
import logging
import os
import sys
from collections.abc import Awaitable, Callable

import cumulus_fhir_support as cfs
import rich
import rich.table

from cumulus_etl import cli_utils, common, deid, errors, fhir, loaders, store
from cumulus_etl.etl import context, tasks
from cumulus_etl.etl.config import JobConfig, JobSummary, validate_output_folder
from cumulus_etl.etl.tasks import task_factory

TaskList = list[type[tasks.EtlTask]]


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


def add_common_etl_args(parser: argparse.ArgumentParser, batch_size: int = 100_000) -> None:
    parser.add_argument("dir_input", metavar="/path/to/input")
    parser.add_argument("dir_output", metavar="/path/to/output")
    parser.add_argument("dir_phi", metavar="/path/to/phi")

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
        default=batch_size,
        help="how many entries to process at once and thus "
        f"how many to put in one output file (default is {batch_size // 1000}k)",
    )
    parser.add_argument("--comment", help="add the comment to the log file")
    parser.add_argument(
        "--errors-to", metavar="DIR", help="where to put resources that could not be processed"
    )

    cli_utils.add_auth(parser)
    cli_utils.add_debugging(parser)


def print_config(
    args: argparse.Namespace, job_datetime: datetime.datetime, all_tasks: TaskList
) -> None:
    """
    Prints the ETL configuration to the console.

    This is often redundant with command line arguments, but it's helpful if you are looking at a
    docker container's logs after the fact.

    This is different from printing a JobConfig's json to the console, because on a broad level,
    this here is to inform the user at the console, while JobConfig's purpose is to inform task
    code how to operate.

    But more specifically:
    (A) we want to print this as early as possible and a JobConfig needs some computed config
        (notably, it needs to be constructed after the bulk export target dir is ready)
    (B) we may want to print some options here that don't particularly need to go into the
        JobConfig/**.json file in the output folder (e.g. export dir)
    (C) this formatting can be very user-friendly, while the other should be more
        machine-processable (e.g. skipping some default-value prints, or formatting time)
    """
    common.print_header("Configuration:")
    table = rich.table.Table("", rich.table.Column(overflow="fold"), box=None, show_header=False)
    table.add_row("Input path:", args.dir_input)
    if args.input_format != "ndjson":
        table.add_row(" Format:", args.input_format)
    if vars(args).get("since"):
        table.add_row(" Since:", args.since)
    if vars(args).get("until"):
        table.add_row(" Until:", args.until)
    table.add_row("Output path:", args.dir_output)
    table.add_row(" Format:", args.output_format)
    table.add_row("PHI/Build path:", args.dir_phi)
    if vars(args).get("export_to"):
        table.add_row("Export path:", args.export_to)
    if args.errors_to:
        table.add_row("Errors path:", args.errors_to)
    table.add_row("Current time:", f"{common.timestamp_datetime(job_datetime)} UTC")
    table.add_row("Batch size:", f"{args.batch_size:,}")
    table.add_row("Tasks:", ", ".join(sorted(t.name for t in all_tasks)))
    if args.comment:
        table.add_row("Comment:", args.comment)
    rich.get_console().print(table)
    common.print_header()


async def check_available_resources(
    loader: loaders.Loader,
    *,
    requested_resources: set[str],
    args: argparse.Namespace,
    is_default_tasks: bool,
    nlp: bool,
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
    has_allow_missing = hasattr(args, "allow_missing_resources")
    if has_allow_missing and args.allow_missing_resources:
        return requested_resources

    detected = await loader.detect_resources()
    if detected is None:
        return requested_resources  # likely we haven't run bulk export yet

    missing_resources = requested_resources - detected
    available_resources = requested_resources & detected

    if nlp and available_resources:
        # As long as there is any resource for NLP to read from, we'll take it
        return available_resources

    if missing_resources:
        for resource in sorted(missing_resources):
            # Log the same message we would print if in common.py if we ran tasks anyway
            logging.warning("No %s files found in %s", resource, loader.root.path)

        if is_default_tasks:
            if not available_resources:
                errors.fatal("No supported resources found.", errors.MISSING_REQUESTED_RESOURCES)
        else:
            msg = "Required resources not found.\n"
            if has_allow_missing:
                msg += "Add --allow-missing-resources to run related tasks anyway with no input."
            errors.fatal(msg, errors.MISSING_REQUESTED_RESOURCES)

    return available_resources


async def run_pipeline(
    args: argparse.Namespace,
    *,
    nlp: bool = False,
    ndjson_args: dict | None = None,
    i2b2_args: dict | None = None,
    prep_scrubber: Callable[
        [cfs.FhirClient, loaders.LoaderResults], Awaitable[tuple[deid.Scrubber, dict]]
    ],
) -> None:
    # record filesystem options like --s3-region before creating Roots
    store.set_user_fs_options(vars(args))

    args.dir_input = cli_utils.process_input_dir(args.dir_input)

    root_input = store.Root(args.dir_input)
    root_output = store.Root(args.dir_output)
    root_phi = store.Root(args.dir_phi, create=True)

    job_context = context.JobContext(root_phi.joinpath("context.json"))
    job_datetime = common.datetime_now()  # grab timestamp before we do anything

    selected_tasks = task_factory.get_selected_tasks(args.task, nlp=nlp)
    is_default_tasks = not args.task

    # Print configuration
    print_config(args, job_datetime, selected_tasks)

    if args.errors_to:
        cli_utils.confirm_dir_is_empty(store.Root(args.errors_to, create=True))

    # Check that cTAKES is running and any other services or binaries we require
    if not args.skip_init_checks:
        for task in selected_tasks:
            await task.init_check()

    # Combine all task resource sets into one big set of required resources
    required_resources = set().union(*(t.get_resource_types() for t in selected_tasks))

    # Create a client to talk to a FHIR server.
    # This is useful even if we aren't doing a bulk export, because some resources like
    # DocumentReference can still reference external resources on the server (like the document
    # text). If we don't need this client (e.g. we're using local data and don't download any
    # attachments), this is a no-op.
    client = fhir.create_fhir_client_for_cli(args, root_input, required_resources)

    async with client:
        if args.input_format == "i2b2":
            config_loader = loaders.I2b2Loader(root_input, **(i2b2_args or {}))
        else:
            config_loader = loaders.FhirNdjsonLoader(
                root_input, client=client, **(ndjson_args or {})
            )

        required_resources = await check_available_resources(
            config_loader,
            args=args,
            is_default_tasks=is_default_tasks,
            requested_resources=required_resources,
            nlp=nlp,
        )
        # Drop any tasks that we didn't find resources for
        selected_tasks = [t for t in selected_tasks if t.get_resource_types() & required_resources]

        # Load resources from a remote location (like s3), convert from i2b2, or do a bulk export
        loader_results = await config_loader.load_resources(required_resources)

        scrubber, config_args = await prep_scrubber(client, loader_results)
        validate_output_folder(root_output, scrubber.codebook.get_codebook_id())

        # Prepare config for jobs
        config = JobConfig(
            args.dir_input,
            loader_results.path,
            args.dir_output,
            args.dir_phi,
            args.input_format,
            args.output_format,
            client,
            codebook_id=scrubber.codebook.get_codebook_id(),
            comment=args.comment,
            batch_size=args.batch_size,
            timestamp=job_datetime,
            dir_errors=args.errors_to,
            tasks=[t.name for t in selected_tasks],
            export_url=loader_results.export_url,
            deleted_ids=loader_results.deleted_ids,
            **config_args,
        )
        common.write_json(config.path_config(), config.as_json(), indent=4)

        # Finally, actually run the meat of the pipeline! (Filtered down to requested tasks)
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
