"""Load, transform, and write out input data to deidentified FHIR"""

import argparse
import asyncio
import itertools
import json
import logging
import os
import shutil
import socket
import sys
import tempfile
import time
from typing import Iterable, List, Type
from urllib.parse import urlparse

import ctakesclient

from cumulus import common, context, deid, errors, fhir_client, loaders, store, tasks
from cumulus.config import JobConfig, JobSummary


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


async def etl_job(config: JobConfig, selected_tasks: List[Type[tasks.EtlTask]]) -> List[JobSummary]:
    """
    :param config: job config
    :param selected_tasks: the tasks to run
    :return: a list of job summaries
    """
    summary_list = []

    scrubber = deid.Scrubber(config.dir_phi)
    for task_class in selected_tasks:
        task = task_class(config, scrubber)
        summary = await task.run()
        summary_list.append(summary)

        path = os.path.join(config.dir_job_config(), f"{summary.label}.json")
        common.write_json(path, summary.as_json(), indent=4)

    return summary_list


###############################################################################
#
# External requirements (like cTAKES)
#
###############################################################################


def is_url_available(url: str) -> bool:
    """Returns whether we are able to make connections to the given URL, with a few retries."""
    url_parsed = urlparse(url)

    num_tries = 6  # try six times / wait five seconds
    for i in range(num_tries):
        try:
            socket.socket().connect((url_parsed.hostname, url_parsed.port))
            return True
        except ConnectionRefusedError:
            if i < num_tries - 1:
                time.sleep(1)

    return False


def check_ctakes() -> None:
    """
    Verifies that cTAKES is available to receive requests.

    Will block while waiting for cTAKES.
    """
    # Check if our cTAKES server is ready (it may still not be fully ready once the socket is open, but at least it
    # will accept requests and then block the reply on it finishing its initialization)
    ctakes_url = ctakesclient.client.get_url_ctakes_rest()
    if not is_url_available(ctakes_url):
        print(
            f"A running cTAKES server was not found at:\n    {ctakes_url}\n\n"
            "Please set the URL_CTAKES_REST environment variable or start the docker support services.",
            file=sys.stderr,
        )
        raise SystemExit(errors.CTAKES_MISSING)


def check_cnlpt() -> None:
    """
    Verifies that the cNLP transformer server is running.
    """
    cnlpt_url = ctakesclient.transformer.get_url_cnlp_negation()

    if not is_url_available(cnlpt_url):
        print(
            f"A running cNLP transformers server was not found at:\n    {cnlpt_url}\n\n"
            "Please set the URL_CNLP_NEGATION environment variable or start the docker support services.",
            file=sys.stderr,
        )
        raise SystemExit(errors.CNLPT_MISSING)


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
    check_ctakes()
    check_cnlpt()
    check_mstool()


###############################################################################
#
# Main
#
###############################################################################


def make_parser() -> argparse.ArgumentParser:
    """Creates an ArgumentParser for Cumulus ETL"""
    parser = argparse.ArgumentParser()
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

    aws = parser.add_argument_group("AWS")
    aws.add_argument("--s3-region", metavar="REGION", help="if using S3 paths (s3://...), this is their region")
    aws.add_argument("--s3-kms-key", metavar="KEY", help="if using S3 paths (s3://...), this is the KMS key ID to use")

    auth = parser.add_argument_group("authentication")
    auth.add_argument("--smart-client-id", metavar="ID", help="Client ID for SMART authentication")
    auth.add_argument("--smart-jwks", metavar="PATH", help="JWKS file for SMART authentication")
    auth.add_argument("--basic-user", metavar="USER", help="Username for Basic authentication")
    auth.add_argument("--basic-passwd", metavar="PATH", help="Password file for Basic authentication")
    auth.add_argument("--bearer-token", metavar="PATH", help="Token file for Bearer authentication")
    auth.add_argument("--fhir-url", metavar="URL", help="FHIR server base URL, only needed if you exported separately")

    export = parser.add_argument_group("bulk export")
    export.add_argument(
        "--export-to", metavar="PATH", help="Where to put exported files (default is to delete after use)"
    )
    export.add_argument("--since", help="Start date for export from the FHIR server")
    export.add_argument("--until", help="End date for export from the FHIR server")

    task = parser.add_argument_group("task selection")
    task.add_argument("--task", action="append", help="Only update the given output tables (comma separated)")
    task.add_argument(
        "--task-filter",
        action="append",
        choices=["covid_symptom", "cpu", "gpu"],
        help="Restrict tasks to only the given sets (comma separated)",
    )

    debug = parser.add_argument_group("debugging")
    debug.add_argument("--skip-init-checks", action="store_true", help=argparse.SUPPRESS)

    return parser


def create_fhir_client(args, root_input, resources):
    client_base_url = args.fhir_url
    if root_input.protocol in {"http", "https"}:
        if args.fhir_url and not root_input.path.startswith(args.fhir_url):
            print(
                "You provided both an input FHIR server and a different --fhir-url. Try dropping --fhir-url.",
                file=sys.stderr,
            )
            raise SystemExit(errors.ARGS_CONFLICT)
        client_base_url = root_input.path

    try:
        try:
            # Try to load client ID from file first (some servers use crazy long ones, like SMART's bulk-data-server)
            smart_client_id = common.read_text(args.smart_client_id).strip() if args.smart_client_id else None
        except FileNotFoundError:
            smart_client_id = args.smart_client_id

        smart_jwks = common.read_json(args.smart_jwks) if args.smart_jwks else None
        basic_password = common.read_text(args.basic_passwd).strip() if args.basic_passwd else None
        bearer_token = common.read_text(args.bearer_token).strip() if args.bearer_token else None
    except OSError as exc:
        print(exc, file=sys.stderr)
        raise SystemExit(errors.ARGS_INVALID) from exc

    return fhir_client.FhirClient(
        client_base_url,
        resources,
        basic_user=args.basic_user,
        basic_password=basic_password,
        bearer_token=bearer_token,
        smart_client_id=smart_client_id,
        smart_jwks=smart_jwks,
    )


async def main(args: List[str]):
    parser = make_parser()
    args = parser.parse_args(args)

    logging.info("Input Directory: %s", args.dir_input)
    logging.info("Output Directory: %s", args.dir_output)
    logging.info("PHI Build Directory: %s", args.dir_phi)

    # Check that cTAKES is running and any other services or binaries we require
    if not args.skip_init_checks:
        check_requirements()

    common.set_user_fs_options(vars(args))  # record filesystem options like --s3-region before creating Roots

    root_input = store.Root(args.dir_input)
    root_phi = store.Root(args.dir_phi, create=True)

    job_context = context.JobContext(root_phi.joinpath("context.json"))
    job_datetime = common.datetime_now()  # grab timestamp before we do anything

    # Check which tasks are being run, allowing comma-separated values
    task_names = args.task and set(itertools.chain.from_iterable(t.split(",") for t in args.task))
    task_filters = args.task_filter and list(itertools.chain.from_iterable(t.split(",") for t in args.task_filter))
    selected_tasks = tasks.EtlTask.get_selected_tasks(task_names, task_filters)

    # Grab a list of all required resource types for the tasks we are running
    required_resources = set(t.resource for t in selected_tasks)

    # Create a client to talk to a FHIR server.
    # This is useful even if we aren't doing a bulk export, because some resources like DocumentReference can still
    # reference external resources on the server (like the document text).
    # If we don't need this client (e.g. we're using local data and don't download any attachments), this is a no-op.
    client = create_fhir_client(args, root_input, required_resources)

    async with client:
        if args.input_format == "i2b2":
            config_loader = loaders.I2b2Loader(root_input, args.batch_size)
        else:
            config_loader = loaders.FhirNdjsonLoader(
                root_input, client, export_to=args.export_to, since=args.since, until=args.until
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
            tasks=[t.name for t in selected_tasks],
        )
        common.write_json(config.path_config(), config.as_json(), indent=4)
        common.print_header("Configuration:")
        print(json.dumps(config.as_json(), indent=4))

        # Finally, actually run the meat of the pipeline! (Filtered down to requested tasks)
        summaries = await etl_job(config, selected_tasks)

    # Print results to the console
    common.print_header("Results:")
    for summary in summaries:
        print(json.dumps(summary.as_json(), indent=4))

    # Update job context for future runs
    job_context.last_successful_datetime = job_datetime
    job_context.last_successful_input_dir = args.dir_input
    job_context.last_successful_output_dir = args.dir_output
    job_context.save()

    # If any task had a failure, flag that for the user
    failed = any(s.success < s.attempt for s in summaries)
    if failed:
        print("** One or more tasks above did not 100% complete! **", file=sys.stderr)
        raise SystemExit(errors.TASK_FAILED)


def main_cli():
    asyncio.run(main(sys.argv[1:]))


if __name__ == "__main__":
    main_cli()
