"""Helper methods for CLI parsing."""

import argparse
import itertools
import os
import socket
import tempfile
import time
import urllib.parse
from collections.abc import Iterable

import rich.progress

from cumulus_etl import common, errors, store


def add_auth(parser: argparse.ArgumentParser, *, use_fhir_url: bool = True):
    group = parser.add_argument_group("authentication")
    group.add_argument("--smart-client-id", metavar="ID", help="client ID for SMART authentication")
    group.add_argument(
        "--smart-key", metavar="PATH", help="JWKS or PEM file for SMART authentication"
    )
    group.add_argument("--basic-user", metavar="USER", help="username for Basic authentication")
    group.add_argument(
        "--basic-passwd", metavar="PATH", help="password file for Basic authentication"
    )
    group.add_argument(
        "--bearer-token", metavar="PATH", help="token file for Bearer authentication"
    )
    if use_fhir_url:
        group.add_argument(
            "--fhir-url",
            metavar="URL",
            help="FHIR server base URL, only needed if you exported separately",
        )

    # --smart-jwks is a deprecated alias for --smart-key (as of Jan 2025)
    # Keep it around for a bit, since it was in common use for a couple years.
    group.add_argument("--smart-jwks", metavar="PATH", help=argparse.SUPPRESS)


def add_aws(parser: argparse.ArgumentParser) -> None:
    group = parser.add_argument_group("AWS")
    group.add_argument(
        "--s3-region",
        metavar="REGION",
        help="if using S3 paths (s3://...), this is their region (default is us-east-1)",
    )
    group.add_argument(
        "--s3-kms-key",
        metavar="KEY",
        help="if using S3 paths (s3://...), this is the KMS key ID to use",
    )


def add_bulk_export(parser: argparse.ArgumentParser, *, as_subgroup: bool = True):
    if as_subgroup:
        parser = parser.add_argument_group("bulk export")
    parser.add_argument(
        "--since", metavar="TIMESTAMP", help="start date for export from the FHIR server"
    )
    # "Until" is not an official part of the bulk FHIR API, but some custom servers support it
    parser.add_argument(
        "--until", metavar="TIMESTAMP", help="end date for export from the FHIR server"
    )
    parser.add_argument("--resume", metavar="URL", help="polling status URL from a previous export")
    parser.add_argument(
        "--inline",
        action="store_true",
        help="attachments will be inlined after the export",
    )
    parser.add_argument(
        "--inline-resource",
        metavar="RESOURCES",
        action="append",
        help="only consider this resource for inlining (default is all supported inline targets: "
        "DiagnosticReport and DocumentReference)",
    )
    parser.add_argument(
        "--inline-mimetype",
        metavar="MIMETYPES",
        action="append",
        help="only inline this attachment mimetype (default is text, HTML, and XHTML)",
    )
    return parser


def add_nlp(parser: argparse.ArgumentParser):
    group = parser.add_argument_group("NLP")
    group.add_argument(
        "--ctakes-overrides",
        metavar="DIR",
        default="/ctakes-overrides",
        help="path to cTAKES overrides dir (default is /ctakes-overrides)",
    )
    return group


def add_output_format(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--output-format",
        default="deltalake",
        choices=["deltalake", "ndjson"],
        help="output format (default is deltalake)",
    )


def add_task_selection(parser: argparse.ArgumentParser):
    task = parser.add_argument_group("task selection")
    task.add_argument(
        "--task",
        action="append",
        help="only consider these tasks (comma separated, "
        "default is all supported FHIR resources, "
        "use '--task help' to see full list)",
    )
    task.add_argument(
        "--task-filter",
        action="append",
        choices=["covid_symptom", "cpu", "gpu"],
        help="restrict tasks to only the given sets (comma separated)",
    )


def add_debugging(parser: argparse.ArgumentParser):
    group = parser.add_argument_group("debugging")
    group.add_argument("--skip-init-checks", action="store_true", help=argparse.SUPPRESS)
    return group


def make_export_dir(export_to: str | None = None) -> common.Directory:
    """Makes a temporary directory to drop exported ndjson files into"""
    # Handle the easy case -- just a random temp dir
    if not export_to:
        return tempfile.TemporaryDirectory()

    # OK the user has a specific spot in mind. Let's do some quality checks.
    # It must be local and empty.

    if urllib.parse.urlparse(export_to).netloc:
        # We require a local folder because that's all that the MS deid tool can operate on.
        # If we were to relax this requirement, we'd want to copy the exported files over to a
        # local dir.
        errors.fatal(
            f"The target export folder '{export_to}' must be local. ",
            errors.BULK_EXPORT_FOLDER_NOT_LOCAL,
        )

    # Allowing a previous export log file is harmless, since we append to it.
    # This helps the UX for resuming an interrupted bulk export by not requiring a new dir.
    confirm_dir_is_empty(store.Root(export_to, create=True), allow=["log.ndjson"])

    return common.RealDirectory(export_to)


def confirm_dir_is_empty(root: store.Root, allow: Iterable[str] | None = None) -> None:
    """Errors out if the dir exists with contents"""
    try:
        files = {os.path.basename(p) for p in root.ls()}
        if allow is not None:
            files -= set(allow)
        if files:
            errors.fatal(
                f"The target folder '{root.path}' already has contents. Please provide an empty folder.",
                errors.FOLDER_NOT_EMPTY,
            )
    except FileNotFoundError:
        pass


def is_url_available(url: str, retry: bool = True) -> bool:
    """Returns whether we are able to make connections to the given URL, with a few retries."""
    url_parsed = urllib.parse.urlparse(url)

    num_tries = 6 if retry else 1  # if retrying, try six times (i.e. wait fifteen seconds)
    for i in range(num_tries):
        try:
            socket.create_connection((url_parsed.hostname, url_parsed.port))
            return True
        except socket.gaierror:  # hostname didn't resolve
            return False
        except ConnectionRefusedError:  # service is not ready yet
            if i < num_tries - 1:
                time.sleep(3)

    return False


def make_progress_bar() -> rich.progress.Progress:
    # The default columns use time remaining, which has felt inaccurate/less useful than a simple elapsed counter.
    # - The estimation logic seems rough (often jumping time around).
    # - For indeterminate bars, the estimate shows nothing.
    columns = [
        rich.progress.TextColumn("[progress.description]{task.description}"),
        rich.progress.BarColumn(),
        rich.progress.TaskProgressColumn(),
        rich.progress.TimeElapsedColumn(),
    ]
    return rich.progress.Progress(*columns)


def expand_inline_resources(arg: Iterable[str] | None) -> set[str]:
    """
    This converts a list of inline resource args into the final properly cased resource names.

    If you have an arg like --inline-resource, this will process that for you.
    """
    allowed = {"diagnosticreport": "DiagnosticReport", "documentreference": "DocumentReference"}

    if arg is None:
        return set(allowed.values())

    resources = set(expand_comma_list_arg(arg))
    for resource in resources:
        if resource.casefold() not in allowed:
            errors.fatal(f"Unsupported resource for inlining: {resource}", errors.ARGS_INVALID)

    return {allowed[resource.casefold()] for resource in resources}


def expand_inline_mimetypes(arg: Iterable[str] | None) -> set[str]:
    """
    This converts a list of inline mimetype args into a set of normalized mimetypes.

    If you have an arg like --inline-mimetype, this will process that for you.
    """
    if arg is None:
        return {"text/plain", "text/html", "application/xhtml+xml"}

    return set(expand_comma_list_arg(arg, casefold=True))


def expand_comma_list_arg(arg: Iterable[str] | None, casefold: bool = False) -> Iterable[str]:
    """
    This converts a list of string args, splits any strings on commas, and combines results.

    This is useful for CLI arguments with action="append" but you also want to allow comma
    separated args. --task does this, as well as others.

    An example CLI:
      --task=patient --task=condition,procedure
    Would give:
      ["patient", "condition,procedure"]
    And this method would turn that into:
      ["patient", "condition", procedure"]
    """
    if arg is None:
        return []
    split_args = itertools.chain.from_iterable(x.split(",") for x in arg)
    if casefold:
        return map(str.casefold, split_args)
    return split_args
