"""Helper methods for CLI parsing."""

import argparse
import os
import socket
import tempfile
import time
import urllib.parse

import rich.progress

from cumulus_etl import common, errors


def add_auth(parser: argparse.ArgumentParser) -> None:
    group = parser.add_argument_group("authentication")
    group.add_argument("--smart-client-id", metavar="ID", help="Client ID for SMART authentication")
    group.add_argument("--smart-jwks", metavar="PATH", help="JWKS file for SMART authentication")
    group.add_argument("--basic-user", metavar="USER", help="Username for Basic authentication")
    group.add_argument("--basic-passwd", metavar="PATH", help="Password file for Basic authentication")
    group.add_argument("--bearer-token", metavar="PATH", help="Token file for Bearer authentication")
    group.add_argument("--fhir-url", metavar="URL", help="FHIR server base URL, only needed if you exported separately")


def add_aws(parser: argparse.ArgumentParser) -> None:
    group = parser.add_argument_group("AWS")
    group.add_argument("--s3-region", metavar="REGION", help="If using S3 paths (s3://...), this is their region")
    group.add_argument(
        "--s3-kms-key", metavar="KEY", help="If using S3 paths (s3://...), this is the KMS key ID to use"
    )


def add_nlp(parser: argparse.ArgumentParser):
    group = parser.add_argument_group("NLP")
    group.add_argument(
        "--ctakes-overrides",
        metavar="DIR",
        default="/ctakes-overrides",
        help="Path to cTAKES overrides dir (default is /ctakes-overrides)",
    )
    return group


def add_debugging(parser: argparse.ArgumentParser):
    group = parser.add_argument_group("debugging")
    group.add_argument("--skip-init-checks", action="store_true", help=argparse.SUPPRESS)
    return group


def make_export_dir(export_to: str = None) -> common.Directory:
    """Makes a temporary directory to drop exported ndjson files into"""
    # Handle the easy case -- just a random temp dir
    if not export_to:
        return tempfile.TemporaryDirectory()  # pylint: disable=consider-using-with

    # OK the user has a specific spot in mind. Let's do some quality checks. It must be local and empty.

    if urllib.parse.urlparse(export_to).netloc:
        # We require a local folder because that's all that the MS deid tool can operate on.
        # If we were to relax this requirement, we'd want to copy the exported files over to a local dir.
        errors.fatal(f"The target export folder '{export_to}' must be local. ", errors.BULK_EXPORT_FOLDER_NOT_LOCAL)

    confirm_dir_is_empty(export_to)

    return common.RealDirectory(export_to)


def confirm_dir_is_empty(path: str) -> None:
    """Errors out if the dir exists with contents, but creates empty dir if not present yet"""
    try:
        if os.listdir(path):
            errors.fatal(
                f"The target folder '{path}' already has contents. Please provide an empty folder.",
                errors.FOLDER_NOT_EMPTY,
            )
    except FileNotFoundError:
        # Target folder doesn't exist, so let's make it
        os.makedirs(path, mode=0o700)


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
