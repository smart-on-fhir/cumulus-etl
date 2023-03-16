"""Helper methods for CLI parsing."""

import argparse
import os
import tempfile
import urllib.parse

from cumulus import errors, loaders


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


def make_export_dir(export_to: str = None) -> loaders.Directory:
    """Makes a temporary directory to drop exported ndjson files into"""
    # Handle the easy case -- just a random temp dir
    if not export_to:
        return tempfile.TemporaryDirectory()  # pylint: disable=consider-using-with

    # OK the user has a specific spot in mind. Let's do some quality checks. It must be local and empty.

    if urllib.parse.urlparse(export_to).netloc:
        # We require a local folder because that's all that the MS deid tool can operate on.
        # If we were to relax this requirement, we'd want to copy the exported files over to a local dir.
        errors.fatal(f"The target export folder '{export_to}' must be local. ", errors.BULK_EXPORT_FOLDER_NOT_LOCAL)

    try:
        if os.listdir(export_to):
            errors.fatal(
                f"The target export folder '{export_to}' already has contents. Please provide an empty folder.",
                errors.BULK_EXPORT_FOLDER_NOT_EMPTY,
            )
    except FileNotFoundError:
        # Target folder doesn't exist, so let's make it
        os.makedirs(export_to, mode=0o700)

    return loaders.RealDirectory(export_to)
