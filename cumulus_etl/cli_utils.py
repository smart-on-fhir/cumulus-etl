"""Helper methods for CLI parsing."""

import argparse
import contextlib
import enum
import itertools
import os
import re
import socket
import tempfile
import time
import tracemalloc
import urllib.parse
from collections.abc import Iterable

import rich

from cumulus_etl import common, errors, store


def add_auth(parser: argparse.ArgumentParser, *, use_fhir_url: bool = True):
    RemovedEhrArg.add(parser, "--smart-client-id")
    RemovedEhrArg.add(parser, "--smart-key")
    RemovedEhrArg.add(parser, "--basic-user")
    RemovedEhrArg.add(parser, "--basic-passwd")
    RemovedEhrArg.add(parser, "--bearer-token")
    if use_fhir_url:
        RemovedEhrArg.add(parser, "--fhir-url")

    # --smart-jwks was a deprecated alias for --smart-key (as of Jan 2025)
    RemovedEhrArg.add(parser, "--smart-jwks")


def add_aws(parser: argparse.ArgumentParser, athena: bool = False) -> None:
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
    if athena:
        group.add_argument(
            "--athena-region",
            metavar="REGION",
            help="the region of your Athena workgroup (default is us-east-1)",
        )
        group.add_argument(
            "--athena-workgroup",
            metavar="GROUP",
            help="the name of your Athena workgroup",
        )
        group.add_argument(
            "--athena-database",
            metavar="DB",
            help="the name of your Athena database",
        )


class RemovedEhrArg(argparse.Action):
    def __call__(self, parser, namespace, values, option_string=None):
        errors.fatal(
            f"The {option_string} flag has been removed.\n"
            "Please use SMART Fetch to extract FHIR data from your EHR instead:\n"
            "  https://docs.smarthealthit.org/cumulus/fetch/",
            errors.FEATURE_REMOVED,
        )

    @classmethod
    def add(cls, parser: argparse.ArgumentParser, flag: str) -> None:
        parser.add_argument(flag, action=cls, help=argparse.SUPPRESS)


def add_ctakes_override(parser: argparse.ArgumentParser):
    parser.add_argument(
        "--ctakes-overrides",
        metavar="DIR",
        default="/ctakes-overrides",
        help="path to cTAKES overrides dir (default is /ctakes-overrides)",
    )


def add_output_format(parser: argparse.ArgumentParser, *, choices: list[str]) -> None:
    parser.add_argument(
        "--output-format",
        default=choices[0],
        choices=choices,
        help=f"output format (default is {choices[0]})",
    )


def add_task_selection(parser: argparse.ArgumentParser):
    parser.add_argument(
        "--task",
        action="append",
        help="only run these tasks (comma separated, default is all supported FHIR resources, "
        "use '--task help' to see full list)",
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
        # We require a local folder because that's all that the MS deid tool we used to use could
        # operate on. If we were to relax this now, we'd need to make sure we're using fsspec in
        # all the right places.
        errors.fatal(
            f"The target export folder '{export_to}' must be local. ",
            errors.BULK_EXPORT_FOLDER_NOT_LOCAL,
        )

    confirm_dir_is_empty(store.Root(export_to, create=True))

    return common.RealDirectory(export_to)


def confirm_dir_is_empty(root: store.Root, message: str | None = None) -> None:
    """Errors out if the dir exists with contents"""
    try:
        if list(root.ls()):
            extra = f"\n\n{message}" if message else ""
            errors.fatal(
                f"The target folder '{root.path}' already has contents. Please provide an empty folder.{extra}",
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


def user_regex_to_pattern(term: str) -> re.Pattern:
    """Takes a user search regex and adds some boundaries to it"""
    # Make a custom version of \b that allows non-word characters to be on edge of the term too.
    # For example:
    #   This misses: re.match(r"\ba\+\b", "a+")
    #   But this hits: re.match(r"\ba\+", "a+")
    # So to work around that, we look for the word boundary ourselves.
    edge = r"(\W|$|^)"
    return re.compile(f"{edge}({term}){edge}", re.IGNORECASE)


def user_term_to_pattern(term: str) -> re.Pattern:
    """Takes a user search term and turns it into a clinical-note-appropriate regex"""
    return user_regex_to_pattern(re.escape(term))


def process_input_dir(folder: str) -> str:
    root = store.Root(folder)
    if not root.is_http and not root.exists(folder):
        if folder == "%EXAMPLE-NLP%":
            return os.path.join(os.path.dirname(__file__), "etl/studies/example/ndjson")
        errors.fatal(f"Input folder '{folder}' does not exist.", errors.FOLDER_DOES_NOT_EXIST)
    return folder


class PromptResponse(enum.Flag):
    # Break these into so many conditions to help avoid spaghetti code of "if" conditions.
    # With this, you can do one simple match tree, hopefully.
    OVERRIDDEN = 1
    NON_INTERACTIVE = 2
    APPROVED = 4
    # Combos:
    SKIPPED = 3  # combo of OVERRIDEN and NON_INTERACTIVE


def prompt(msg: str, override: bool = False) -> PromptResponse:
    if override:
        return PromptResponse.SKIPPED
    elif not rich.get_console().is_interactive:
        return PromptResponse.NON_INTERACTIVE
    elif rich.prompt.Confirm.ask(msg, default=False):
        return PromptResponse.APPROVED
    else:
        raise SystemExit(0)


def plural(single: str, plural: str, count: int) -> str:
    target = single if count == 1 else plural
    return target.replace("%d", f"{count:,}")


#######################################################
# Unused methods for manual debugging below this point.
#######################################################


@contextlib.contextmanager
def time_it(desc: str | None = None):  # pragma: no cover
    """Tiny little timer context manager that is useful when debugging"""
    start = time.perf_counter()
    yield
    end = time.perf_counter()
    suffix = f" ({desc})" if desc else ""
    rich.print(f"TIME IT: {end - start:.2f}s{suffix}")


@contextlib.contextmanager
def mem_it(desc: str | None = None):  # pragma: no cover
    """Tiny little context manager to measure memory usage"""
    start_tracing = not tracemalloc.is_tracing()
    if start_tracing:
        tracemalloc.start()

    before, before_peak = tracemalloc.get_traced_memory()
    yield
    after, after_peak = tracemalloc.get_traced_memory()

    if start_tracing:
        tracemalloc.stop()

    suffix = f" ({desc})" if desc else ""
    if after_peak > before_peak:
        suffix = f"{suffix} ({after_peak - before_peak:,} PEAK change)"
    rich.print(f"MEM IT: {after - before:,}{suffix}")
