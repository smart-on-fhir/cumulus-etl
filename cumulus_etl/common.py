"""Utility methods"""

import contextlib
import csv
import datetime
import gzip
import itertools
import json
import logging
import os
from collections import defaultdict
from collections.abc import Iterator
from functools import partial
from typing import Any, Protocol, TextIO

import cumulus_fhir_support
import rich

from cumulus_etl import store

###############################################################################
#
# Types
#
###############################################################################


# We often want the ability to provide a TemporaryDirectory _or_ an actual real folder that isn't temporary.
# So for that use case, we define a Directory protocol for typing purposes, which looks like a TemporaryDirectory.
# And then a RealDirectory class that does not delete its folder.
class Directory(Protocol):
    name: str


class RealDirectory:
    def __init__(self, path: str):
        self.name = path


###############################################################################
#
# Helper Functions: temporary dir handling
#
###############################################################################

_temp_dir: str | None = None


def set_global_temp_dir(path: str) -> None:
    global _temp_dir
    _temp_dir = path


def get_temp_dir(subdir: str) -> str:
    """
    Use to access a specific subdir of the ETL temporary directory.

    This directory is guaranteed to exist and to be removed when the ETL stops running.

    Use this for sensitive content that you want to share among tasks, like downloaded clinical notes.

    :returns: an absolute path to a temporary folder
    """
    if not _temp_dir:
        raise ValueError("No temporary directory was created yet")

    full_path = os.path.join(_temp_dir, subdir)
    os.makedirs(full_path, mode=0o700, exist_ok=True)

    return full_path


###############################################################################
#
# Helper Functions: listing files
#
###############################################################################


def ls_resources(root: store.Root, resources: set[str], warn_if_empty: bool = False) -> list[str]:
    found_files = cumulus_fhir_support.list_multiline_json_in_dir(
        root.path, resources, fsspec_fs=root.fs
    )

    if warn_if_empty:
        # Invert the {path: type} found_files dictionary into {type: [paths...]}
        type_to_files = defaultdict(list)
        for k, v in found_files.items():
            type_to_files[v].append(k)

        # Now iterate expected types and warn if we didn't see any of them
        for resource in sorted(resources):
            if not type_to_files.get(resource):
                logging.warning("No %s files found in %s", resource, root.path)

    return list(found_files)


###############################################################################
#
# Helper Functions: reading/writing files
#
###############################################################################


@contextlib.contextmanager
def _atomic_open(path: str, mode: str, **kwargs) -> TextIO:
    """A version of open() that handles atomic file access across many filesystems (like S3)"""
    root = store.Root(path)

    with contextlib.ExitStack() as stack:
        if "w" in mode:
            # fsspec is atomic per-transaction.
            # If an error occurs inside the transaction, partial writes will be discarded.
            # But we only want a transaction if we're writing - read transactions may error out
            stack.enter_context(root.fs.transaction)

        yield stack.enter_context(root.fs.open(path, mode=mode, encoding="utf8", **kwargs))


def read_text(path: str) -> str:
    """
    Reads data from the given path, in text format
    :param path: (currently filesystem path)
    :return: message: coded message
    """
    logging.debug("read_text() %s", path)

    with _atomic_open(path, "r") as f:
        return f.read()


def write_text(path: str, text: str) -> None:
    """
    Writes data to the given path, in text format
    :param path: filesystem path
    :param text: the text to write to disk
    """
    logging.debug("write_text() %s", path)

    with _atomic_open(path, "w") as f:
        f.write(text)


def read_json(path: str) -> Any:
    """
    Reads json from a file
    :param path: filesystem path
    :return: message: coded message
    """
    logging.debug("read_json() %s", path)

    with _atomic_open(path, "r") as f:
        return json.load(f)


def write_json(path: str, data: Any, indent: int | None = None) -> None:
    """
    Writes data to the given path, in json format
    :param path: filesystem path
    :param data: the structure to write to disk
    :param indent: whether and how much to indent the output
    """
    logging.debug("write_json() %s", path)

    with _atomic_open(path, "w") as f:
        json.dump(data, f, indent=indent)


@contextlib.contextmanager
def read_csv(path: str) -> csv.DictReader:
    # Python docs say to use newline="", to support quoted multi-line fields
    with _atomic_open(path, "r", newline="") as csvfile:
        yield csv.DictReader(csvfile)


def read_ndjson(root: store.Root, path: str) -> Iterator[dict]:
    """Yields parsed json from the input ndjson file, line-by-line."""
    yield from cumulus_fhir_support.read_multiline_json(path, fsspec_fs=root.fs)


def read_resource_ndjson(
    root: store.Root, resources: str | set[str], warn_if_empty: bool = False
) -> Iterator[dict]:
    """
    Grabs all ndjson files from a folder, of a particular resource type.
    """
    if isinstance(resources, str):
        resources = {resources}
    for filename in ls_resources(root, resources, warn_if_empty=warn_if_empty):
        for line in cumulus_fhir_support.read_multiline_json(filename, fsspec_fs=root.fs):
            # Sanity check the incoming NDJSON - who knows what could happen and we should surface
            # a nice message about it. This *could* be very noisy on the console if there are a lot
            # of rows, but hopefully this is a very rare occurence and one that should be fixed
            # quickly.
            if (
                not isinstance(line, dict)  # god help us if non-dicts are in the input
                # folks have accidentally combined files
                or line.get("resourceType") not in resources
            ):
                logging.warning(f"Encountered invalid or unexpected FHIR: `{line}`")
                continue  # skip it
            yield line


def write_rows_to_ndjson(path: str, rows: list[dict], sparse: bool = False) -> None:
    """
    Writes the data out, row by row, to an .ndjson file (non-atomically).

    :param path: where to write the file
    :param rows: data to write
    :param sparse: if True, None entries are skipped
    """
    with NdjsonWriter(path, allow_empty=True) as f:
        for row in rows:
            if sparse:
                row = sparse_dict(row)
            f.write(row)


class NdjsonWriter:
    """
    Convenience context manager to write multiple objects to a ndjson file.

    Note that this is not atomic - partial writes will make it to the target file.
    """

    def __init__(
        self, path: str, append: bool = False, allow_empty: bool = False, compressed: bool = False
    ):
        self._root = store.Root(path)
        self._append = append
        self._compressed = compressed
        self._file = None
        if allow_empty:
            self._ensure_file()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        if self._file:
            self._file.close()
            self._file = None

    def _ensure_file(self):
        if not self._file:
            mode = "a" if self._append else "w"
            if self._compressed:
                inner_file = self._root.fs.open(self._root.path, mode + "b")
                self._file = gzip.open(inner_file, mode + "t", encoding="utf8")
            else:
                self._file = self._root.fs.open(self._root.path, mode + "t", encoding="utf8")

    def write(self, obj: dict) -> None:
        # lazily create the file, to avoid 0-line ndjson files (unless created in __init__)
        self._ensure_file()

        # Specify separators for the most compact (no whitespace) representation saves disk space.
        json.dump(obj, self._file, separators=(",", ":"))
        self._file.write("\n")


def read_local_line_count(path) -> int:
    """Reads a local file and provides the count of new line characters."""
    # From https://stackoverflow.com/a/27517681/239668
    # Copyright Michael Bacon, licensed CC-BY-SA 3.0
    count = 0
    buf = None
    open_func = gzip.open if path.casefold().endswith(".gz") else partial(open, buffering=0)
    with open_func(path, "rb") as f:
        bufgen = itertools.takewhile(
            lambda x: x, (f.read(1024 * 1024) for _ in itertools.repeat(None))
        )
        for buf in bufgen:
            count += buf.count(b"\n")
    if buf and buf[-1] != ord("\n"):  # catch a final line without a trailing newline
        count += 1
    return count


def sparse_dict(dictionary: dict) -> dict:
    """Returns a value of the input dictionary without any keys with None values."""

    def iteration(item: Any) -> Any:
        if isinstance(item, dict):
            return {key: iteration(val) for key, val in item.items() if val is not None}
        elif isinstance(item, list):
            return [iteration(x) for x in item]
        return item

    return iteration(dictionary)


###############################################################################
#
# Helper Functions: Logging
#
###############################################################################


def _pretty_float(num: float, precision: int = 1) -> str:
    """
    Returns a formatted float with trailing zeros chopped off.

    Could not find a cleaner builtin solution.
    Prior art: https://stackoverflow.com/questions/2440692/formatting-floats-without-trailing-zeros
    """
    return f"{num:.{precision}f}".rstrip("0").rstrip(".")


def human_file_size(count: int) -> str:
    """
    Returns a human-readable version of a count of bytes.

    I couldn't find a version of this that's sitting in a library we use. Very annoying.
    """
    for suffix in ("KB", "MB"):
        count /= 1024
        if count < 1024:
            return f"{_pretty_float(count)}{suffix}"
    return f"{_pretty_float(count / 1024)}GB"


def human_time_offset(seconds: int) -> str:
    """
    Returns a (fuzzy) human-readable version of a count of seconds.

    Examples:
      49 => "49s"
      90 => "1.5m"
      18000 => "5h"
    """
    if seconds < 60:
        return f"{seconds}s"

    minutes = seconds / 60
    if minutes < 60:
        return f"{_pretty_float(minutes)}m"

    hours = minutes / 60
    return f"{_pretty_float(hours)}h"


def print_header(name: str | None = None) -> None:
    """Prints a section break to the console, with a name for the user"""
    rich.get_console().rule()
    if name:
        print(name)


###############################################################################
#
# Helper Functions: date and time
#
###############################################################################


def datetime_now(local: bool = False) -> datetime.datetime:
    """
    Current date and time, suitable for use as a FHIR 'instant' data type

    The returned datetime is always 'aware' (not 'naive').

    :param local: whether to use local timezone or (if False) UTC
    """
    now = datetime.datetime.now(datetime.timezone.utc)
    if local:
        now = now.astimezone()
    return now


def timestamp_datetime(time: datetime.datetime | None = None) -> str:
    """
    Human-readable UTC date and time
    :return: MMMM-DD-YYY hh:mm:ss
    """
    time = time or datetime_now()
    return time.strftime("%Y-%m-%d %H:%M:%S")


def timestamp_filename(time: datetime.datetime | None = None) -> str:
    """
    Human-readable UTC date and time suitable for a filesystem path

    In particular, there are no characters that need awkward escaping.

    :return: MMMM-DD-YYY__hh.mm.ss
    """
    time = time or datetime_now()
    return time.strftime("%Y-%m-%d__%H.%M.%S")
