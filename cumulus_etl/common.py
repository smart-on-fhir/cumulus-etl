"""Utility methods"""

import contextlib
import csv
import datetime
import json
import logging
import re
from typing import Any, Iterator, List, Optional
from urllib.parse import urlparse

import fsspec


###############################################################################
#
# Helper Functions: listing files
#
###############################################################################


def ls_resources(root, resource: str) -> List[str]:
    pattern = re.compile(rf".*/([0-9]+.)?{resource}(.[0-9]+)?.ndjson")
    all_files = root.ls()
    return sorted(filter(pattern.match, all_files))


###############################################################################
#
# Helper Functions: reading/writing files
#
###############################################################################

_user_fs_options = {}  # don't access this directly, use get_fs_options()


def set_user_fs_options(args: dict) -> None:
    """Records user arguments that can affect filesystem options (like s3_region)"""
    _user_fs_options.update(args)


def get_fs_options(protocol: str) -> dict:
    """Provides a set of storage option kwargs for fsspec calls or pandas storage_options arguments"""
    options = {}

    if protocol == "s3":
        # Check for region manually. If you aren't using us-east-1, you usually need to specify the region
        # explicitly, and fsspec doesn't seem to check the environment variables for us, nor pull it from
        # ~/.aws/config
        region_name = _user_fs_options.get("s3_region")
        if region_name:
            options["client_kwargs"] = {"region_name": region_name}

        # Assume KMS encryption for now - we can make this tunable to AES256 if folks have a need.
        # But in general, I believe we want to enforce server side encryption when possible, KMS or not.
        options["s3_additional_kwargs"] = {
            "ServerSideEncryption": "aws:kms",
        }

        # Buckets can be set up to require a specific KMS key ID, so allow specifying it here
        kms_key = _user_fs_options.get("s3_kms_key")
        if kms_key:
            options["s3_additional_kwargs"]["SSEKMSKeyId"] = kms_key

    return options


def open_file(path: str, mode: str):
    """A version of open() that handles remote access, like to S3"""
    # Grab protocol if present
    parsed = urlparse(path)
    protocol = parsed.scheme or "file"  # assume local if no obvious scheme

    # We pass auto_mkdir because on some backends (like S3), we may not have permissions that fsspec might want,
    # like CreateBucket. We elsewhere call Root.makedirs as needed.
    return fsspec.open(path, mode, encoding="utf8", auto_mkdir=False, **get_fs_options(protocol))


def read_text(path: str) -> str:
    """
    Reads data from the given path, in text format
    :param path: (currently filesystem path)
    :return: message: coded message
    """
    logging.debug("read_text() %s", path)

    with open_file(path, "r") as f:
        return f.read()


def write_text(path: str, text: str) -> None:
    """
    Writes data to the given path, in text format
    :param path: filesystem path
    :param text: the text to write to disk
    """
    logging.debug("write_text() %s", path)

    with open_file(path, "w") as f:
        f.write(text)


def read_json(path: str) -> Any:
    """
    Reads json from a file
    :param path: filesystem path
    :return: message: coded message
    """
    logging.debug("read_json() %s", path)

    with open_file(path, "r") as f:
        return json.load(f)


def write_json(path: str, data: Any, indent: Optional[int] = None) -> None:
    """
    Writes data to the given path, in json format
    :param path: filesystem path
    :param data: the structure to write to disk
    :param indent: whether and how much to indent the output
    """
    logging.debug("write_json() %s", path)

    with open_file(path, "w") as f:
        json.dump(data, f, indent=indent)


@contextlib.contextmanager
def read_csv(path: str) -> csv.DictReader:
    with open(path, newline="", encoding="utf8") as csvfile:
        yield csv.DictReader(csvfile)


def read_ndjson(path: str) -> Iterator[dict]:
    """Yields parsed json from the input ndjson file, line-by-line."""
    with open_file(path, "r") as f:
        for line in f:
            yield json.loads(line)


def read_resource_ndjson(root, resource: str) -> Iterator[dict]:
    """
    Grabs all ndjson files from a folder, of a particular resource type.

    Supports filenames like Condition.ndjson, Condition.000.ndjson, or 1.Condition.ndjson.
    """
    for filename in ls_resources(root, resource):
        yield from read_ndjson(filename)


class NdjsonWriter:
    """Convenience context manager to write multiple objects to an ndjson file."""

    def __init__(self, path: str, mode: str = "w"):
        self._path = path
        self._mode = mode
        self._file = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        if self._file:
            self._file.close()
            self._file = None

    def write(self, obj: dict) -> None:
        # lazily create the file, to avoid 0-line ndjson files
        if not self._file:
            self._file = open(self._path, self._mode, encoding="utf8")  # pylint: disable=consider-using-with

        json.dump(obj, self._file)
        self._file.write("\n")


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
    Pandas has one, but it's private.
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


def info_mode():
    logging.basicConfig()
    logging.getLogger().setLevel(logging.INFO)


def debug_mode():
    logging.basicConfig()
    logging.getLogger().setLevel(logging.DEBUG)


def warn_mode():
    logging.basicConfig()
    logging.getLogger().setLevel(logging.WARN)


_first_header = True


def print_header(name: str) -> None:
    """Prints a section break to the console, with a name for the user"""
    global _first_header
    if not _first_header:
        print("###############################################################")
    _first_header = False
    print(name)


###############################################################################
#
# Helper Functions: date and time
#
###############################################################################


def datetime_now() -> datetime.datetime:
    """
    UTC date and time, suitable for use as a FHIR 'instant' data type
    """
    return datetime.datetime.now(datetime.timezone.utc)


def timestamp_datetime(time: datetime.datetime = None) -> str:
    """
    Human-readable UTC date and time
    :return: MMMM-DD-YYY hh:mm:ss
    """
    time = time or datetime_now()
    return time.strftime("%Y-%m-%d %H:%M:%S")


def timestamp_filename(time: datetime.datetime = None) -> str:
    """
    Human-readable UTC date and time suitable for a filesystem path

    In particular, there are no characters that need awkward escaping.

    :return: MMMM-DD-YYY__hh.mm.ss
    """
    time = time or datetime_now()
    return time.strftime("%Y-%m-%d__%H.%M.%S")