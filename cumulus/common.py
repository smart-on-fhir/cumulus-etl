"""Utility methods"""

import datetime
import json
import os
import logging
from typing import Any, Optional
from urllib.parse import urlparse

import fsspec


###############################################################################
#
# Helper Functions: Pandas / CSV / SQL
#
###############################################################################
def list_csv(folder: str, mask=".csv") -> list:
    """
    :param folder: folder to select files from
    :param mask: csv is typical
    :return:
    """
    if not os.path.exists(folder):
        logging.warning("list_csv() does not exist: folder:%s, mask=%s", folder, mask)
        return []

    match = []
    for file in sorted(os.listdir(folder)):
        if file.endswith(mask):
            match.append(os.path.join(folder, file))
    return match


def find_by_name(folder, path_contains="filemask", progress_bar=1000) -> list:
    """
    :param folder: root folder where JSON files are stored
    :param startswith: ctakes.json default, or other file pattern.
    :param progress_bar: show progress status for every ### files found
    :return:
    """
    del progress_bar

    found = []
    for dirpath, _, files in os.walk(folder):
        for filename in files:
            path = os.path.join(dirpath, filename)
            if path_contains in path:
                found.append(path)
                if 0 == len(found) % 1000:
                    print(f"found: {len(found)}")
                    print(path)
    return found


###############################################################################
#
# Helper Functions: read/write text and JSON with logging messages
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


###############################################################################
#
# Helper Functions: Logging
#
###############################################################################


def human_file_size(count: int) -> str:
    """
    Returns a human-readable version of a count of bytes.

    I couldn't find a version of this that's sitting in a library we use. Very annoying.
    Pandas has one, but it's private.
    """
    for suffix in ("KB", "MB"):
        count /= 1024
        if count < 1024:
            return f"{count:.1f}{suffix}"
    return f"{count / 1024:.1f}GB"


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
# Helper Functions: Timedatestamp
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
