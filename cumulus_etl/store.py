"""Abstraction for where to write and read data"""

import os
from collections.abc import Iterator
from urllib.parse import urlparse

import fsspec

_user_fs_options = {}  # don't access this directly, use get_fs_options()


def set_user_fs_options(args: dict) -> None:
    """Records user arguments that can affect filesystem options (like s3_region)"""
    _user_fs_options.update(args)


def get_fs_options(protocol: str) -> dict:
    """Provides a set of storage option kwargs for fsspec calls"""
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


class Root:
    """
    Abstraction for 'a place where we want to do some reading/writing'

    If you want to do any file I/O at all, use this class.

    Usually there are three roots in a given etl run:
    - Input folder
    - Output folder
    - PHI build folder (for codebook, etc.)

    This is mostly a coupling of a target path and the fsspec filesystem
    to use. With some useful methods mixed in.
    """

    def __init__(self, path: str, create=False):
        """
        :param path: location (local path or URL)
        :param create: whether to create the folder if it doesn't exist
        """
        parsed = urlparse(path)
        self.protocol = parsed.scheme or "file"  # assume local if no obvious scheme
        self.path = path if parsed.scheme else os.path.abspath(path)

        try:
            self.fs = fsspec.filesystem(self.protocol, **self.fsspec_options())
        except ValueError:
            # Some valid input URLs (like tcp://) aren't valid fsspec URLs, so allow a failure here.
            # If any of the more interesting calls in this class are made, we'll fail, but that's fine.
            self.fs = None

        if create:
            # Ensure we exist to start
            self.makedirs(self.path)

    def joinpath(self, *args) -> str:
        """Provides a child path based off of the root path"""
        return os.path.join(self.path, *args)

    def exists(self, path: str) -> bool:
        """Alias for os.path.exists"""
        return self.fs.exists(path)

    def get(self, rpath: str, lpath: str, *, recursive: bool = False) -> None:
        """Download files"""
        return self.fs.get(rpath, lpath, recursive=recursive)

    def put(self, lpath: str, rpath: str, *, recursive: bool = False) -> None:
        """Upload files"""
        return self.fs.put(lpath, rpath, recursive=recursive)

    def ls(self) -> Iterator[str]:
        return self.fs.ls(self.path, detail=False)

    def makedirs(self, path: str) -> None:
        """Ensures the given path and all parents are created"""
        if self.protocol == "s3":
            # s3 doesn't really care about folders, and if we try to make one,
            # fsspec would want the CreateBucket permission as it goes up the tree
            return
        self.fs.makedirs(path, exist_ok=True)

    def fsspec_options(self) -> dict:
        """Provides a set of storage option kwargs for fsspec calls"""
        return get_fs_options(self.protocol)
