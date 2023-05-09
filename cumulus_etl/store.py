"""Abstraction for where to write and read data"""

import os
from typing import Iterator
from urllib.parse import urlparse

import fsspec

from cumulus_etl import common


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
        self.path = path

        parsed = urlparse(path)
        self.protocol = parsed.scheme or "file"  # assume local if no obvious scheme

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

    def _confirm_in_root(self, path: str) -> None:
        """
        Make sure that a provided path is actually in our root

        Just for sanity, confirm that any paths provided to us actually belong
        underneath us.
        """
        if not path.startswith(self.path):
            raise ValueError(f'Path "{path}" is not inside root "{self.path}"')

    def exists(self, path: str) -> bool:
        """Alias for os.path.exists"""
        self._confirm_in_root(path)
        return self.fs.exists(path)

    def get(self, rpath: str, lpath: str, *, recursive: bool = False) -> None:
        """Download files"""
        self._confirm_in_root(rpath)
        return self.fs.get(rpath, lpath, recursive=recursive)

    def put(self, lpath: str, rpath: str, *, recursive: bool = False) -> None:
        """Upload files"""
        self._confirm_in_root(rpath)
        return self.fs.put(lpath, rpath, recursive=recursive)

    def ls(self) -> Iterator[str]:
        files = self.fs.ls(self.path, detail=False)

        # Backends like S3 rudely don't include the protocol prefix in the results.
        # So when we try to actually use the filename, fsspec gets confused and thinks it's a local path.
        # If the backend is trying to do us dirty, we'll just re-insert the protocol.
        if self.protocol != "file" and files and not files[0].startswith(f"{self.protocol}://"):
            files = [f"{self.protocol}://{filename}" for filename in files]

        return files

    def makedirs(self, path: str) -> None:
        """Ensures the given path and all parents are created"""
        self._confirm_in_root(path)
        if self.protocol == "s3":
            # s3 doesn't really care about folders, and if we try to make one,
            # fsspec would want the CreateBucket permission as it goes up the tree
            return
        self.fs.makedirs(path, exist_ok=True)

    def rm(self, path: str, recursive=False) -> None:
        """Delete a file (alias for fs.rm)"""
        self._confirm_in_root(path)
        self.fs.rm(path, recursive=recursive)

    def fsspec_options(self) -> dict:
        """Provides a set of storage option kwargs for fsspec calls or pandas storage_options arguments"""
        return common.get_fs_options(self.protocol)
