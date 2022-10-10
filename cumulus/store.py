"""Abstraction for where to write and read data"""

import abc
import os
from urllib.parse import urlparse

import fsspec
import pandas


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
        self.path = path

        parsed = urlparse(path)
        self.protocol = parsed.scheme or 'file'  # assume local if no obvious scheme

        options = self.fsspec_options()
        self.fs = fsspec.filesystem(self.protocol, **options)

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

    def makedirs(self, path: str) -> None:
        """Ensures the given path and all parents are created"""
        self._confirm_in_root(path)
        self.fs.makedirs(path, exist_ok=True)

    def rm(self, path: str) -> None:
        """Delete a file (alias for fs.rm)"""
        self._confirm_in_root(path)
        self.fs.rm(path)

    def fsspec_options(self) -> dict:
        """Provides a set of storage option kwargs for fsspec calls or pandas storage_options arguments"""
        if self.protocol == 's3':
            # Assume KMS encryption for now - we can make this tunable to AES256 if folks have a need.
            # But in general, I believe we want to enforce server side encryption when possible, KMS or not.
            return {
                's3_additional_kwargs': {
                    'ServerSideEncryption': 'aws:kms',
                },
            }
        return {}


class Format(abc.ABC):
    """
    An abstraction for how to write cumulus output

    Subclass this to provide a different output format (like ndjson or parquet).
    """

    def __init__(self, root: Root):
        """
        Initialize a new Format class
        :param root: the base location to write data to
        """
        self.root = root

    @abc.abstractmethod
    def store_conditions(self, job, conditions: pandas.DataFrame) -> None:
        pass

    @abc.abstractmethod
    def store_docrefs(self, job, docrefs: pandas.DataFrame) -> None:
        pass

    @abc.abstractmethod
    def store_encounters(self, job, encounters: pandas.DataFrame) -> None:
        pass

    @abc.abstractmethod
    def store_labs(self, job, labs: pandas.DataFrame) -> None:
        pass

    @abc.abstractmethod
    def store_patients(self, job, patients: pandas.DataFrame) -> None:
        pass

    @abc.abstractmethod
    def store_symptoms(self, job, observations: pandas.DataFrame) -> None:
        pass
