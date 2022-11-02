"""Ndjson FHIR loader"""

import tempfile

from cumulus.loaders.base import Loader


class FhirNdjsonLoader(Loader):
    """
    Loader for fhir ndjson data.

    Expected format is a folder with one ndjson files per category (i.e. Condition.ndjson)
    TODO: make this a little more flexible, once we know what we want here
    """

    def load_all(self) -> tempfile.TemporaryDirectory:
        if self.root.protocol == 'file':
            # We can actually just re-use the input dir without copying the files, since everything is local.
            class Dir:
                name: str = self.root.path
            return Dir()  # once we drop python3.7, we can have load_all return a Protocol for proper typing

        tmpdir = tempfile.TemporaryDirectory()  # pylint: disable=consider-using-with
        self.root.get(self.root.joinpath('*.ndjson'), f'{tmpdir.name}/')
        return tmpdir
