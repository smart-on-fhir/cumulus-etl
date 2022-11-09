"""Ndjson FHIR loader"""

import tempfile

from cumulus.loaders import base


class FhirNdjsonLoader(base.Loader):
    """
    Loader for fhir ndjson data.

    Expected local-folder format is a folder with ndjson files labeled by resource type.
    (i.e. Condition.000.ndjson or Condition.ndjson)
    """

    def load_all(self) -> tempfile.TemporaryDirectory:
        # Are we reading from a local directory?
        if self.root.protocol == 'file':
            # We can actually just re-use the input dir without copying the files, since everything is local.
            class Dir:
                name: str = self.root.path
            return Dir()  # once we drop python3.7, we can have load_all return a Protocol for proper typing

        # Fall back to copying from a remote directory (like S3 buckets) to a local one
        tmpdir = tempfile.TemporaryDirectory()  # pylint: disable=consider-using-with
        self.root.get(self.root.joinpath('*.ndjson'), f'{tmpdir.name}/')
        return tmpdir
