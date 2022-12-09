"""Ndjson FHIR loader"""

import sys
import tempfile
from typing import List

from cumulus import common, store
from cumulus.loaders import base
from cumulus.loaders.fhir.backend_service import BackendServiceServer, FatalError
from cumulus.loaders.fhir.bulk_export import BulkExporter


class FhirNdjsonLoader(base.Loader):
    """
    Loader for fhir ndjson data, either locally or from a FHIR server.

    Expected local-folder format is a folder with ndjson files labeled by resource type.
    (i.e. Condition.000.ndjson or Condition.ndjson)
    """

    def __init__(self, root: store.Root, client_id: str = None, jwks: str = None):
        """
        :param root: location to load ndjson from
        :param jwks: path to a JWKS file, used if root points at a FHIR server
        """
        super().__init__(root)
        try:
            self.client_id = common.read_text(client_id).strip() if client_id else None
        except FileNotFoundError:
            self.client_id = client_id
        self.jwks = common.read_json(jwks) if jwks else None

    def load_all(self, resources: List[str]) -> tempfile.TemporaryDirectory:
        # Are we doing a bulk FHIR export from a server?
        if self.root.protocol in ['http', 'https']:
            return self._load_from_bulk_export(resources)

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

    def _load_from_bulk_export(self, resources: List[str]) -> tempfile.TemporaryDirectory:
        # First, check that the extra arguments we need were provided
        errors = []
        if not self.client_id:
            errors.append('You must provide a client ID with --smart-client-id to connect to a SMART FHIR server.')
        if not self.jwks:
            errors.append('You must provide a JWKS file with --smart-jwks to connect to a SMART FHIR server.')
        if errors:
            print('\n'.join(errors), file=sys.stderr)
            raise SystemExit(1)

        tmpdir = tempfile.TemporaryDirectory()  # pylint: disable=consider-using-with

        try:
            server = BackendServiceServer(self.root.path, self.client_id, self.jwks, resources)
            bulk_exporter = BulkExporter(server, resources, tmpdir.name)
            bulk_exporter.export()
        except FatalError as exc:
            print(str(exc), file=sys.stderr)
            raise SystemExit(2) from exc  # just to differentiate from the 1 system exit above in tests

        return tmpdir
