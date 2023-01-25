"""Ndjson FHIR loader"""

import logging
import sys
import tempfile
from typing import List

from cumulus import common, errors, store
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
        if self.root.protocol in ["http", "https"]:
            return self._load_from_bulk_export(resources)

        # Copy the resources we need from the remote directory (like S3 buckets) to a local one.
        #
        # We do this even if the files are local, because the next step in our pipeline is the MS deid tool,
        # and it will just process *everything* in a directory. So if there are other *.ndjson sitting next to our
        # target resources, they'll get processed by the MS tool and that slows down running a single task with
        # "--task" a lot.
        #
        # This uses more disk space temporarily (copied files will get deleted once the MS tool is done and this
        # TemporaryDirectory gets discarded), but that seems reasonable.
        common.print_header("Copying ndjson input files...")
        tmpdir = tempfile.TemporaryDirectory()  # pylint: disable=consider-using-with
        for resource in resources:
            try:
                self.root.get(self.root.joinpath(f"*{resource}*.ndjson"), f"{tmpdir.name}/")
            except FileNotFoundError:
                logging.warning("No resources found for %s", resource)
        return tmpdir

    def _load_from_bulk_export(self, resources: List[str]) -> tempfile.TemporaryDirectory:
        # First, check that the extra arguments we need were provided
        error_list = []
        if not self.client_id:
            error_list.append("You must provide a client ID with --smart-client-id to connect to a SMART FHIR server.")
        if not self.jwks:
            error_list.append("You must provide a JWKS file with --smart-jwks to connect to a SMART FHIR server.")
        if error_list:
            print("\n".join(error_list), file=sys.stderr)
            raise SystemExit(errors.SMART_CREDENTIALS_MISSING)

        tmpdir = tempfile.TemporaryDirectory()  # pylint: disable=consider-using-with

        try:
            server = BackendServiceServer(self.root.path, self.client_id, self.jwks, resources)
            bulk_exporter = BulkExporter(server, resources, tmpdir.name)
            bulk_exporter.export()
        except FatalError as exc:
            print(str(exc), file=sys.stderr)
            raise SystemExit(errors.BULK_EXPORT_FAILED) from exc

        return tmpdir
