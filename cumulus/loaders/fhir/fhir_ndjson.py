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

    def __init__(
        self,
        root: store.Root,
        client_id: str = None,
        jwks: str = None,
        bearer_token: str = None,
        since: str = None,
        until: str = None,
    ):
        """
        :param root: location to load ndjson from
        :param client_id: client ID for a SMART server
        :param jwks: path to a JWKS file for a SMART server
        :param bearer_token: path to a file with a bearer token for a FHIR server
        :param since: export start date for a FHIR server
        :param until: export end date for a FHIR server
        """
        super().__init__(root)

        try:
            try:
                self.client_id = common.read_text(client_id).strip() if client_id else None
            except FileNotFoundError:
                self.client_id = client_id

            self.jwks = common.read_json(jwks) if jwks else None
            self.bearer_token = common.read_text(bearer_token).strip() if bearer_token else None
        except OSError as exc:
            print(exc, file=sys.stderr)
            raise SystemExit(errors.ARGS_INVALID) from exc

        self.since = since
        self.until = until

    def load_all(self, resources: List[str]) -> tempfile.TemporaryDirectory:
        # Are we doing a bulk FHIR export from a server?
        if self.root.protocol in ["http", "https"]:
            return self._load_from_bulk_export(resources)

        if self.client_id or self.jwks or self.bearer_token or self.since or self.until:
            print("You provided FHIR bulk export parameters but did not provide a FHIR server", file=sys.stderr)
            raise SystemExit(errors.ARGS_CONFLICT)

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
        tmpdir = tempfile.TemporaryDirectory()  # pylint: disable=consider-using-with

        try:
            server = BackendServiceServer(
                self.root.path, resources, client_id=self.client_id, jwks=self.jwks, bearer_token=self.bearer_token
            )
            bulk_exporter = BulkExporter(server, resources, tmpdir.name, self.since, self.until)
            bulk_exporter.export()
        except FatalError as exc:
            print(str(exc), file=sys.stderr)
            raise SystemExit(errors.BULK_EXPORT_FAILED) from exc

        return tmpdir
