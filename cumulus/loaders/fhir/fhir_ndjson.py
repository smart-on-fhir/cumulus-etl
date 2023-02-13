"""Ndjson FHIR loader"""

import logging
import sys
import tempfile
from typing import List

from cumulus import common, errors, store
from cumulus.fhir_client import FatalError, FhirClient
from cumulus.loaders import base
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
        client: FhirClient,
        since: str = None,
        until: str = None,
    ):
        """
        :param root: location to load ndjson from
        :param client: client ready to talk to a FHIR server
        :param since: export start date for a FHIR server
        :param until: export end date for a FHIR server
        """
        super().__init__(root)
        self.client = client
        self.since = since
        self.until = until

    async def load_all(self, resources: List[str]) -> tempfile.TemporaryDirectory:
        # Are we doing a bulk FHIR export from a server?
        if self.root.protocol in ["http", "https"]:
            return await self._load_from_bulk_export(resources)

        if self.since or self.until:
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

    async def _load_from_bulk_export(self, resources: List[str]) -> tempfile.TemporaryDirectory:
        tmpdir = tempfile.TemporaryDirectory()  # pylint: disable=consider-using-with

        try:
            bulk_exporter = BulkExporter(self.client, resources, tmpdir.name, self.since, self.until)
            await bulk_exporter.export()
        except FatalError as exc:
            print(str(exc), file=sys.stderr)
            raise SystemExit(errors.BULK_EXPORT_FAILED) from exc

        return tmpdir
