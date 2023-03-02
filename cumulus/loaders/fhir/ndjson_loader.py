"""Ndjson FHIR loader"""

import logging
import os
import tempfile
import urllib.parse
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
        export_to: str = None,
        since: str = None,
        until: str = None,
    ):
        """
        :param root: location to load ndjson from
        :param client: client ready to talk to a FHIR server
        :param export_to: folder to write the results into, instead of a temporary directory
        :param since: export start date for a FHIR server
        :param until: export end date for a FHIR server
        """
        super().__init__(root)
        self.client = client
        self.export_to = export_to
        self.since = since
        self.until = until

        # Do some quality checks on the export-to folder, if it was specified. Must be local, present, and empty.
        if self.export_to:
            if urllib.parse.urlparse(self.export_to).netloc:
                # We require a local folder because that's all that the MS deid tool can operate on.
                # If we were to relax this requirement, we'd want to copy the exported files over to a local dir.
                errors.fatal(
                    f"The target export folder '{self.export_to}' must be local. ", errors.BULK_EXPORT_FOLDER_NOT_LOCAL
                )

            try:
                if os.listdir(self.export_to):
                    errors.fatal(
                        f"The target export folder '{self.export_to}' already has contents. "
                        "Please provide an empty folder.",
                        errors.BULK_EXPORT_FOLDER_NOT_EMPTY,
                    )
            except FileNotFoundError:
                # Target folder doesn't exist, so let's make it
                os.makedirs(self.export_to, mode=0o700)

    async def load_all(self, resources: List[str]) -> base.Directory:
        # Are we doing a bulk FHIR export from a server?
        if self.root.protocol in ["http", "https"]:
            return await self._load_from_bulk_export(resources)

        if self.export_to or self.since or self.until:
            errors.fatal(
                "You provided FHIR bulk export parameters but did not provide a FHIR server", errors.ARGS_CONFLICT
            )

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

    async def _load_from_bulk_export(self, resources: List[str]) -> base.Directory:
        if self.export_to:
            target_dir = base.RealDirectory(self.export_to)
        else:
            target_dir = tempfile.TemporaryDirectory()  # pylint: disable=consider-using-with

        try:
            bulk_exporter = BulkExporter(
                self.client, resources, self.root.path, target_dir.name, self.since, self.until
            )
            await bulk_exporter.export()
        except FatalError as exc:
            errors.fatal(str(exc), errors.BULK_EXPORT_FAILED)

        return target_dir
