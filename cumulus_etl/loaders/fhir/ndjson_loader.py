"""Ndjson FHIR loader"""

import tempfile

from cumulus_etl import cli_utils, common, errors, fhir, store
from cumulus_etl.loaders import base
from cumulus_etl.loaders.fhir.bulk_export import BulkExporter
from cumulus_etl.loaders.fhir.export_log import BulkExportLogParser


class FhirNdjsonLoader(base.Loader):
    """
    Loader for fhir ndjson data, either locally or from a FHIR server.
    """

    def __init__(
        self,
        root: store.Root,
        client: fhir.FhirClient = None,
        export_to: str | None = None,
        since: str | None = None,
        until: str | None = None,
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

    async def load_all(self, resources: list[str]) -> common.Directory:
        # Are we doing a bulk FHIR export from a server?
        if self.root.protocol in ["http", "https"]:
            loaded_dir = await self._load_from_bulk_export(resources)
            input_root = store.Root(loaded_dir.name)
        else:
            if self.export_to or self.since or self.until:
                errors.fatal(
                    "You provided FHIR bulk export parameters but did not provide a FHIR server", errors.ARGS_CONFLICT
                )

            input_root = self.root

            # Parse logs for export information
            try:
                parser = BulkExportLogParser(input_root)
                self.group_name = parser.group_name
                self.export_datetime = parser.export_datetime
            except BulkExportLogParser.LogParsingError:
                # Once we require group name & export datetime, we should warn about this.
                # For now, just ignore any errors.
                pass
        # Copy the resources we need from the remote directory (like S3 buckets) to a local one.
        #
        # We do this even if the files are local, because the next step in our pipeline is the MS deid tool,
        # and it will just process *everything* in a directory. So if there are other *.ndjson sitting next to our
        # target resources, they'll get processed by the MS tool and that slows down running a single task with
        # "--task" a lot. (Or it'll be invalid FHIR ndjson like our log.ndjson and the MS tool will complain.)
        #
        # This uses more disk space temporarily (copied files will get deleted once the MS tool is done and this
        # TemporaryDirectory gets discarded), but that seems reasonable.
        print("Copying ndjson input files…")
        tmpdir = tempfile.TemporaryDirectory()  # pylint: disable=consider-using-with
        filenames = common.ls_resources(input_root, set(resources), warn_if_empty=True)
        for filename in filenames:
            input_root.get(filename, f"{tmpdir.name}/")
        return tmpdir

    async def _load_from_bulk_export(self, resources: list[str]) -> common.Directory:
        target_dir = cli_utils.make_export_dir(self.export_to)

        try:
            bulk_exporter = BulkExporter(
                self.client, resources, self.root.path, target_dir.name, self.since, self.until
            )
            await bulk_exporter.export()

            # Copy back these settings from the export
            self.group_name = bulk_exporter.group_name
            self.export_datetime = bulk_exporter.export_datetime

        except errors.FatalError as exc:
            errors.fatal(str(exc), errors.BULK_EXPORT_FAILED)

        return target_dir
