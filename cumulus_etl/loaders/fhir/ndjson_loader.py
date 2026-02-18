"""Ndjson FHIR loader"""

import tempfile

import cumulus_fhir_support as cfs

from cumulus_etl import cli_utils, common, errors, feedback, fhir, store
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
        client: cfs.FhirClient = None,
        export_to: str | None = None,
        since: str | None = None,
        until: str | None = None,
        type_filter: list[str] | None = None,
        resume: str | None = None,
    ):
        """
        :param root: location to load ndjson from
        :param client: client ready to talk to a FHIR server
        :param export_to: folder to write the results into, instead of a temporary directory
        :param since: export start date for a FHIR server
        :param until: export end date for a FHIR server
        :param resume: a polling status URL from a previous expor
        """
        super().__init__(root)
        self.client = client
        self.export_to = export_to
        self.since = since
        self.until = until
        self.type_filter = type_filter
        self.resume = resume
        self.detected_files = None

    @staticmethod
    def _scan_files(root: store.Root, progress: feedback.Progress) -> dict[str, str | None]:
        with progress.show_indeterminate_task("Scanning input files"):
            return cfs.list_multiline_json_in_dir(root.path, fsspec_fs=root.fs, recursive=True)

    async def detect_resources(self, *, progress: feedback.Progress) -> set[str] | None:
        if self.root.is_http:
            # We haven't done the export yet, so there are no files to inspect yet.
            # Returning None means "dunno" (i.e. "just accept whatever you eventually get").
            return None

        self.detected_files = self._scan_files(self.root, progress)
        return {resource for resource in self.detected_files.values() if resource}

    async def load_resources(
        self, resources: set[str], *, progress: feedback.Progress
    ) -> base.LoaderResults:
        # Are we doing a bulk FHIR export from a server?
        if self.root.is_http:
            bulk_dir = await self.load_from_bulk_export(resources)
            input_root = store.Root(bulk_dir.name)
        else:
            if self.export_to or self.since or self.until or self.resume:
                errors.fatal(
                    "You provided FHIR bulk export parameters but did not provide a FHIR server",
                    errors.ARGS_CONFLICT,
                )
            input_root = self.root

        # Scan for files if needed (usually this is cached from a detect_resources() call, but it
        # might not be in bulk export mode or if --allow-missing-resources was passed
        found_files = self.detected_files
        if found_files is None:
            found_files = self._scan_files(input_root, progress)
        common.warn_on_missing_resources(found_files, resources)

        # Gather filenames of interest
        target_types = resources | fhir.linked_resources(resources)
        filenames = [path for path, res_type in found_files.items() if res_type in target_types]

        # Copy the resources we need from the remote directory (like S3 buckets) to a local one.
        #
        # We do this as a performance improvement, because tasks will each scan the input dir,
        # looking for their own resource files. In order to remove this, we'd need to persist the
        # initial scan in this class down to the tasks and have them re-use the results.
        #
        # Until then, we'll just use a little more time and disk space.
        tmpdir = tempfile.TemporaryDirectory()

        task = progress.add_task(description="Copying input files", total=len(filenames))
        for filename in filenames:
            input_root.get(filename, f"{tmpdir.name}/")
            progress.advance(task)

        return self.read_loader_results(input_root, tmpdir)

    async def load_from_bulk_export(
        self, resources: set[str], prefer_url_resources: bool = False
    ) -> common.Directory:
        """
        Performs a bulk export and drops the results in an export dir.

        :param resources: a list of resources to load
        :param prefer_url_resources: if the URL includes _type, ignore the provided resources
        """
        target_dir = cli_utils.make_export_dir(self.export_to)

        bulk_exporter = BulkExporter(
            self.client,
            resources,
            self.root.path,
            target_dir.name,
            since=self.since,
            until=self.until,
            type_filter=self.type_filter,
            resume=self.resume,
            prefer_url_resources=prefer_url_resources,
        )
        await bulk_exporter.export()

        return target_dir

    def read_loader_results(
        self, input_root: store.Root, results_dir: common.Directory
    ) -> base.LoaderResults:
        results = base.LoaderResults(
            directory=results_dir,
            deleted_ids=self.read_deleted_ids(input_root),
        )

        # Parse logs for export information
        try:
            parser = BulkExportLogParser(input_root)
            results.group_name = parser.group_name
            results.export_datetime = parser.export_datetime
            results.export_url = parser.export_url
        except BulkExportLogParser.LogParsingError:
            # Once we require group name & export datetime, we should warn about this.
            # For now, just ignore any errors.
            pass

        return results

    def read_deleted_ids(self, root: store.Root) -> dict[str, set[str]]:
        """
        Reads any deleted IDs that a bulk export gave us.

        See https://hl7.org/fhir/uv/bulkdata/export.html for details.
        """
        deleted_ids = {}

        subdir = store.Root(root.joinpath("deleted"))
        bundles = common.read_resource_ndjson(subdir, "Bundle")
        for bundle in bundles:
            if bundle.get("type") != "transaction":
                continue
            for entry in bundle.get("entry", []):
                request = entry.get("request", {})
                if request.get("method") != "DELETE":
                    continue
                url = request.get("url")
                # Sanity check that we have a relative URL like "Patient/123"
                if not url or url.count("/") != 1:
                    continue
                resource, res_id = url.split("/")
                deleted_ids.setdefault(resource, set()).add(res_id)

        return deleted_ids
