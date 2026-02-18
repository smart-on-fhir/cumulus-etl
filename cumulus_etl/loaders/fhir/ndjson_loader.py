"""Ndjson FHIR loader"""

import tempfile

import cumulus_fhir_support as cfs

from cumulus_etl import cli_utils, common, errors, feedback, fhir, store
from cumulus_etl.loaders import base
from cumulus_etl.loaders.fhir.export_log import BulkExportLogParser


class FhirNdjsonLoader(base.Loader):
    """
    Loader for fhir ndjson data, either locally or from a FHIR server.
    """

    def __init__(self, root: store.Root):
        """
        :param root: location to load ndjson from
        """
        super().__init__(root)
        self.detected_files = None

        if root.is_http:
            errors.fatal(
                "The input folder cannot be an HTTP URL. If you are trying to export from an EHR, "
                "try running 'smart-fetch export' to a folder first.",
                errors.FEATURE_REMOVED,
            )

    @staticmethod
    def _scan_files(root: store.Root, progress: feedback.Progress) -> dict[str, str | None]:
        with progress.show_indeterminate_task("Scanning input files"):
            return cfs.list_multiline_json_in_dir(root.path, fsspec_fs=root.fs, recursive=True)

    async def detect_resources(self, *, progress: feedback.Progress) -> set[str] | None:
        self.detected_files = self._scan_files(self.root, progress)
        return {resource for resource in self.detected_files.values() if resource}

    async def load_resources(
        self, resources: set[str], *, progress: feedback.Progress
    ) -> base.LoaderResults:
        # Scan for files if needed (usually this is cached from a detect_resources() call, but it
        # might not be in bulk export mode or if --allow-missing-resources was passed
        found_files = self.detected_files
        if found_files is None:
            found_files = self._scan_files(self.root, progress)
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
            self.root.get(filename, f"{tmpdir.name}/")
            progress.advance(task)

        return self.read_loader_results(self.root, tmpdir)

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
