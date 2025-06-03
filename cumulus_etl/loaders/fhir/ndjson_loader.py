"""Ndjson FHIR loader"""

import gzip
import os
import shutil
import tempfile

import cumulus_fhir_support

from cumulus_etl import cli_utils, common, errors, fhir, inliner, store
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
        type_filter: list[str] | None = None,
        resume: str | None = None,
        inline: bool = False,
        inline_resources: set[str] | None = None,
        inline_mimetypes: set[str] | None = None,
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
        self.inline = inline
        self.inline_resources = inline_resources
        self.inline_mimetypes = inline_mimetypes

    async def detect_resources(self) -> set[str] | None:
        if self.root.protocol in {"http", "https"}:
            # We haven't done the export yet, so there are no files to inspect yet.
            # Returning None means "dunno" (i.e. "just accept whatever you eventually get").
            return None

        found_files = cumulus_fhir_support.list_multiline_json_in_dir(
            self.root.path, fsspec_fs=self.root.fs
        )
        return {resource for resource in found_files.values() if resource}

    async def load_resources(self, resources: set[str]) -> base.LoaderResults:
        # Are we doing a bulk FHIR export from a server?
        if self.root.protocol in ["http", "https"]:
            bulk_dir = await self.load_from_bulk_export(resources)
            input_root = store.Root(bulk_dir.name)
        else:
            if self.export_to or self.since or self.until or self.resume or self.inline:
                errors.fatal(
                    "You provided FHIR bulk export parameters but did not provide a FHIR server",
                    errors.ARGS_CONFLICT,
                )
            input_root = self.root

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
        tmpdir = tempfile.TemporaryDirectory()
        filenames = common.ls_resources(input_root, resources, warn_if_empty=True)
        filenames += common.ls_resources(input_root, fhir.linked_resources(resources))
        for filename in filenames:
            input_root.get(filename, f"{tmpdir.name}/")
            # Decompress any *.gz files, because the MS tool can't understand them
            self._decompress_file(f"{tmpdir.name}/{os.path.basename(filename)}")

        return self.read_loader_results(input_root, tmpdir)

    @staticmethod
    def _decompress_file(path: str):
        if not path.casefold().endswith(".gz"):
            return
        target_path = path[:-3]
        with gzip.open(path, "rb") as f_in:
            with open(target_path, "wb") as f_out:
                shutil.copyfileobj(f_in, f_out)
        os.remove(path)

    async def load_from_bulk_export(
        self, resources: set[str], prefer_url_resources: bool = False
    ) -> common.Directory:
        """
        Performs a bulk export and drops the results in an export dir.

        :param resources: a list of resources to load
        :param prefer_url_resources: if the URL includes _type, ignore the provided resources
        """
        target_dir = cli_utils.make_export_dir(self.export_to)

        if self.inline and not self.export_to:
            errors.fatal(
                "Attachment inlining requested, but without an export folder. "
                "If you want to save inlined attachments for archiving, please specify an "
                "export folder to preserve the downloaded NDJSON with --export-to. "
                "See --help for more information.",
                errors.INLINE_WITHOUT_FOLDER,
            )

        try:
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

        except errors.FatalError as exc:
            errors.fatal(str(exc), errors.BULK_EXPORT_FAILED)

        if self.inline:
            common.print_header()
            await inliner.inliner(
                self.client,
                store.Root(target_dir.name),
                self.inline_resources,
                self.inline_mimetypes,
            )

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
