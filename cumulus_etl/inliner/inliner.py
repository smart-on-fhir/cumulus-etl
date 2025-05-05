import base64
import dataclasses
import hashlib
import tempfile
from collections.abc import Iterable
from functools import partial

import cumulus_fhir_support
import fsspec
import rich.progress
import rich.table

from cumulus_etl import cli_utils, common, errors, fhir, store
from cumulus_etl.inliner import reader, writer


@dataclasses.dataclass
class InliningStats:
    total_attachments: int = 0
    total_resources: int = 0

    already_inlined_attachments: int = 0
    already_inlined_resources: int = 0

    newly_inlined_attachments: int = 0
    newly_inlined_resources: int = 0

    fatal_error_attachments: int = 0
    fatal_error_resources: int = 0

    fatal_retry_attachments: int = 0
    fatal_retry_resources: int = 0

    def merge_attachment_stats(self, other: "InliningStats") -> None:
        """Takes stats for attachments in a resource and merges them into the resource's stats"""
        for field in {
            "total_attachments",
            "already_inlined_attachments",
            "newly_inlined_attachments",
            "fatal_error_attachments",
            "fatal_retry_attachments",
        }:
            if other_val := getattr(other, field):
                setattr(self, field, getattr(self, field) + other_val)
                resource_field = field.replace("attachments", "resources")
                setattr(self, resource_field, getattr(self, resource_field) + 1)


async def inliner(
    client: fhir.FhirClient,
    in_root: store.Root,
    resources: Iterable[str],
    mimetypes: Iterable[str],
) -> None:
    mimetypes = set(mimetypes)

    # Grab files to read for the given resources
    found_files = cumulus_fhir_support.list_multiline_json_in_dir(
        in_root.path, resources, fsspec_fs=in_root.fs
    )

    # Predict how much work we'll have to do by getting counts of lines and files
    if in_root.protocol == "file":
        total_lines = sum(common.read_local_line_count(path) for path in found_files)
    else:
        # We shouldn't double our network usage just for pretty progress bars...
        total_lines = 0
    total_files = len(found_files)

    # Actually do the work
    stats = InliningStats()
    with cli_utils.make_progress_bar() as progress:
        progress_task = progress.add_task("Inlining…", total=total_lines or total_files)
        for path in found_files:
            await _inline_one_file(
                client,
                path,
                in_root.fs,
                mimetypes=mimetypes,
                stats=stats,
                progress=progress if total_lines else None,
                progress_task=progress_task if total_lines else None,
            )
            if not total_lines:
                progress.update(progress_task, advance=1)

    table = rich.table.Table(
        "",
        rich.table.Column(header="Attachments", justify="right"),
        rich.table.Column(header="Resources", justify="right"),
        box=None,
    )
    table.add_row("Total examined", f"{stats.total_attachments:,}", f"{stats.total_resources:,}")
    if stats.already_inlined_attachments:
        table.add_row(
            "Already inlined",
            f"{stats.already_inlined_attachments:,}",
            f"{stats.already_inlined_resources:,}",
        )
    table.add_row(
        "Newly inlined",
        f"{stats.newly_inlined_attachments:,}",
        f"{stats.newly_inlined_resources:,}",
    )
    if stats.fatal_error_attachments:
        table.add_row(
            "Fatal errors", f"{stats.fatal_error_attachments:,}", f"{stats.fatal_error_resources:,}"
        )
    if stats.fatal_retry_resources:
        table.add_row(
            "Retried but gave up",
            f"{stats.fatal_retry_attachments:,}",
            f"{stats.fatal_retry_resources:,}",
        )
    rich.get_console().print(table)


async def _inline_one_file(
    client: fhir.FhirClient,
    path: str,
    fs: fsspec.AbstractFileSystem,
    *,
    mimetypes: set[str],
    stats: InliningStats,
    progress: rich.progress.Progress | None,
    progress_task: rich.progress.TaskID | None,
) -> None:
    compressed = path.casefold().endswith(".gz")
    with tempfile.NamedTemporaryFile() as output_file:
        # Use an ordered NDJSON writer to preserve the order of lines in the input file,
        # which preserves the ability for users to append updated row data to files.
        with writer.OrderedNdjsonWriter(output_file.name, compressed=compressed) as output:
            await reader.peek_ahead_processor(
                cumulus_fhir_support.read_multiline_json(path, fsspec_fs=fs),
                partial(
                    _inline_one_line,
                    client=client,
                    mimetypes=mimetypes,
                    output=output,
                    stats=stats,
                    progress=progress,
                    progress_task=progress_task,
                ),
                # Look at twice the allowed connections - downloads will block, but that's OK.
                # This will allow us to download some attachments while other workers are sleeping
                # because they are waiting to retry due to an HTTP error.
                peek_at=fhir.FhirClient.MAX_CONNECTIONS * 2,
            )
        # Atomically swap out the inlined version with the original
        with fs.transaction:
            fs.put_file(output_file.name, path)


async def _inline_one_line(
    index: int,
    resource: dict,
    *,
    client: fhir.FhirClient,
    mimetypes: set[str],
    output: writer.OrderedNdjsonWriter,
    stats: InliningStats,
    progress: rich.progress.Progress | None,
    progress_task: rich.progress.TaskID | None,
) -> None:
    match resource.get("resourceType"):
        case "DiagnosticReport":
            attachments = resource.get("presentedForm", [])
        case "DocumentReference":
            attachments = [
                content["attachment"]
                for content in resource.get("content", [])
                if "attachment" in content
            ]
        case _:
            attachments = []  # don't do anything, but we will leave the resource line in place

    attachment_stats = InliningStats()
    for attachment in attachments:
        await _inline_one_attachment(
            client, attachment, mimetypes=mimetypes, stats=attachment_stats
        )
    stats.merge_attachment_stats(attachment_stats)

    output.write(index, resource)
    if progress:
        progress.update(progress_task, advance=1)


async def _inline_one_attachment(
    client: fhir.FhirClient, attachment: dict, *, mimetypes: set[str], stats: InliningStats
) -> None:
    # First, check if we should even examine this attachment
    if "contentType" not in attachment:
        return
    mimetype, _charset = fhir.parse_content_type(attachment["contentType"])
    if mimetype not in mimetypes:
        return

    # OK - this is a valid attachment to process
    stats.total_attachments += 1

    if "data" in attachment:
        stats.already_inlined_attachments += 1
        return

    if "url" not in attachment:
        return  # neither data nor url... nothing to do

    try:
        response = await fhir.request_attachment(client, attachment)
    except errors.FatalNetworkError:
        stats.fatal_error_attachments += 1
        return
    except errors.TemporaryNetworkError:
        stats.fatal_retry_attachments += 1
        return

    stats.newly_inlined_attachments += 1

    attachment["data"] = base64.standard_b64encode(response.content).decode("ascii")
    # Overwrite other associated metadata with latest info (existing metadata might now be stale)
    attachment["contentType"] = f"{mimetype}; charset={response.encoding}"
    attachment["size"] = len(response.content)
    sha1_hash = hashlib.sha1(response.content).digest()  # noqa: S324
    attachment["hash"] = base64.standard_b64encode(sha1_hash).decode("ascii")
