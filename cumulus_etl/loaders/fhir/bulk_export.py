"""Support for FHIR bulk exports"""

import asyncio
import datetime
import os
import urllib.parse
from collections.abc import Callable
from functools import partial

import httpx
import rich.live
import rich.text

from cumulus_etl import common, errors, fhir, http, store
from cumulus_etl.loaders.fhir import export_log


class BulkExporter:
    """
    Perform a bulk export from a FHIR server that supports the Backend Service SMART profile.

    This has been manually tested against at least:
    - The bulk-data-server test server (https://github.com/smart-on-fhir/bulk-data-server)
    - Cerner (https://www.cerner.com/)
    - Epic (https://www.epic.com/)
    """

    _TIMEOUT_THRESHOLD = 60 * 60 * 24 * 30  # thirty days (we've seen some multi-week Epic waits)

    def __init__(
        self,
        client: fhir.FhirClient,
        resources: set[str],
        url: str,
        destination: str,
        *,
        since: str | None = None,
        until: str | None = None,
        type_filter: list[str] | None = None,
        resume: str | None = None,
        prefer_url_resources: bool = False,
    ):
        """
        Initialize a bulk exporter (but does not start an export).

        :param client: a client ready to make requests
        :param resources: a list of resource names to export
        :param url: a target export URL (like https://example.com/Group/1234)
        :param destination: a local folder to store all the files
        :param since: start date for export
        :param until: end date for export
        :param type_filter: search filter for export (_typeFilter)
        :param resume: a polling status URL from a previous expor
        :param prefer_url_resources: if the URL includes _type, ignore the provided resources
        """
        super().__init__()
        self._client = client
        self._destination = destination
        self._total_wait_time = 0  # in seconds, across all our requests
        self._log: export_log.BulkExportLogWriter = None

        self._resume = resume
        if self._resume and ":" not in self._resume:
            # Decode URL first (we normally encode the URL for ease of copy-paste in the
            # terminal). We check if we need to first, to also support accepting resume URLs
            # that we didn't generate.
            self._resume = urllib.parse.unquote(self._resume)

        self._url = self._format_kickoff_url(
            url,
            resources=resources,
            since=since,
            until=until,
            type_filter=type_filter,
            prefer_url_resources=prefer_url_resources,
        )

        # Public properties, to be read after the export:
        self.export_datetime = None
        self.group_name = fhir.FhirUrl(self._url).group
        self.export_url = self._url

    def _format_kickoff_url(
        self,
        url: str,
        *,
        resources: set[str],
        since: str | None,
        until: str | None,
        type_filter: list[str] | None,
        prefer_url_resources: bool,
    ) -> str:
        parsed = urllib.parse.urlsplit(url)

        # Add an export path to the end of the URL if it's not provided
        if not parsed.path.endswith("/$export"):
            parsed = parsed._replace(path=os.path.join(parsed.path, "$export"))

        # Integrate in any externally-provided flags
        query = urllib.parse.parse_qs(parsed.query)
        ignore_provided_resources = prefer_url_resources and "_type" in query
        if not ignore_provided_resources:
            query.setdefault("_type", []).extend(resources)
        if type_filter:
            query.setdefault("_typeFilter", []).extend(type_filter)
        if since:
            query["_since"] = since
        if until:
            # This parameter is not part of the FHIR spec and is unlikely to be supported by your
            # server. But some custom servers *do* support it, so we added support for it too.
            query["_until"] = until

        # Combine repeated query params into a single comma-delimited params.
        # The spec says servers SHALL support repeated params (and even prefers them, claiming
        # that comma-delimited params may be deprecated in future releases).
        # But... Cerner doesn't support repeated _type at least.
        # So for wider compatibility, we'll condense into comma-delimited params.
        query = {k: ",".join(v) if isinstance(v, list) else v for k, v in query.items()}

        parsed = parsed._replace(query=urllib.parse.urlencode(query, doseq=True))

        return urllib.parse.urlunsplit(parsed)

    async def cancel(self) -> bool:
        if not self._resume:
            return False
        return await self._delete_export(self._resume)

    async def export(self) -> None:
        """
        Bulk export resources from a FHIR server into local ndjson files.

        This call will block for a while, as resources tend to be large, and we may also have to
        wait if the server is busy. Because it is a slow operation, this function will also print
        status updates to the console.

        After this completes, the destination folder will be full of files that look like
        Resource.000.ndjson, like so:

        destination/
          Encounter.000.ndjson
          Encounter.001.ndjson
          Patient.000.ndjson
          log.ndjson

        See http://hl7.org/fhir/uv/bulkdata/export/index.html for details.
        """
        self._log = export_log.BulkExportLogWriter(store.Root(self._destination))

        if self._resume:
            poll_location = self._resume
            self._log.export_id = poll_location
            print("Resuming bulk FHIR export… (all other export arguments ignored)")
        else:
            poll_location = await self._kick_off()
            print("Starting bulk FHIR export…")

        # Request status report, until export is done
        response = await self._request_with_delay_status(
            poll_location,
            headers={"Accept": "application/json"},
            retry_errors=True,
            log_progress=self._log.status_progress,
            log_error=self._log.status_error,
        )
        self._log.status_complete(response)

        # Finished! We're done waiting and can download all the files
        response_json = response.json()

        try:
            transaction_time = response_json.get("transactionTime", "")
            self.export_datetime = datetime.datetime.fromisoformat(transaction_time)
        except ValueError:
            pass  # server gave us a bad timestamp, ignore it :shrug:

        # Download all the files
        print("Bulk FHIR export finished, now downloading resources…")
        await self._download_all_ndjson_files(response_json, "output")
        await self._download_all_ndjson_files(response_json, "error")
        await self._download_all_ndjson_files(response_json, "deleted")

        self._log.export_complete()

        # If we raised an error in the above code, we intentionally will not reach this DELETE
        # call. If we had an issue talking to the server (like http errors), we want to leave the
        # files up there, so the user could try to manually recover.
        await self._delete_export(poll_location)

        # Were there any server-side errors during the export?
        error_texts, warning_texts = self._gather_all_messages()
        if warning_texts:
            print("\n - ".join(["Messages from server:", *warning_texts]))

        # Make sure we're fully done before we bail because the server told us the export has
        # issues. We still want to DELETE the export in this case. And we still want to download
        # all the files the server DID give us. Servers may have lots of ignorable errors that
        # need human review, before passing back to us as input ndjson.
        if error_texts:
            raise errors.FatalError("\n - ".join(["Errors occurred during export:", *error_texts]))

    ##############################################################################################
    #
    # Helpers
    #
    ##############################################################################################

    async def _kick_off(self):
        """Initiate bulk export"""
        try:
            response = await self._request_with_delay_status(
                self._url,
                headers={"Prefer": "respond-async"},
                target_status_code=202,
            )
        except Exception as exc:
            self._log.kickoff(self._url, self._client.get_capabilities(), exc)
            raise

        # Grab the poll location URL for status updates
        poll_location = response.headers["Content-Location"]
        self._log.export_id = poll_location

        self._log.kickoff(self._url, self._client.get_capabilities(), response)

        print()
        print("If interrupted, try again but add the following argument to resume the export:")
        rich.get_console().print(
            # Quote the poll location here only so that it's easier to copy & paste the whole
            # argument in one double-click. Colons are often used as separators in word-
            # highlighting algorithms.
            f"--resume={urllib.parse.quote(poll_location)}",
            style="bold",
            highlight=False,
            soft_wrap=True,
        )
        print()

        return poll_location

    async def _delete_export(self, poll_url: str) -> bool:
        """
        Send a DELETE to the polling location.

        This could be a mere kindness, so that the server knows it can delete the files.
        But it can also be necessary, as some servers (Epic at least) only let you do one export
        per client/group combo.
        """
        try:
            await self._request_with_delay_status(poll_url, method="DELETE", target_status_code=202)
            return True
        except errors.FatalError as err:
            # Don't bail on ETL as a whole, this isn't a show stopper error.
            print(f"Failed to clean up export job on the server side: {err}")
            return False

    async def _request_with_delay_status(self, *args, **kwargs) -> httpx.Response:
        """
        Requests a file, while respecting any requests to wait longer and telling the user.

        :returns: the HTTP response
        """
        status_box = rich.text.Text()
        with rich.get_console().status(status_box):
            response = await self._request_with_retries(*args, rich_text=status_box, **kwargs)

        if status_box.plain:
            print(f"  Waited for a total of {common.human_time_offset(self._total_wait_time)}")

        return response

    async def _request_with_retries(
        self,
        path: str,
        *,
        headers: dict | None = None,
        target_status_code: int = 200,
        method: str = "GET",
        log_request: Callable[[], None] | None = None,
        log_progress: Callable[[httpx.Response], None] | None = None,
        log_error: Callable[[Exception], None] | None = None,
        stream: bool = False,
        retry_errors: bool = False,
        rich_text: rich.text.Text | None = None,
    ) -> httpx.Response:
        """
        Requests a file, while respecting any requests to wait longer and telling the user.

        :param path: path to request
        :param headers: headers for request
        :param target_status_code: retries until this status code is returned
        :param method: HTTP method to request
        :param log_request: method to call to report every request attempt
        :param log_progress: method to call to report a successful request but not yet done
        :param log_error: method to call to report request failures
        :param stream: whether to stream the response
        :param retry_errors: if True, server-side errors will be retried a few times
        :returns: the HTTP response
        """

        def _add_new_delay(response: httpx.Response | None, delay: int) -> None:
            # Print a message to the user, so they don't see us do nothing for a while
            if rich_text is not None:
                progress_msg = response and response.headers.get("X-Progress")
                progress_msg = progress_msg or "waiting…"
                formatted_total = common.human_time_offset(self._total_wait_time)
                formatted_delay = common.human_time_offset(delay)
                rich_text.plain = (
                    f"{progress_msg} ({formatted_total} so far, waiting for {formatted_delay} more)"
                )

            self._total_wait_time += delay

        # Actually loop, attempting the request multiple times as needed
        while self._total_wait_time < self._TIMEOUT_THRESHOLD:
            response = await self._client.request(
                method,
                path,
                headers=headers,
                stream=stream,
                # These retry times are extremely generous - partly because we can afford to be
                # as a long-running async task and partly because EHR servers seem prone to
                # outages that clear up after a bit.
                retry_delays=[1, 2, 4, 8],  # five tries, 15 minutes total
                request_callback=log_request,
                error_callback=log_error,
                retry_callback=_add_new_delay,
            )

            if response.status_code == target_status_code:
                return response

            # 202 == server is still working on it.
            if response.status_code == 202:
                if log_progress:
                    log_progress(response)

                # Some servers can request unreasonably long delays (e.g. I've seen Cerner
                # ask for five hours), which is... not helpful for our UX and often way
                # too long for small exports. So limit the delay time to 5 minutes.
                delay = min(http.get_retry_after(response, 60), 300)

                _add_new_delay(response, delay)
                await asyncio.sleep(delay)

            else:
                # It feels silly to abort on an unknown *success* code, but the spec has such clear
                # guidance on what the expected response codes are, that it's not clear if a code
                # outside those parameters means we should keep waiting or stop waiting.
                # So let's be strict here for now.
                raise errors.NetworkError(
                    f"Unexpected status code {response.status_code} "
                    "from the bulk FHIR export server.",
                    response,
                )

        exc = errors.FatalError("Timed out waiting for the bulk FHIR export to finish.")
        if log_error:
            log_error(exc)
        raise exc

    def _gather_all_messages(self) -> (list[str], list[str]):
        """
        Parses all error/info ndjson files from the bulk export server.

        :returns: (error messages, non-fatal messages)
        """
        # The spec acknowledges that "error" is perhaps misleading for an array that can contain
        # info messages.
        error_folder = store.Root(f"{self._destination}/error")
        outcomes = common.read_resource_ndjson(error_folder, "OperationOutcome")

        fatal_messages = []
        info_messages = []
        for outcome in outcomes:
            for issue in outcome.get("issue", []):
                text = issue.get("diagnostics")
                text = text or issue.get("details", {}).get("text")
                text = text or issue.get("code")  # code is required at least
                if issue.get("severity") in ("fatal", "error"):
                    fatal_messages.append(text)
                else:
                    info_messages.append(text)

        return fatal_messages, info_messages

    async def _download_all_ndjson_files(self, resource_json: dict, item_type: str) -> None:
        """
        Downloads all exported ndjson files from the bulk export server.

        :param resource_json: the status response from bulk FHIR server
        :param item_type: which type of object to download: output, error, or deleted
        """
        files = resource_json.get(item_type, [])

        # Use the same (sensible) download folder layout as bulk-data-client:
        subfolder = "" if item_type == "output" else item_type

        resource_counts = {}  # how many of each resource we've seen
        coroutines = []
        for file in files:
            count = resource_counts.get(file["type"], -1) + 1
            resource_counts[file["type"]] = count
            filename = f"{file['type']}.{count:03}.ndjson"
            coroutines.append(
                self._download_ndjson_file(
                    file["url"],
                    file["type"],
                    os.path.join(self._destination, subfolder, filename),
                    item_type,
                ),
            )
        await asyncio.gather(*coroutines)

    async def _download_ndjson_file(
        self, url: str, resource_type: str, filename: str, item_type: str
    ) -> None:
        """
        Downloads a single ndjson file from the bulk export server.

        :param url: URL location of file to download
        :param resource_type: the resource type of the file
        :param filename: local path to write data to
        """
        decompressed_size = 0

        response = await self._request_with_retries(
            url,
            headers={"Accept": "application/fhir+ndjson"},
            stream=True,
            retry_errors=True,
            log_request=partial(self._log.download_request, url, item_type, resource_type),
            log_error=partial(self._log.download_error, url),
        )
        try:
            os.makedirs(os.path.dirname(filename), exist_ok=True)
            with open(filename, "w", encoding="utf8") as file:
                async for block in response.aiter_text():
                    file.write(block)
                    decompressed_size += len(block)
        except Exception as exc:
            self._log.download_error(url, exc)
            raise errors.FatalError(f"Error downloading '{url}': {exc}")
        finally:
            await response.aclose()

        lines = common.read_local_line_count(filename)
        self._log.download_complete(url, lines, decompressed_size)

        filename_last_part = filename.split("/")[-1]
        human_size = common.human_file_size(response.num_bytes_downloaded)
        print(f"  Downloaded {filename_last_part} ({human_size})")
