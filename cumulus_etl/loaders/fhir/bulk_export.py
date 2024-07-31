"""Support for FHIR bulk exports"""

import asyncio
import datetime
import json
import os
import urllib.parse
from collections.abc import Callable
from functools import partial

import httpx
import rich.live
import rich.text

from cumulus_etl import common, errors, fhir, store
from cumulus_etl.loaders.fhir import export_log


class BulkExporter:
    """
    Perform a bulk export from a FHIR server that supports the Backend Service SMART profile.

    This has been manually tested against at least:
    - The bulk-data-server test server (https://github.com/smart-on-fhir/bulk-data-server)
    - Cerner (https://www.cerner.com/)
    - Epic (https://www.epic.com/)

    TODO: make it more robust against server flakiness (like random http errors during file
     download or status checks). At least for intermittent issues (i.e. we should do some
     retrying). Actual server errors we should continue to surface, if they persist after retries.
    """

    _TIMEOUT_THRESHOLD = 60 * 60 * 24  # a day, which is probably an overly generous timeout

    def __init__(
        self,
        client: fhir.FhirClient,
        resources: list[str],
        url: str,
        destination: str,
        since: str | None = None,
        until: str | None = None,
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
        :param prefer_url_resources: if the URL includes _type, ignore the provided resources
        """
        super().__init__()
        self._client = client
        self._destination = destination
        self._total_wait_time = 0  # in seconds, across all our requests
        self._log: export_log.BulkExportLogWriter = None

        self._url = self.format_kickoff_url(
            url,
            resources=resources,
            since=since,
            until=until,
            prefer_url_resources=prefer_url_resources,
        )

        # Public properties, to be read after the export:
        self.export_datetime = None
        self.group_name = fhir.parse_group_from_url(self._url)

    def format_kickoff_url(
        self,
        url: str,
        *,
        resources: list[str],
        since: str | None,
        until: str | None,
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
        if since:
            query["_since"] = since
        if until:
            # This parameter is not part of the FHIR spec and is unlikely to be supported by your
            # server. But some custom servers *do* support it, so we added support for it too.
            query["_until"] = until
        parsed = parsed._replace(query=urllib.parse.urlencode(query, doseq=True))

        return urllib.parse.urlunsplit(parsed)

    async def export(self) -> None:
        """
        Bulk export resources from a FHIR server into local ndjson files.

        This call will block for a while, as resources tend to be large, and we may also have to wait if the server is
        busy. Because it is a slow operation, this function will also print status updates to the console.

        After this completes, the destination folder will be full of files that look like Resource.000.ndjson, like so:

        destination/
          Encounter.000.ndjson
          Encounter.001.ndjson
          Patient.000.ndjson
          log.ndjson

        See http://hl7.org/fhir/uv/bulkdata/export/index.html for details.
        """
        # Initiate bulk export
        print("Starting bulk FHIR export…")
        self._log = export_log.BulkExportLogWriter(store.Root(self._destination))

        try:
            response = await self._request_with_delay(
                self._url,
                headers={"Prefer": "respond-async"},
                target_status_code=202,
            )
        except Exception as exc:
            self._log.kickoff(self._url, self._client.get_capabilities(), exc)
            raise
        else:
            self._log.kickoff(self._url, self._client.get_capabilities(), response)

        # Grab the poll location URL for status updates
        poll_location = response.headers["Content-Location"]

        # Request status report, until export is done
        response = await self._request_with_logging(
            poll_location,
            headers={"Accept": "application/json"},
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

        # Were there any server-side errors during the export?
        # The spec acknowledges that "error" is perhaps misleading for an array that can contain info messages.
        error_texts, warning_texts = await self._gather_all_messages(response_json.get("error", []))
        if warning_texts:
            print("\n - ".join(["Messages from server:", *warning_texts]))

        # Download all the files
        print("Bulk FHIR export finished, now downloading resources…")
        files = response_json.get("output", [])
        await self._download_all_ndjson_files(files)

        self._log.export_complete()

        # If we raised an error in the above code, we intentionally will not reach this DELETE
        # call. If we had an issue talking to the server (like http errors), we want to leave the
        # files up there, so the user could try to manually recover.
        await self._delete_export(poll_location)

        # Make sure we're fully done before we bail because the server told us the export has issues.
        # We still want to DELETE the export in this case. And we still want to download all the files
        # the server DID give us. Servers may have lots of ignorable errors that need human review,
        # before passing back to us as input ndjson.
        if error_texts:
            raise errors.FatalError("\n - ".join(["Errors occurred during export:", *error_texts]))

    ###################################################################################################################
    #
    # Helpers
    #
    ###################################################################################################################

    async def _delete_export(self, poll_url: str) -> None:
        """As a kindness, send a DELETE to the polling location. Then the server knows it can delete the files."""
        try:
            await self._request_with_delay(poll_url, method="DELETE", target_status_code=202)
        except errors.FatalError:
            # Ignore any fatal issue with this, since we don't actually need this to succeed
            pass

    async def _request_with_delay(
        self,
        path: str,
        headers: dict | None = None,
        target_status_code: int = 200,
        method: str = "GET",
        log_progress: Callable[[httpx.Response], None] | None = None,
    ) -> httpx.Response:
        """
        Requests a file, while respecting any requests to wait longer.

        :param path: path to request
        :param headers: headers for request
        :param target_status_code: retries until this status code is returned
        :param method: HTTP method to request
        :returns: the HTTP response
        """
        status_box = rich.text.Text()
        with rich.get_console().status(status_box) as status:
            while self._total_wait_time < self._TIMEOUT_THRESHOLD:
                response = await self._client.request(method, path, headers=headers)

                if response.status_code == target_status_code:
                    if status_box.plain:
                        status.stop()
                        print(
                            f"  Waited for a total of {common.human_time_offset(self._total_wait_time)}"
                        )
                    return response

                # 202 == server is still working on it, 429 == server is busy -- in both cases, we wait
                if response.status_code in [202, 429]:
                    if log_progress:
                        log_progress(response)

                    # Print a message to the user, so they don't see us do nothing for a while
                    delay = int(response.headers.get("Retry-After", 60))
                    if response.status_code == 202:
                        # Some servers can request unreasonably long delays (e.g. I've seen Cerner ask for five hours),
                        # which is... not helpful for our UX and often way too long for small exports.
                        # So as long as the server isn't telling us it's overloaded, limit the delay time to 5 minutes.
                        delay = min(delay, 300)
                    progress_msg = response.headers.get("X-Progress", "waiting…")
                    formatted_total = common.human_time_offset(self._total_wait_time)
                    formatted_delay = common.human_time_offset(delay)
                    status_box.plain = f"{progress_msg} ({formatted_total} so far, waiting for {formatted_delay} more)"

                    # And wait as long as the server requests
                    await asyncio.sleep(delay)
                    self._total_wait_time += delay

                else:
                    # It feels silly to abort on an unknown *success* code, but the spec has such clear guidance on
                    # what the expected response codes are, that it's not clear if a code outside those parameters means
                    # we should keep waiting or stop waiting. So let's be strict here for now.
                    raise errors.NetworkError(
                        f"Unexpected status code {response.status_code} from the bulk FHIR export server.",
                        response,
                    )

        raise errors.FatalError("Timed out waiting for the bulk FHIR export to finish.")

    async def _request_with_logging(
        self,
        *args,
        log_begin: Callable[[], None] | None = None,
        log_error: Callable[[Exception], None] | None = None,
        **kwargs,
    ) -> httpx.Response:
        if log_begin:
            log_begin()

        try:
            return await self._request_with_delay(*args, **kwargs)
        except Exception as exc:
            if log_error:
                log_error(exc)
            raise

    async def _gather_all_messages(self, error_list: list[dict]) -> (list[str], list[str]):
        """
        Downloads all outcome message ndjson files from the bulk export server.

        :param error_list: info about each error file from the bulk FHIR server
        :returns: (error messages, non-fatal messages)
        """
        coroutines = []
        for error in error_list:
            # per spec as of writing, OperationOutcome is the only allowed type
            if error.get("type") == "OperationOutcome":
                coroutines.append(
                    self._request_with_logging(
                        error["url"],
                        headers={"Accept": "application/fhir+ndjson"},
                        log_begin=partial(
                            self._log.download_request,
                            error["url"],
                            "error",
                            error["type"],
                        ),
                        log_error=partial(self._log.download_error, error["url"]),
                    ),
                )
        responses = await asyncio.gather(*coroutines)

        fatal_messages = []
        info_messages = []
        for response in responses:
            # Create a list of OperationOutcomes
            outcomes = [json.loads(x) for x in response.text.split("\n") if x]
            self._log.download_complete(response.url, len(outcomes), len(response.text))
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

    async def _download_all_ndjson_files(self, files: list[dict]) -> None:
        """
        Downloads all exported ndjson files from the bulk export server.

        :param files: info about each file from the bulk FHIR server
        """
        resource_counts = {}  # how many of each resource we've seen
        coroutines = []
        for file in files:
            count = resource_counts.get(file["type"], -1) + 1
            resource_counts[file["type"]] = count
            filename = f'{file["type"]}.{count:03}.ndjson'
            coroutines.append(
                self._download_ndjson_file(
                    file["url"],
                    file["type"],
                    os.path.join(self._destination, filename),
                ),
            )
        await asyncio.gather(*coroutines)

    async def _download_ndjson_file(self, url: str, resource_type: str, filename: str) -> None:
        """
        Downloads a single ndjson file from the bulk export server.

        :param url: URL location of file to download
        :param resource_type: the resource type of the file
        :param filename: local path to write data to
        """

        self._log.download_request(url, "output", resource_type)
        decompressed_size = 0

        try:
            response = await self._client.request(
                "GET",
                url,
                headers={"Accept": "application/fhir+ndjson"},
                stream=True,
            )
            try:
                with open(filename, "w", encoding="utf8") as file:
                    async for block in response.aiter_text():
                        file.write(block)
                        decompressed_size += len(block)
            finally:
                await response.aclose()
        except Exception as exc:
            self._log.download_error(url, exc)
            raise

        lines = common.read_local_line_count(filename)
        self._log.download_complete(url, lines, decompressed_size)

        url_last_part = url.split("/")[-1]
        filename_last_part = filename.split("/")[-1]
        human_size = common.human_file_size(response.num_bytes_downloaded)
        print(f"  Downloaded {url_last_part} as {filename_last_part} ({human_size})")
