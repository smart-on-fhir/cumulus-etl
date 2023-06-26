"""Support for FHIR bulk exports"""

import asyncio
import json
import os
import urllib.parse

import httpx

from cumulus_etl import common, errors, fhir


class BulkExporter:
    """
    Perform a bulk export from a FHIR server that supports the Backend Service SMART profile.

    This has been manually tested against at least:
    - The bulk-data-server test server (https://github.com/smart-on-fhir/bulk-data-server)
    - Cerner (https://www.cerner.com/)
    - Epic (https://www.epic.com/)
    """

    _TIMEOUT_THRESHOLD = 60 * 60 * 24  # a day, which is probably an overly generous timeout

    def __init__(
        self,
        client: fhir.FhirClient,
        resources: list[str],
        url: str,
        destination: str,
        since: str = None,
        until: str = None,
    ):
        """
        Initialize a bulk exporter (but does not start an export).

        :param client: a client ready to make requests
        :param resources: a list of resource names to export
        :param url: a target export URL (like https://example.com/Group/1234)
        :param destination: a local folder to store all the files
        :param since: start date for export
        :param until: end date for export
        """
        super().__init__()
        self._client = client
        self._resources = resources
        self._url = url
        if not self._url.endswith("/"):
            self._url += "/"  # This will ensure the last segment does not get chopped off by urljoin
        self._destination = destination
        self._total_wait_time = 0  # in seconds, across all our requests
        self._since = since
        self._until = until

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

        See http://hl7.org/fhir/uv/bulkdata/export/index.html for details.
        """
        # Initiate bulk export
        common.print_header("Starting bulk FHIR export...")

        params = {"_type": ",".join(self._resources)}
        if self._since:
            params["_since"] = self._since
        if self._until:
            # This parameter is not part of the FHIR spec and is unlikely to be supported by your server.
            # But some servers do support it, and it is a possible future addition to the spec.
            params["_until"] = self._until

        response = await self._request_with_delay(
            urllib.parse.urljoin(self._url, f"$export?{urllib.parse.urlencode(params)}"),
            headers={"Prefer": "respond-async"},
            target_status_code=202,
        )

        # Grab the poll location URL for status updates
        poll_location = response.headers["Content-Location"]

        try:
            # Request status report, until export is done
            response = await self._request_with_delay(poll_location, headers={"Accept": "application/json"})

            # Finished! We're done waiting and can download all the files
            response_json = response.json()

            # Were there any server-side errors during the export?
            # The spec acknowledges that "error" is perhaps misleading for an array that can contain info messages.
            error_texts, warning_texts = await self._gather_all_messages(response_json.get("error", []))
            if error_texts:
                raise errors.FatalError("\n - ".join(["Errors occurred during export:"] + error_texts))
            if warning_texts:
                print("\n - ".join(["Messages from server:"] + warning_texts))

            # Download all the files
            print("Bulk FHIR export finished, now downloading resources...")
            files = response_json.get("output", [])
            await self._download_all_ndjson_files(files)
        finally:
            await self._delete_export(poll_location)

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
        self, path: str, headers: dict = None, target_status_code: int = 200, method: str = "GET"
    ) -> httpx.Response:
        """
        Requests a file, while respecting any requests to wait longer.

        :param path: path to request
        :param headers: headers for request
        :param target_status_code: retries until this status code is returned
        :param method: HTTP method to request
        :returns: the HTTP response
        """
        while self._total_wait_time < self._TIMEOUT_THRESHOLD:
            response = await self._client.request(method, path, headers=headers)

            if response.status_code == target_status_code:
                return response

            # 202 == server is still working on it, 429 == server is busy -- in both cases, we wait
            if response.status_code in [202, 429]:
                # Print a message to the user, so they don't see us do nothing for a while
                delay = int(response.headers.get("Retry-After", 60))
                if response.status_code == 202:
                    # Some servers can request unreasonably long delays (e.g. I've seen Cerner ask for five hours),
                    # which is... not helpful for our UX and often way too long for small exports.
                    # So as long as the server isn't telling us it's overloaded, limit the delay time to five minutes.
                    delay = min(delay, 300)
                progress_msg = response.headers.get("X-Progress", "waiting...")
                formatted_total = common.human_time_offset(self._total_wait_time)
                formatted_delay = common.human_time_offset(delay)
                print(f"  {progress_msg} ({formatted_total} so far, waiting for {formatted_delay} more)")

                # And wait as long as the server requests
                await asyncio.sleep(delay)
                self._total_wait_time += delay

            else:
                # It feels silly to abort on an unknown *success* code, but the spec has such clear guidance on
                # what the expected response codes are, that it's not clear if a code outside those parameters means
                # we should keep waiting or stop waiting. So let's be strict here for now.
                raise errors.FatalError(
                    f"Unexpected status code {response.status_code} from the bulk FHIR export server."
                )

        raise errors.FatalError("Timed out waiting for the bulk FHIR export to finish.")

    async def _gather_all_messages(self, error_list: list[dict]) -> (list[str], list[str]):
        """
        Downloads all outcome message ndjson files from the bulk export server.

        :param error_list: info about each error file from the bulk FHIR server
        :returns: (error messages, non-fatal messages)
        """
        coroutines = []
        for error in error_list:
            if error.get("type") == "OperationOutcome":  # per spec as of writing, the only allowed type
                coroutines.append(self._request_with_delay(error["url"], headers={"Accept": "application/fhir+ndjson"}))
        responses = await asyncio.gather(*coroutines)

        fatal_messages = []
        info_messages = []
        for response in responses:
            outcomes = (json.loads(x) for x in response.text.split("\n") if x)  # a list of OperationOutcomes
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
            coroutines.append(self._download_ndjson_file(file["url"], os.path.join(self._destination, filename)))
        await asyncio.gather(*coroutines)

    async def _download_ndjson_file(self, url: str, filename: str) -> None:
        """
        Downloads a single ndjson file from the bulk export server.

        :param url: URL location of file to download
        :param filename: local path to write data to
        """
        response = await self._client.request("GET", url, headers={"Accept": "application/fhir+ndjson"}, stream=True)
        try:
            with open(filename, "w", encoding="utf8") as file:
                async for block in response.aiter_text():
                    file.write(block)
        finally:
            await response.aclose()

        url_last_part = url.split("/")[-1]
        filename_last_part = filename.split("/")[-1]
        human_size = common.human_file_size(response.num_bytes_downloaded)
        print(f"  Downloaded {url_last_part} as {filename_last_part} ({human_size})")
