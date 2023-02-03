"""Support for FHIR bulk exports"""

import os
import time
import urllib.parse
from typing import List

import requests

from cumulus import common
from cumulus.loaders.fhir.backend_service import BackendServiceServer, FatalError


class BulkExporter:
    """
    Perform a bulk export from a FHIR server that supports the Backend Service SMART profile.

    This has been manually tested against:
    - The bulk-data-server test server (https://github.com/smart-on-fhir/bulk-data-server)
    - Cerner Millennium (https://www.cerner.com/)
    """

    _TIMEOUT_THRESHOLD = 60 * 60 * 24  # a day, which is probably an overly generous timeout

    def __init__(
        self, server: BackendServiceServer, resources: List[str], destination: str, since: str = None, until: str = None
    ):
        """
        Initialize a bulk exporter (but does not start an export).

        :param server: a server instance ready to make requests
        :param resources: a list of resource names to export
        :param destination: a local folder to store all the files
        :param since: start date for export
        :param until: end date for export
        """
        super().__init__()
        self._server = server
        self._resources = resources
        self._destination = destination
        self._total_wait_time = 0  # in seconds, across all our requests
        self._since = since
        self._until = until

    def export(self) -> None:
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

        response = self._request_with_delay(
            f"$export?{urllib.parse.urlencode(params)}",
            headers={"Prefer": "respond-async"},
            target_status_code=202,
        )

        # Grab the poll location URL for status updates
        poll_location = response.headers["Content-Location"]

        try:
            # Request status report, until export is done
            response = self._request_with_delay(poll_location, headers={"Accept": "application/json"})

            # Finished! We're done waiting and can download all the files
            response_json = response.json()

            # Were there any server-side errors during the export?
            errors = response_json.get("error", [])
            if errors:
                message = "Errors occurred during export:"
                for error in errors:
                    if error.get("type") == "OperationOutcome":  # per spec as of writing, the only allowed type
                        outcome = self._request_with_delay(error["url"]).json()
                        message += f"\n - {outcome['issue'][0]['diagnostics']}"
                raise FatalError(message)

            # Download all the files
            print("Bulk FHIR export finished, now downloading resources...")
            files = response_json.get("output", [])
            self._download_all_ndjson_files(files)
        finally:
            self._delete_export(poll_location)

    ###################################################################################################################
    #
    # Helpers
    #
    ###################################################################################################################

    def _delete_export(self, poll_url: str) -> None:
        """As a kindness, send a DELETE to the polling location. Then the server knows it can delete the files."""
        try:
            self._request_with_delay(poll_url, method="DELETE", target_status_code=202)
        except FatalError:
            # Ignore any fatal issue with this, since we don't actually need this to succeed
            pass

    def _request_with_delay(
        self, path: str, headers: dict = None, target_status_code: int = 200, method: str = "GET"
    ) -> requests.Response:
        """
        Requests a file, while respecting any requests to wait longer.

        :param path: path to request
        :param headers: headers for request
        :param target_status_code: retries until this status code is returned
        :param method: HTTP method to request
        :returns: the HTTP response
        """
        while self._total_wait_time < self._TIMEOUT_THRESHOLD:
            response = self._server.request(method, path, headers=headers)

            if response.status_code == target_status_code:
                return response

            # 202 == server is still working on it, 429 == server is busy -- in both cases, we wait
            if response.status_code in [202, 429]:
                # Print a message to the user, so they don't see us do nothing for a while
                print(f'  {response.headers.get("X-Progress", "waiting...")} ({self._total_wait_time}s so far)')

                # And wait as long as the server requests
                delay = int(response.headers.get("Retry-After", 60))
                time.sleep(delay)
                self._total_wait_time += delay

            else:
                # It feels silly to abort on an unknown *success* code, but the spec has such clear guidance on
                # what the expected response codes are, that it's not clear if a code outside those parameters means
                # we should keep waiting or stop waiting. So let's be strict here for now.
                raise FatalError(f"Unexpected status code {response.status_code} from the bulk FHIR export server.")

        raise FatalError("Timed out waiting for the bulk FHIR export to finish.")

    def _download_all_ndjson_files(self, files: List[dict]) -> None:
        """
        Downloads all exported ndjson files from the bulk export server.

        :param files: info about each file from the bulk FHIR server
        """
        resource_counts = {}  # how many of each resource we've seen
        for file in files:
            count = resource_counts.get(file["type"], -1) + 1
            resource_counts[file["type"]] = count
            filename = f'{file["type"]}.{count:03}.ndjson'
            self._download_ndjson_file(file["url"], os.path.join(self._destination, filename))
            print(f"  Downloaded {filename}")

    def _download_ndjson_file(self, url: str, filename: str) -> None:
        """
        Downloads a single ndjson file from the bulk export server.

        :param url: URL location of file to download
        :param filename: local path to write data to
        """
        response = self._server.request("GET", url, headers={"Accept": "application/fhir+ndjson"}, stream=True)
        with open(filename, "w", encoding="utf8") as file:
            # Make sure iter_content() returns a string (rather than bytes) by enforcing an encoding
            response.encoding = response.encoding or "utf8"

            # Now actually iterate over the stream and write straight to disk
            for block in response.iter_content(chunk_size=None, decode_unicode=True):
                file.write(block)
