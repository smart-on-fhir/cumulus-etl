"""Download just the docrefs we need for a chart review."""

import asyncio
import itertools
import logging
import os
import urllib.parse
from collections.abc import Container, Iterable

from cumulus_etl import cli_utils, common, deid, errors, fhir, loaders, store


async def download_docrefs_from_fhir_server(
    client: fhir.FhirClient,
    root_input: store.Root,
    codebook: deid.Codebook,
    docrefs: str | None = None,
    anon_docrefs: str | None = None,
    export_to: str | None = None,
):
    if docrefs:
        return await _download_docrefs_from_real_ids(client, docrefs, export_to=export_to)
    elif anon_docrefs:
        return await _download_docrefs_from_fake_ids(
            client, codebook, anon_docrefs, export_to=export_to
        )
    else:
        # else we'll download the entire target path as a bulk export (presumably the user has scoped a Group)
        ndjson_loader = loaders.FhirNdjsonLoader(root_input, client, export_to=export_to)
        return await ndjson_loader.load_all(["DocumentReference"])


async def _download_docrefs_from_fake_ids(
    client: fhir.FhirClient,
    codebook: deid.Codebook,
    docref_csv: str,
    export_to: str | None = None,
) -> common.Directory:
    """Download DocumentReference resources for the given patient and docref identifiers"""
    output_folder = cli_utils.make_export_dir(export_to)

    # Grab identifiers for which specific docrefs & patients we need
    with common.read_csv(docref_csv) as reader:
        rows = list(reader)
        fake_docref_ids = {row["docref_id"] for row in rows}
        fake_patient_ids = {row["patient_id"] for row in rows}

    # We know how to reverse-map the patient identifiers, so do that up front
    patient_ids = codebook.real_ids("Patient", fake_patient_ids)

    # Kick off a bunch of requests to the FHIR server for any documents for these patients
    # (filtered to only the given fake IDs)
    coroutines = [
        _request_docrefs_for_patient(client, patient_id, codebook, fake_docref_ids)
        for patient_id in patient_ids
    ]
    docrefs_per_patient = await asyncio.gather(*coroutines)

    # And write them all out
    _write_docrefs_to_output_folder(
        itertools.chain.from_iterable(docrefs_per_patient), output_folder.name
    )
    return output_folder


async def _download_docrefs_from_real_ids(
    client: fhir.FhirClient,
    docref_csv: str,
    export_to: str | None = None,
) -> common.Directory:
    """Download DocumentReference resources for the given patient and docref identifiers"""
    output_folder = cli_utils.make_export_dir(export_to)

    # Grab identifiers for which specific docrefs we need
    with common.read_csv(docref_csv) as reader:
        docref_ids = sorted({row["docref_id"] for row in reader})

    # Kick off a bunch of requests to the FHIR server for these documents
    coroutines = [_request_docref(client, docref_id) for docref_id in docref_ids]
    docrefs = await asyncio.gather(*coroutines)
    docrefs = filter(None, docrefs)  # filter out the failing requests

    # And write them all out
    _write_docrefs_to_output_folder(docrefs, output_folder.name)
    return output_folder


def _write_docrefs_to_output_folder(docrefs: Iterable[dict], output_folder: str) -> None:
    # Figure out where to put these docrefs
    output_file_path = os.path.join(output_folder, "DocumentReference.ndjson")

    # Stitch the resulting documents together and return as one big iterator
    with common.NdjsonWriter(output_file_path) as output_file:
        for docref in docrefs:
            output_file.write(docref)


async def _request_docrefs_for_patient(
    client: fhir.FhirClient,
    patient_id: str,
    codebook: deid.Codebook,
    fake_docref_ids: Container[str],
) -> list[dict]:
    """Returns all DocumentReferences for a given patient"""
    params = {
        "patient": patient_id,
        "_elements": "content",  # doesn't seem widely supported? But harmless to *try* to restrict size of response
    }
    print(f"  Searching for all docrefs for patient {patient_id}.")
    response = await client.request("GET", f"DocumentReference?{urllib.parse.urlencode(params)}")
    bundle = response.json()

    docrefs = []
    for entry in bundle.get("entry", []):
        resource = entry["resource"]
        fake_id = codebook.fake_id("DocumentReference", resource["id"], caching_allowed=False)
        if fake_id in fake_docref_ids:
            print(f"  ⭐ Including docref {resource['id']} ⭐")
            docrefs.append(resource)
        else:
            print(f"  Ignoring docref {resource['id']}")
    return docrefs


async def _request_docref(client: fhir.FhirClient, docref_id: str) -> dict | None:
    """Returns one DocumentReference for a given ID"""
    print(f"  Downloading docref {docref_id}.")
    try:
        response = await client.request("GET", f"DocumentReference/{docref_id}")
        return response.json()
    except errors.FatalError:
        logging.warning("Error getting docref %s", docref_id)
        return None
