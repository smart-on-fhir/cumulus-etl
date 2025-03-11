"""Download just the docrefs we need for a chart review."""

import asyncio
import itertools
import logging
import os
import urllib.parse
from collections.abc import Container, Iterable

from cumulus_etl import cli_utils, common, deid, errors, fhir, loaders, store
from cumulus_etl.upload_notes.id_handling import get_ids_from_csv


async def download_resources_from_fhir_server(
    client: fhir.FhirClient,
    root_input: store.Root,
    codebook: deid.Codebook,
    id_file: str | None = None,
    anon_id_file: str | None = None,
    export_to: str | None = None,
):
    if id_file:
        return await _download_resources_from_real_ids(client, id_file, export_to=export_to)
    elif anon_id_file:
        return await _download_resources_from_fake_ids(
            client, codebook, anon_id_file, export_to=export_to
        )
    else:
        # else we'll download the entire target path as a bulk export (presumably the user has scoped a Group)
        ndjson_loader = loaders.FhirNdjsonLoader(root_input, client, export_to=export_to)
        return await ndjson_loader.load_resources({"DiagnosticReport", "DocumentReference"})


async def _download_resources_from_fake_ids(
    client: fhir.FhirClient,
    codebook: deid.Codebook,
    docref_csv: str,
    export_to: str | None = None,
) -> common.Directory:
    """Download DocumentReference resources for the given patient and docref identifiers"""
    output_folder = cli_utils.make_export_dir(export_to)

    # Grab identifiers for which specific docrefs & patients we need
    fake_dxreport_ids = get_ids_from_csv(docref_csv, "DiagnosticReport", is_anon=True)
    fake_docref_ids = get_ids_from_csv(docref_csv, "DocumentReference", is_anon=True)
    fake_patient_ids = get_ids_from_csv(docref_csv, "Patient", is_anon=True)

    # We know how to reverse-map the patient identifiers, so do that up front
    patient_ids = list(codebook.real_ids("Patient", fake_patient_ids))

    async def handle_resource(resource_type, fake_ids):
        if not fake_ids:
            return

        # Kick off a bunch of requests to the FHIR server for any documents for these patients
        # (filtered to only the given fake IDs)
        coroutines = [
            _request_resources_for_patient(client, patient_id, resource_type, codebook, fake_ids)
            for patient_id in patient_ids
        ]
        resources_per_patient = await asyncio.gather(*coroutines)

        # And write them all out
        _write_resources_to_output_folder(
            itertools.chain.from_iterable(resources_per_patient), resource_type, output_folder.name
        )

    await handle_resource("DiagnosticReport", fake_dxreport_ids)
    await handle_resource("DocumentReference", fake_docref_ids)

    return output_folder


async def _download_resources_from_real_ids(
    client: fhir.FhirClient,
    docref_csv: str,
    export_to: str | None = None,
) -> common.Directory:
    """Download DocumentReference resources for the given patient and docref identifiers"""
    output_folder = cli_utils.make_export_dir(export_to)

    # Grab identifiers for which specific docrefs we need
    dxreport_ids = get_ids_from_csv(docref_csv, "DiagnosticReport")
    docref_ids = get_ids_from_csv(docref_csv, "DocumentReference")

    async def handle_resource(resource_type: str, id_list: set[str]):
        if not id_list:
            return

        # Kick off a bunch of requests to the FHIR server for these documents
        coroutines = [
            _request_resource(client, resource_type, resource_id) for resource_id in id_list
        ]
        docrefs = await asyncio.gather(*coroutines)
        docrefs = filter(None, docrefs)  # filter out the failing requests

        # And write them all out
        _write_resources_to_output_folder(docrefs, resource_type, output_folder.name)

    await handle_resource("DiagnosticReport", dxreport_ids)
    await handle_resource("DocumentReference", docref_ids)

    return output_folder


def _write_resources_to_output_folder(
    resources: Iterable[dict], resource_type: str, output_folder: str
) -> None:
    # Figure out where to put these resources
    output_file_path = os.path.join(output_folder, f"{resource_type}.ndjson")

    # Stitch the resulting documents together and return as one big iterator
    with common.NdjsonWriter(output_file_path) as output_file:
        for resource in resources:
            output_file.write(resource)


async def _request_resources_for_patient(
    client: fhir.FhirClient,
    patient_id: str,
    resource_type: str,
    codebook: deid.Codebook,
    fake_ids: Container[str],
) -> list[dict]:
    """Returns all resources for a given patient"""
    params = {
        "patient": patient_id,
        "_elements": "content",  # doesn't seem widely supported? But harmless to *try* to restrict size of response
    }
    print(f"  Searching for all {resource_type} resources for patient {patient_id}.")
    response = await client.request("GET", f"{resource_type}?{urllib.parse.urlencode(params)}")
    bundle = response.json()

    results = []
    for entry in bundle.get("entry", []):
        resource = entry["resource"]
        fake_id = codebook.fake_id(resource_type, resource["id"], caching_allowed=False)
        if fake_id in fake_ids:
            print(f"  ⭐ Including {resource_type}/{resource['id']} ⭐")
            results.append(resource)
        else:
            print(f"  Ignoring {resource_type}/{resource['id']}")
    return results


async def _request_resource(
    client: fhir.FhirClient, resource_type: str, resource_id: str
) -> dict | None:
    """Returns one DocumentReference for a given ID"""
    print(f"  Downloading {resource_type}/{resource_id}.")
    try:
        response = await client.request("GET", f"{resource_type}/{resource_id}")
        return response.json()
    except errors.FatalError:
        logging.warning("Error getting %s/%s", resource_type, resource_id)
        return None
