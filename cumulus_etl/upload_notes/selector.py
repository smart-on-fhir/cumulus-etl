"""Selection & filtering of input ndjson files, by resource ID"""

import functools
import os
from collections.abc import Callable, Collection, Iterable, Iterator

from cumulus_etl import cli_utils, common, deid, store


def select_resources_from_files(
    root_input: store.Root,
    codebook: deid.Codebook,
    id_file: str | None = None,
    anon_id_file: str | None = None,
    export_to: str | None = None,
) -> common.Directory:
    """Takes an input folder of ndjson and exports just the chosen ones to a new ndjson folder"""
    # Get an appropriate filter method, for the given id_file
    dxreport_filter = _create_resource_filter(codebook, "DiagnosticReport", id_file, anon_id_file)
    docref_filter = _create_resource_filter(codebook, "DocumentReference", id_file, anon_id_file)

    # Set up export folder
    output_folder = cli_utils.make_export_dir(export_to=export_to)

    _process_one_resource(root_input, output_folder.name, "DiagnosticReport", dxreport_filter)
    _process_one_resource(root_input, output_folder.name, "DocumentReference", docref_filter)

    return output_folder


def _process_one_resource(
    root_input: store.Root,
    output_folder: str,
    resource_type: str,
    resource_filter: Callable[[Iterable[dict]], Iterator[dict]],
) -> None:
    output_file_path = os.path.join(output_folder, f"{resource_type}.ndjson")

    # Read all input documents, filtering along the way
    with common.NdjsonWriter(output_file_path) as output_file:
        resources = common.read_resource_ndjson(root_input, resource_type, warn_if_empty=True)
        for resources in resource_filter(resources):
            output_file.write(resources)


def _create_resource_filter(
    codebook: deid.Codebook,
    resource_type: str,
    id_file: str | None,
    anon_id_file: str | None,
) -> Callable[[Iterable[dict]], Iterator[dict]]:
    """This returns a method that filters down an iterator of resources"""
    # Decide how we're filtering the input files (by real or fake ID, or no filtering at all!)
    if id_file:
        return functools.partial(_filter_real_ids, resource_type, id_file)
    elif anon_id_file:
        return functools.partial(_filter_fake_ids, codebook, resource_type, anon_id_file)
    else:
        # Just accept everything (we still want to read them though, to copy them to a possible export folder).
        # So this lambda just returns an iterator over its input.
        return lambda x: iter(x)


def _find_header(
    fieldnames: Collection[str], resource_type: str, is_anon: bool = False
) -> Callable[[str], str] | None:
    """Returns a field name and a mutator to go from that field value to a plain ID"""
    resource_type = resource_type.casefold()
    id_name = f"{resource_type}_id"
    ref_name = f"{resource_type}_ref"

    if is_anon:
        # Look for both anon_ and normal versions, but prefer an explicit column in case both exist
        id_names = [f"anon_{id_name}", id_name]
        ref_names = [f"anon_{ref_name}", ref_name]
    else:
        id_names = [id_name]
        ref_names = [ref_name]

    if resource_type == "DocumentReference":
        id_names = ["docref_id", *id_names]  # historical alias

    for field in id_names:
        if field in fieldnames:
            return lambda x: x[field]
    for field in ref_names:
        if field in fieldnames:
            return lambda x: x[field].removeprefix(f"{resource_type}/")

    print(f"Provided ID file does not have a column that provides a {resource_type} ID.")
    return None


def _filter_real_ids(resource_type: str, id_file: str, resources: Iterable[dict]) -> Iterator[dict]:
    """Keeps any resources that match the csv list"""
    with common.read_csv(id_file) as reader:
        get_value = _find_header(reader.fieldnames, resource_type)
        if get_value is None:
            return resources
        real_resource_ids = {get_value(row) for row in reader}

    for resource in resources:
        if resource["id"] in real_resource_ids:
            yield resource

            real_resource_ids.remove(resource["id"])
            if not real_resource_ids:
                break


def _filter_fake_ids(
    codebook: deid.Codebook, resource_type: str, anon_id_file: str, resources: Iterable[dict]
) -> Iterator[dict]:
    """Keeps any resources that match the anonymized csv list"""
    with common.read_csv(anon_id_file) as reader:
        get_value = _find_header(reader.fieldnames, resource_type, is_anon=True)
        if get_value is None:
            return resources
        fake_resource_ids = {get_value(row) for row in reader}

    for resource in resources:
        fake_id = codebook.fake_id(resource_type, resource["id"], caching_allowed=False)
        if fake_id in fake_resource_ids:
            yield resource

            fake_resource_ids.remove(fake_id)
            if not fake_resource_ids:
                break
