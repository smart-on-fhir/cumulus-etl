"""Selection & filtering of input ndjson files, by resource ID"""

import cumulus_fhir_support as cfs

from cumulus_etl import cli_utils, common


def select_resources_from_files(
    root_input: cfs.FsPath,
    note_filter: cfs.NoteFilter,
    export_to: cfs.FsPath | None = None,
) -> common.Directory:
    """Takes an input folder of ndjson and exports just the chosen ones to a new ndjson folder"""
    # Set up export folder
    output_folder = cli_utils.make_export_dir(export_to=export_to)

    _process_one_resource(root_input, output_folder.name, "DiagnosticReport", note_filter)
    _process_one_resource(root_input, output_folder.name, "DocumentReference", note_filter)

    return output_folder


def _process_one_resource(
    root_input: cfs.FsPath,
    output_folder: str,
    resource_type: str,
    resource_filter: cfs.NoteFilter,
) -> None:
    output_file_path = cfs.FsPath(output_folder, f"{resource_type}.ndjson")

    # Read all input documents, filtering along the way
    with common.NdjsonWriter(output_file_path) as output_file:
        resources = common.read_resource_ndjson(root_input, resource_type, warn_if_empty=True)
        for resource in resources:
            if resource_filter(resource):
                output_file.write(resource)
