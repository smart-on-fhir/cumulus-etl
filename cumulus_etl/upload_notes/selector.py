"""Selection & filtering of input ndjson files, by docref ID"""

import functools
import os
from collections.abc import Callable, Iterable, Iterator

from cumulus_etl import cli_utils, common, deid, store


def select_docrefs_from_files(
    root_input: store.Root,
    codebook: deid.Codebook,
    docrefs: str | None = None,
    anon_docrefs: str | None = None,
    export_to: str | None = None,
) -> common.Directory:
    """Takes an input folder of ndjson and exports just the chosen docrefs to a new ndjson folder"""
    # Get an appropriate filter method, for the given docrefs
    docref_filter = _create_docref_filter(codebook, docrefs, anon_docrefs)

    # Set up export folder
    output_folder = cli_utils.make_export_dir(export_to=export_to)
    output_file_path = os.path.join(output_folder.name, "DocumentReference.ndjson")

    # Read all input documents, filtering along the way
    with common.NdjsonWriter(output_file_path) as output_file:
        resources = common.read_resource_ndjson(root_input, "DocumentReference", warn_if_empty=True)
        for docref in docref_filter(resources):
            output_file.write(docref)

    return output_folder


def _create_docref_filter(
    codebook: deid.Codebook, docrefs: str | None = None, anon_docrefs: str | None = None
) -> Callable[[Iterable[dict]], Iterator[dict]]:
    """This returns a method that will can an iterator of docrefs and returns an iterator of fewer docrefs"""
    # Decide how we're filtering the input files (by real or fake ID, or no filtering at all!)
    if docrefs:
        return functools.partial(_filter_real_docrefs, docrefs)
    elif anon_docrefs:
        return functools.partial(_filter_fake_docrefs, codebook, anon_docrefs)
    else:
        # Just accept everything (we still want to read them though, to copy them to a possible export folder).
        # So this lambda just returns an iterator over its input.
        return lambda x: iter(x)


def _filter_real_docrefs(docrefs_csv: str, docrefs: Iterable[dict]) -> Iterator[dict]:
    """Keeps any docrefs that match the csv list"""
    with common.read_csv(docrefs_csv) as reader:
        real_docref_ids = {row["docref_id"] for row in reader}

    for docref in docrefs:
        if docref["id"] in real_docref_ids:
            yield docref

            real_docref_ids.remove(docref["id"])
            if not real_docref_ids:
                break


def _filter_fake_docrefs(
    codebook: deid.Codebook, anon_docrefs_csv: str, docrefs: Iterable[dict]
) -> Iterator[dict]:
    """Calculates the fake ID for all docrefs found, and keeps any that match the csv list"""
    with common.read_csv(anon_docrefs_csv) as reader:
        # ignore the patient_id column, not needed
        fake_docref_ids = {row["docref_id"] for row in reader}

    for docref in docrefs:
        fake_id = codebook.fake_id("DocumentReference", docref["id"], caching_allowed=False)
        if fake_id in fake_docref_ids:
            yield docref

            fake_docref_ids.remove(fake_id)
            if not fake_docref_ids:
                break
