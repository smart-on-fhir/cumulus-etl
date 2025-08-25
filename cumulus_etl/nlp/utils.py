"""Misc NLP functions"""

import hashlib
import os
from collections.abc import Callable
from typing import TypeVar

from cumulus_etl import common, deid, fhir, store

Obj = TypeVar("Obj")


async def is_note_valid(codebook: deid.Codebook, note: dict) -> bool:
    """
    Returns True if this note is not a draft or entered-in-error resource

    i.e. if it's a good candidate for NLP
    """
    del codebook  # only passed in to look like a "resource_filter" callback

    match note["resourceType"]:
        case "DiagnosticReport":
            valid_status_types = {"final", "amended", "corrected", "appended", "unknown", None}
            return note.get("status") in valid_status_types

        case "DocumentReference":
            good_status = note.get("status") in {"current", None}  # status of DocRef itself
            # docStatus is status of clinical note attachments
            good_doc_status = note.get("docStatus") in {"final", "amended", None}
            return good_status and good_doc_status

        case _:  # pragma: no cover
            return False  # pragma: no cover


def get_note_info(note: dict) -> tuple[str, str, str]:
    """
    Returns note_ref, encounter_id, subject_id for the given DocRef/DxReport.

    Raises KeyError if any of them aren't present.
    """
    note_ref = f"{note['resourceType']}/{note['id']}"
    encounters = note.get("context", {}).get("encounter", [])
    if not encounters:  # check for dxreport encounter field
        encounters = [note["encounter"]] if "encounter" in note else []
    if not encounters:
        raise KeyError(f"No encounters for note {note_ref}")
    _, encounter_id = fhir.unref_resource(encounters[0])
    _, subject_id = fhir.unref_resource(note["subject"])
    return note_ref, encounter_id, subject_id


async def cache_wrapper(
    cache_dir: str,
    namespace: str,
    content: str,
    from_file: Callable[[str], Obj],
    to_file: Callable[[Obj], str],
    method: Callable,
    *args,
    **kwargs,
) -> Obj:
    """Looks up an NLP result in the cache first, falling back to actually calling NLP."""
    # First, what is our target path for a possible cache file
    cache_dir = store.Root(cache_dir, create=True)
    checksum = hashlib.sha256(content.encode("utf8")).hexdigest()
    path = f"nlp-cache/{namespace}/{checksum[0:4]}/sha256-{checksum}.cache"
    cache_filename = cache_dir.joinpath(path)

    # And try to read that file, falling back to calling the given method if a cache is not available
    try:
        result = from_file(common.read_text(cache_filename))
    except (FileNotFoundError, PermissionError):
        result = await method(*args, **kwargs)
        cache_dir.makedirs(os.path.dirname(cache_filename))
        common.write_text(cache_filename, to_file(result))

    return result
