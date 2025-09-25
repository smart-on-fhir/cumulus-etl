"""Misc NLP functions"""

import datetime
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
    Returns note_ref, encounter_id, subject_ref for the given DocRef/DxReport.

    Raises KeyError if any of them aren't present.
    """
    note_ref = f"{note['resourceType']}/{note['id']}"
    encounters = note.get("context", {}).get("encounter", [])
    if not encounters:  # check for dxreport encounter field
        encounters = [note["encounter"]] if "encounter" in note else []
    if not encounters:
        raise KeyError(f"No encounters for note '{note_ref}'")
    _, encounter_id = fhir.unref_resource(encounters[0])
    subject_ref = get_note_subject_ref(note)
    if not subject_ref:
        raise KeyError(f"No subject for note '{note_ref}'")
    return note_ref, encounter_id, subject_ref


def get_note_subject_ref(note: dict) -> str | None:
    """Returns the subject ref of a note, suitable for cross-referencing across notes"""
    try:
        subject_type, subject_id = fhir.unref_resource(note.get("subject"))
    except ValueError:
        return None

    if subject_type:
        return f"{subject_type}/{subject_id}"
    else:
        # avoids dealing with contained refs or other oddities that won't match across notes
        return None


def get_note_date(note: dict) -> datetime.datetime | None:
    """Returns the date of a note - preferring clinical dates, then administrative ones"""
    if note.get("resourceType") == "DiagnosticReport":
        if time := fhir.parse_datetime(note.get("effectiveDateTime")):
            return time
        if time := fhir.parse_datetime(note.get("effectivePeriod", {}).get("start")):
            return time
        if time := fhir.parse_datetime(note.get("issued")):
            return time
    elif note.get("resourceType") == "DocumentReference":
        if time := fhir.parse_datetime(note.get("context", {}).get("period", {}).get("start")):
            return time
        if time := fhir.parse_datetime(note.get("date")):
            return time
    return None


def _cache_metadata_filename(cache_dir: str, namespace: str, filename: str) -> str:
    cache_dir = store.Root(cache_dir, create=True)
    path = f"nlp-cache/{namespace}/{filename}"
    return cache_dir.joinpath(path)


def cache_metadata_write(cache_dir: str, namespace: str, content: dict) -> None:
    path = _cache_metadata_filename(cache_dir, namespace, "metadata.json")
    store.Root(os.path.dirname(path), create=True)
    common.write_json(path, content, indent=2)


def cache_metadata_read(cache_dir: str, namespace: str) -> dict:
    path = _cache_metadata_filename(cache_dir, namespace, "metadata.json")
    try:
        return common.read_json(path)
    except (FileNotFoundError, PermissionError):
        return {}


def _cache_filename(cache_dir: str, namespace: str, checksum: str) -> str:
    cache_dir = store.Root(cache_dir, create=True)
    path = f"nlp-cache/{namespace}/{checksum[0:4]}/sha256-{checksum}.cache"
    return cache_dir.joinpath(path)


def cache_checksum(note_text: str) -> str:
    return hashlib.sha256(note_text.encode("utf8"), usedforsecurity=False).hexdigest()


def cache_write(cache_dir: str, namespace: str, checksum: str, content: str) -> None:
    path = _cache_filename(cache_dir, namespace, checksum)
    store.Root(os.path.dirname(path), create=True)
    common.write_text(path, content)


def cache_read(cache_dir: str, namespace: str, checksum: str) -> str | None:
    path = _cache_filename(cache_dir, namespace, checksum)
    try:
        return common.read_text(path)
    except (FileNotFoundError, PermissionError):
        return None


async def cache_wrapper(
    cache_dir: str,
    namespace: str,
    checksum: str,
    from_file: Callable[[str], Obj],
    to_file: Callable[[Obj], str],
    method: Callable,
    *args,
    **kwargs,
) -> Obj:
    """Looks up an NLP result in the cache first, falling back to actually calling NLP."""
    result = cache_read(cache_dir, namespace, checksum)

    if result is None:
        result = await method(*args, **kwargs)
        cache_write(cache_dir, namespace, checksum, to_file(result))
    else:
        result = from_file(result)

    return result
