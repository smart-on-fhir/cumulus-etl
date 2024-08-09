"""Misc NLP functions"""

import hashlib
import os
from collections.abc import Callable
from typing import TypeVar

from cumulus_etl import common, fhir, store

Obj = TypeVar("Obj")


def is_docref_valid(docref: dict) -> bool:
    """Returns True if this docref is not a draft or entered-in-error resource and could be considered for NLP"""
    good_status = docref.get("status") in {"current", None}  # status of DocRef itself
    # docStatus is status of clinical note attachments
    good_doc_status = docref.get("docStatus") in {"final", "amended", None}
    return good_status and good_doc_status


def get_docref_info(docref: dict) -> (str, str, str):
    """
    Returns docref_id, encounter_id, subject_id for the given DocRef.

    Raises KeyError if any of them aren't present.
    """
    docref_id = docref["id"]
    encounters = docref.get("context", {}).get("encounter", [])
    if not encounters:
        raise KeyError(f"No encounters for docref {docref_id}")
    _, encounter_id = fhir.unref_resource(encounters[0])
    _, subject_id = fhir.unref_resource(docref["subject"])
    return docref_id, encounter_id, subject_id


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
