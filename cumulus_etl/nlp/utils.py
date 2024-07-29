"""Misc NLP functions"""

import hashlib
import os
from collections.abc import Callable

from cumulus_etl import common, store


def is_docref_valid(docref: dict) -> bool:
    """Returns True if this docref is not a draft or entered-in-error resource and could be considered for NLP"""
    good_status = docref.get("status") in {"current", None}  # status of DocRef itself
    # docStatus is status of clinical note attachments
    good_doc_status = docref.get("docStatus") in {"final", "amended", None}
    return good_status and good_doc_status


async def cache_wrapper(
    cache_dir: str, namespace: str, content: str, method: Callable, *args, **kwargs
) -> str:
    """Looks up an NLP result in the cache first, falling back to actually calling NLP."""
    # First, what is our target path for a possible cache file
    cache_dir = store.Root(cache_dir, create=True)
    checksum = hashlib.sha256(content.encode("utf8")).hexdigest()
    path = f"ctakes-cache/{namespace}/{checksum[0:4]}/sha256-{checksum}.json"  # "ctakes-cache" is historical
    cache_filename = cache_dir.joinpath(path)

    # And try to read that file, falling back to calling the given method if a cache is not available
    try:
        result = common.read_text(cache_filename)
    except (FileNotFoundError, PermissionError):
        result = await method(*args, **kwargs)
        cache_dir.makedirs(os.path.dirname(cache_filename))
        common.write_text(cache_filename, result)

    return result
