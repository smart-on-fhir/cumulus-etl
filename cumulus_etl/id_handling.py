from collections.abc import Callable, Collection

from cumulus_etl import common


def get_ids_from_csv(path: str, resource_type: str, is_anon: bool = False) -> set[str]:
    with common.read_csv(path) as reader:
        get_value = _find_header(reader.fieldnames, resource_type, is_anon=is_anon)
        if get_value is None:
            return set()
        return set(filter(None, (get_value(row) for row in iter(reader))))


def _find_header(
    fieldnames: Collection[str], resource_type: str, is_anon: bool = False
) -> Callable[[str], str] | None:
    """Returns a field name and a mutator to go from that field value to a plain ID"""
    folded = resource_type.casefold()
    id_names = [f"{folded}_id"]
    ref_names = [f"{folded}_ref"]

    if resource_type in {"DiagnosticReport", "DocumentReference"}:
        ref_names.append("document_ref")
        ref_names.append("note_ref")
    if resource_type == "DocumentReference":
        id_names.append("docref_id")
    if resource_type == "Patient":
        id_names.append("subject_id")
        ref_names.append("subject_ref")

    if is_anon:
        # Look for both anon_ and normal versions, but prefer an explicit column in case both exist
        id_names = [f"anon_{x}" for x in id_names] + id_names
        ref_names = [f"anon_{x}" for x in ref_names] + ref_names

    for field in id_names:
        if field in fieldnames:
            return lambda x: x[field]
    for field in ref_names:
        if field in fieldnames:
            prefix = f"{resource_type}/"
            return lambda x: x[field].removeprefix(prefix) if x[field].startswith(prefix) else None

    return None
