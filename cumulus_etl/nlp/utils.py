"""Misc NLP functions"""

import datetime
import hashlib
from collections.abc import Callable
from typing import TypeVar

import cumulus_fhir_support as cfs

from cumulus_etl import fhir

Obj = TypeVar("Obj")


def get_note_info(note: dict) -> tuple[str, str, str]:
    """
    Returns note_ref, encounter_id, subject_ref for the given DocRef/DxReport.

    Raises KeyError if any of them aren't present.
    """
    note_ref = f"{note['resourceType']}/{note['id']}"
    encounter_id = get_note_encounter_id(note)
    if not encounter_id:
        raise KeyError(f"No encounters for note '{note_ref}'")
    subject_ref = get_note_subject_ref(note)
    if not subject_ref:
        raise KeyError(f"No subject for note '{note_ref}'")
    return note_ref, encounter_id, subject_ref


def get_note_encounter_id(note: dict) -> str | None:
    """Returns the encounter ID of a note"""
    encounters = note.get("context", {}).get("encounter", [])
    if not encounters:  # check for dxreport encounter field
        encounters = [note["encounter"]] if "encounter" in note else []
    if encounters:
        _, encounter_id = fhir.unref_resource(encounters[0])
        return encounter_id
    else:
        return None  # pragma: no cover


def get_note_subject_ref(note: dict) -> str | None:
    """Returns the subject ref of a note, suitable for cross-referencing across notes"""
    try:
        subject_type, subject_id = fhir.unref_resource(note.get("subject"))
    except ValueError:  # pragma: no cover
        return None

    if subject_type:
        return f"{subject_type}/{subject_id}"
    else:
        # avoids dealing with contained refs or other oddities that won't match across notes
        return None  # pragma: no cover


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
