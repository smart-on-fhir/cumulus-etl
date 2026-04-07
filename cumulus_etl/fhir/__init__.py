"""Support for talking to FHIR servers & handling the FHIR spec"""

from .fhir_utils import (
    FhirUrl,
    get_clinical_note_role_info,
    get_concept_user_text,
    parse_datetime,
    ref_resource,
    unref_resource,
)
