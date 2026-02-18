"""Support for talking to FHIR servers & handling the FHIR spec"""

from .fhir_utils import (
    FhirUrl,
    RemoteAttachment,
    get_clinical_note,
    get_clinical_note_attachment,
    get_clinical_note_role_info,
    get_concept_user_text,
    linked_resources,
    parse_content_type,
    parse_datetime,
    ref_resource,
    unref_resource,
)
