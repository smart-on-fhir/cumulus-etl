"""Support for talking to FHIR servers & handling the FHIR spec"""

from .fhir_client import create_fhir_client_for_cli
from .fhir_utils import (
    FhirUrl,
    download_reference,
    get_clinical_note,
    get_clinical_note_role_info,
    get_concept_user_text,
    linked_resources,
    parse_content_type,
    parse_datetime,
    ref_resource,
    request_attachment,
    unref_resource,
)
