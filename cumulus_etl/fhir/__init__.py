"""Support for talking to FHIR servers & handling the FHIR spec"""

from .fhir_client import FhirClient, create_fhir_client_for_cli
from .fhir_utils import (
    download_reference,
    get_docref_note,
    parse_datetime,
    parse_group_from_url,
    ref_resource,
    unref_resource,
)
