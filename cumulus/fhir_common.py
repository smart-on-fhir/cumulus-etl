"""FHIR utility methods"""

import re

# A relative reference is something like Patient/123 or Patient?identifier=http://hl7.org/fhir/sid/us-npi|9999999299
# (vs a contained reference that starts with # or an absolute URL reference like http://example.org/Patient/123)
RELATIVE_REFERENCE_REGEX = re.compile("[A-Za-z]+[/?].+")
RELATIVE_SEPARATOR_REGEX = re.compile("[/?]")


###############################################################################
# Standard FHIR References are ResourceType/id
###############################################################################


def ref_resource(resource_type: str, resource_id: str) -> dict:
    """
    Reference the FHIR proper way
    :param resource_type: Name of resource, like "Patient"
    :param resource_id: ID for resource (isa REF can be UUID)
    :return: FHIRReference as Resource/$id
    """
    if not resource_id:
        raise ValueError("Missing resource ID")
    return {"reference": f"{resource_type}/{resource_id}"}


def unref_resource(ref: dict) -> (str, str):
    """
    Returns the type & ID for the target of the reference

    Examples:
    - reference=Patient/ABC -> (Patient, ABC)
    - reference=ABC, type=Patient -> (Patient, ABC)

    Raises ValueError if the reference could not be understood
    """
    # FIXME: Support contained resources like '#p1' and absolute resources like
    #        http://fhir.hl7.org/svc/StructureDefinition/c8973a22-2b5b-4e76-9c66-00639c99e61b
    if not ref or not ref.get("reference") or ref["reference"].startswith("#"):
        raise ValueError(f'Reference type not handled: "{ref}"')

    if not RELATIVE_REFERENCE_REGEX.match(ref["reference"]) and not ref.get("type"):
        raise ValueError(f'Unrecognized reference: "{ref["reference"]}"')

    tokens = RELATIVE_SEPARATOR_REGEX.split(ref["reference"], maxsplit=1)
    if len(tokens) > 1:
        return tokens[0], tokens[1]

    if not ref.get("type"):
        raise ValueError(f'Reference does not have a type: "{ref["reference"]}"')
    return ref.get("type"), tokens[0]
