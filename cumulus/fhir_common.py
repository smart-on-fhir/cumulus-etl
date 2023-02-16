"""FHIR utility methods"""

import re
from typing import Optional

# A relative reference is something like Patient/123 or Patient?identifier=http://hl7.org/fhir/sid/us-npi|9999999299
# (vs a contained reference that starts with # or an absolute URL reference like http://example.org/Patient/123)
RELATIVE_REFERENCE_REGEX = re.compile("[A-Za-z]+[/?].+")
RELATIVE_SEPARATOR_REGEX = re.compile("[/?]")


###############################################################################
# Standard FHIR References are ResourceType/id
###############################################################################


def ref_resource(resource_type: Optional[str], resource_id: str) -> dict:
    """
    Reference the FHIR proper way
    :param resource_type: Name of resource, like "Patient"
    :param resource_id: ID for resource (isa REF can be UUID)
    :return: FHIRReference as Resource/$id
    """
    if not resource_id:
        raise ValueError("Missing resource ID")
    return {"reference": f"{resource_type}/{resource_id}" if resource_type else resource_id}


def unref_resource(ref: dict) -> (Optional[str], str):
    """
    Returns the type & ID for the target of the reference

    Examples:
    - reference=Patient/ABC -> (Patient, ABC)
    - reference=ABC, type=Patient -> (Patient, ABC)

    Raises ValueError if the reference could not be understood
    """
    if not ref or not ref.get("reference"):
        raise ValueError(f'Reference type not handled: "{ref}"')

    if ref["reference"].startswith("#"):
        # This is a contained reference (internal reference to or from the toplevel resource / contained resources).
        # See https://www.hl7.org/fhir/references.html#contained for more info.
        #
        # We don't need to support these super well, since they aren't used for cross-reference joining.
        # In particular, we don't need to care about the resource type, since we don't need to record any of these
        # in the codebook for debugging, which is the primary purpose of grabbing the resource type.
        # In fact, we don't even want to try to grab the "type" field, since we shouldn't record these in the codebook
        # or attempt to merge the type and id like "Patient/#123".
        #
        # We include the pound sign in the returned ID string, to allow others to detect this case.
        return None, ref["reference"]

    # FIXME: Support absolute resources like http://fhir.hl7.org/svc/StructureDefinition/c8973a22
    if not RELATIVE_REFERENCE_REGEX.match(ref["reference"]) and not ref.get("type"):
        raise ValueError(f'Unrecognized reference: "{ref["reference"]}"')

    tokens = RELATIVE_SEPARATOR_REGEX.split(ref["reference"], maxsplit=1)
    if len(tokens) > 1:
        return tokens[0], tokens[1]

    if not ref.get("type"):
        raise ValueError(f'Reference does not have a type: "{ref["reference"]}"')
    return ref.get("type"), tokens[0]
