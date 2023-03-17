"""FHIR utility methods"""

import base64
import cgi
import re
from typing import Optional, Tuple

from cumulus import fhir_client

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


######################################################################################################################
#
# DocumentReference parsing (downloading notes etc)
#
######################################################################################################################


def _parse_content_type(content_type: str) -> (str, str):
    """Returns (mimetype, encoding)"""
    # TODO: switch to message.Message parsing, since cgi is deprecated
    mimetype, params = cgi.parse_header(content_type)
    return mimetype, params.get("charset", "utf8")


def _mimetype_priority(mimetype: str) -> int:
    """
    Returns priority of mimetypes for docref notes.

    0 means "ignore"
    Higher numbers are higher priority
    """
    if mimetype == "text/plain":
        return 3
    elif mimetype.startswith("text/"):
        return 2
    elif mimetype in ("application/xml", "application/xhtml+xml"):
        return 1
    return 0


async def _get_docref_note_from_attachment(client: fhir_client.FhirClient, attachment: dict) -> Optional[str]:
    """
    Decodes or downloads a note from an attachment.

    Note that it is assumed a contentType is provided.

    :returns: the attachment's note text
    """
    mimetype, charset = _parse_content_type(attachment["contentType"])

    if "data" in attachment:
        return base64.standard_b64decode(attachment["data"]).decode(charset)

    # TODO: At some point we should centralize the downloading of attachments -- once we have multiple NLP tasks,
    #  we may not want to re-download the overlapping notes. When we do that, it should not be part of our bulk
    #  exporter, since we may be given already-exported ndjson.
    #
    # TODO: There are future optimizations to try to use our ctakes cache to avoid downloading in the first place:
    #   - use attachment["hash"] if available (algorithm mismatch though... maybe we should switch to sha1...)
    #   - send a HEAD request with "Want-Digest: sha-256" but Cerner at least does not support that
    if "url" in attachment:
        # We need to pass Accept to get the raw data, not a Binary object. See https://www.hl7.org/fhir/binary.html
        response = await client.request("GET", attachment["url"], headers={"Accept": mimetype})
        return response.text

    return None


async def get_docref_note(client: fhir_client.FhirClient, docref: dict) -> Tuple[Optional[str], Optional[str]]:
    attachments = [content["attachment"] for content in docref["content"]]

    # Find the best attachment to use, based on mimetype.
    # We prefer basic text documents, to avoid confusing cTAKES with extra formatting (like <body>).
    best_attachment_index = -1
    best_attachment_mimetype = None
    best_attachment_priority = 0
    for index, attachment in enumerate(attachments):
        if "contentType" in attachment:
            mimetype, _ = _parse_content_type(attachment["contentType"])
            priority = _mimetype_priority(mimetype)
            if priority > best_attachment_priority:
                best_attachment_priority = priority
                best_attachment_mimetype = mimetype
                best_attachment_index = index

    if best_attachment_index < 0:
        # We didn't find _any_ of our target text content types.
        # A content type isn't required by the spec with external URLs, so it's possible an unmarked link could be good.
        # But let's optimistically enforce the need for a content type ourselves by bailing here.
        # If we find a real-world need to be more permissive, we can change this later.
        # But note that if we do, we'll need to handle downloading Binary FHIR objects, in addition to arbitrary URLs.
        return None, None

    note = await _get_docref_note_from_attachment(client, attachments[best_attachment_index])

    # Strip this "line feed" character that often shows up in notes and is confusing for NLP.
    # Hopefully not many notes are using actual Spanish.
    note = note and note.replace("¿", " ")

    return note, best_attachment_mimetype
