"""FHIR utility methods"""

import base64
import datetime
import email.message
import re
import urllib.parse
from collections.abc import Iterable
from typing import TYPE_CHECKING

import httpx
import inscriptis

from cumulus_etl import common, errors

if TYPE_CHECKING:
    from cumulus_etl.fhir.fhir_client import FhirClient  # pragma: no cover

# A relative reference is something like Patient/123 or Patient?identifier=http://hl7.org/fhir/sid/us-npi|9999999299
# (vs a contained reference that starts with # or an absolute URL reference like http://example.org/Patient/123)
RELATIVE_REFERENCE_REGEX = re.compile("[A-Za-z]+[/?].+")
RELATIVE_SEPARATOR_REGEX = re.compile("[/?]")


###############################################################################
# Standard FHIR References are ResourceType/id
###############################################################################


def ref_resource(resource_type: str | None, resource_id: str) -> dict:
    """
    Reference the FHIR proper way
    :param resource_type: Name of resource, like "Patient"
    :param resource_id: ID for resource (isa REF can be UUID)
    :return: FHIRReference as Resource/$id
    """
    if not resource_id:
        raise ValueError("Missing resource ID")
    return {"reference": f"{resource_type}/{resource_id}" if resource_type else resource_id}


def unref_resource(ref: dict | None) -> (str | None, str):
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

    return ref.get("type"), tokens[0]


######################################################################################################################
#
# Field parsing
#
######################################################################################################################


def parse_datetime(value: str | None) -> datetime.datetime | None:
    """
    Converts FHIR instant/dateTime/date types into a Python format.

    - This tries to be very graceful - any errors will result in a None return.
    - Missing month/day fields are treated as the earliest possible date (i.e. '1')

    CAUTION: Returned datetime might be naive - which makes more sense for dates without a time.
             The spec says any field with hours/minutes SHALL have a timezone.
             But fields that are just dates SHALL NOT have a timezone.
    """
    if not value:
        return None

    try:
        # Handle partial dates like "1980-12" (which spec allows, but fromisoformat can't handle)
        pieces = value.split("-")
        if len(pieces) == 1:
            return datetime.datetime(int(pieces[0]), 1, 1)  # note: naive datetime
        elif len(pieces) == 2:
            return datetime.datetime(int(pieces[0]), int(pieces[1]), 1)  # note: naive datetime

        # Until we depend on Python 3.11+, manually handle Z
        value = value.replace("Z", "+00:00")

        return datetime.datetime.fromisoformat(value)
    except ValueError:
        return None


class FhirUrl:
    """
    Parses a FHIR URL into relevant parts.

    Example URL parsing:
      - https://hostname/root/Group/my-group
        - Group: `my-group`, Base URL: https://hostname/root
      - https://hostname/root/Group/my-group/$export?_type=Patient
        - Group: `my-group`, Base URL: https://hostname/root
      - https://hostname/root
        - Group: ``, Base URL: https://hostname/root
    """

    def __init__(self, url: str):
        parsed = urllib.parse.urlsplit(url)
        if not parsed.scheme:
            errors.fatal(f"Could not parse URL '{url}'", errors.ARGS_INVALID)

        # Strip off common bulk-export-suffixes that the user might give us, to get the base path
        root_path = parsed.path
        root_path = root_path.removesuffix("/")
        root_path = root_path.removesuffix("/$export")
        root_path = root_path.removesuffix("/Patient")  # all-patient export
        group_pieces = root_path.split("/Group/", 2)
        root_path = group_pieces[0]

        # Original unmodified URL
        self.full_url = url

        # The root of the FHIR server (i.e. https://host/api/FHIR/R4/)
        root_parsed = parsed._replace(path=root_path, query="", fragment="")
        self.root_url = urllib.parse.urlunsplit(root_parsed)

        # When exporting, the lack of a group means a global (system or all-patient) export,
        # which doesn't seem super realistic, but is possible for the user to request.
        # An empty-string group indicates there was no group.
        self.group = group_pieces[1] if len(group_pieces) == 2 else ""


######################################################################################################################
#
# Resource downloading
#
######################################################################################################################


def linked_resources(resources: Iterable[str]) -> set[str]:
    """
    Returns all linked resources that we might care about.

    This is used to look for / download related resources as available.
    For example, we will want to request FhirClient scopes for these resources.
    Or scoop these resources up from the input folder.
    """
    linked = set()
    for resource in resources:
        match resource:
            case "DiagnosticReport":
                linked.add("Binary")
            case "DocumentReference":
                linked.add("Binary")
            case "MedicationRequest":
                linked.add("Medication")
    return linked


async def download_reference(client: "FhirClient", reference: str) -> dict | None:
    """
    Downloads a resource, given a FHIR reference.

    :param client: a FhirClient instance
    :param reference: the "reference" field from a Reference FHIR object (i.e. a string like "Resource/123" or a URL)
    :returns: the downloaded resource or None if it didn't need to be downloaded (contained resource)
    """
    # Is it a blank or contained reference? We can just bail if so.
    if not reference or reference.startswith("#"):
        return None

    # FhirClient will figure out whether this is an absolute or relative URL for us
    response = await client.request("GET", reference)
    return response.json()


######################################################################################################################
#
# Clinical note parsing (downloading notes etc)
#
######################################################################################################################


def parse_content_type(content_type: str) -> (str, str):
    """Returns (mimetype, encoding)"""
    msg = email.message.EmailMessage()
    msg["content-type"] = content_type
    return msg.get_content_type(), msg.get_content_charset("utf8")


def _mimetype_priority(mimetype: str) -> int:
    """
    Returns priority of mimetypes for docref notes.

    0 means "ignore"
    Higher numbers are higher priority
    """
    if mimetype == "text/plain":
        return 3
    elif mimetype == "text/html":
        return 2
    elif mimetype == "application/xhtml+xml":
        return 1
    return 0


async def request_attachment(client: "FhirClient", attachment: dict) -> httpx.Response:
    """
    Download the given attachment by URL.
    """
    mimetype, _charset = parse_content_type(attachment["contentType"])
    return await client.request(
        "GET",
        attachment["url"],
        # We need to pass Accept to get the raw data, not a Binary FHIR object.
        # See https://www.hl7.org/fhir/binary.html
        headers={"Accept": mimetype},
    )


async def _get_note_from_attachment(client: "FhirClient", attachment: dict) -> str:
    """
    Decodes or downloads a note from an attachment.

    Note that it is assumed a contentType is provided.

    :returns: the attachment's note text
    """
    _mimetype, charset = parse_content_type(attachment["contentType"])

    if "data" in attachment:
        return base64.standard_b64decode(attachment["data"]).decode(charset)

    # TODO: There are future optimizations to try to use our ctakes cache to avoid downloading in
    #  the first place:
    #   - use attachment["hash"] if available (algorithm mismatch though... maybe we should switch
    #     to sha1...)
    #   - send a HEAD request with "Want-Digest: sha-256" but Cerner at least does not support that
    if "url" in attachment:
        response = await request_attachment(client, attachment)
        return response.text

    raise ValueError("No data or url field present")


def _get_cached_note_path(resource: dict) -> str:
    return f"{common.get_temp_dir('notes')}/{resource['resourceType']}.{resource['id']}.txt"


def _get_cached_note(resource: dict) -> str | None:
    note_path = _get_cached_note_path(resource)
    try:
        return common.read_text(note_path)
    except FileNotFoundError:
        return None


def _save_cached_note(resource: dict, note: str) -> None:
    note_path = _get_cached_note_path(resource)
    common.write_text(note_path, note)


async def get_clinical_note(client: "FhirClient", resource: dict) -> str:
    """
    Returns the clinical note contained in or referenced by the given resource.

    It will try to find the simplest version (plain text) or convert html to plain text if needed.

    This also caches the note for the duration of the ETL, to avoid redundant downloads.
    """
    note = _get_cached_note(resource)
    if note is not None:
        return note

    match resource["resourceType"]:
        case "DiagnosticReport":
            attachments = resource.get("presentedForm", [])
        case "DocumentReference":
            attachments = [
                content["attachment"]
                for content in resource.get("content", [])
                if "attachment" in content
            ]
        case _:
            raise ValueError(f"{resource['resourceType']} is not a supported clinical note type.")

    # Find the best attachment to use, based on mimetype.
    # We prefer basic text documents, to avoid confusing cTAKES with extra formatting (like <body>).
    best_attachment_index = -1
    best_attachment_mimetype = None
    best_attachment_priority = 0
    for index, attachment in enumerate(attachments):
        if "contentType" in attachment:
            mimetype, _ = parse_content_type(attachment["contentType"])
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
        raise ValueError("No textual mimetype found")

    note = await _get_note_from_attachment(client, attachments[best_attachment_index])

    if best_attachment_mimetype in ("text/html", "application/xhtml+xml"):
        # An HTML note can confuse/stall cTAKES and also makes philtering difficult.
        # It may include mountains of spans/styling or inline base64 images that aren't relevant to our interests.
        # Upload Notes and ETL modes thus both prefer to work with plain text.
        #
        # Inscriptis makes a very readable version of the note, with a focus on maintaining the HTML layout,
        # which is especially helpful for upload-notes (and maybe also helps NLP by avoiding odd line breaks).
        note = inscriptis.get_text(note)

    # Strip this "line feed" character that often shows up in notes and is confusing for NLP.
    # Hopefully not many notes are using actual Spanish.
    note = note.replace("¿", " ")

    _save_cached_note(resource, note)
    return note
