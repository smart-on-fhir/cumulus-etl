"""FHIR utility methods"""

import datetime
import re
import urllib.parse

import cumulus_fhir_support as cfs

from cumulus_etl import errors

# A relative reference is something like Patient/123 or Patient?identifier=http://hl7.org/fhir/sid/us-npi|9999999299
# (vs a contained reference that starts with # or an absolute URL reference like http://example.org/Patient/123)
RELATIVE_REFERENCE_REGEX = re.compile("[A-Za-z]+[/?].+")
RELATIVE_SEPARATOR_REGEX = re.compile("[/?]")


class RemoteAttachment(Exception):
    """A note was requested, but it was only available remotely"""


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
# Clinical note parsing (downloading notes etc)
#
######################################################################################################################


def get_concept_user_text(concept: dict | None) -> str | None:
    concept = concept or {}

    # First try overall text, if provided
    if text := concept.get("text"):
        return text
    else:
        # Else use first display value we see (no need to grab multiples, they will be
        # redundant inside the same concept)
        for coding in concept.get("coding", []):
            if display := coding.get("display"):
                return display

    return None


def get_concept_list_user_text(concepts: list[dict] | None) -> set[str]:
    results = set()

    if concepts is None:
        return results

    for concept in concepts:
        if text := get_concept_user_text(concept):
            results.add(text)

    return results


def _get_human_name(names: list[dict] | None) -> str | None:
    if names is None:
        return None

    def name_priority(name: dict) -> int:
        match name.get("use"):
            case "official":
                return 2
            case "usual":
                return 1
            case _:
                return 0

    names = sorted(names, key=name_priority, reverse=True)

    for name in names:
        if text := name.get("text"):
            return text

    return None


def get_clinical_note_role_info(note: dict, src_dir: str) -> str:
    # Gather author/performer info
    match note.get("resourceType"):
        case "DiagnosticReport":
            title = "Performers:"
            people_refs = note.get("performer", [])
        case "DocumentReference":
            title = "Authors:"
            people_refs = note.get("author", [])
        case _:
            raise ValueError(f"{note.get('resourceType')} is not a supported clinical note type.")

    # Parse the refs
    people_sorted = []
    for ref in people_refs:
        try:
            people_sorted.append(unref_resource(ref))
        except ValueError:
            pass
    people = set(people_sorted)  # just in case there are repeats

    # Search through all our source PractitionerRole resources for matches.
    # This is a little slow, since we don't cache anything, but it's usually used in contexts like
    # uploading notes or NLP, where the number of documents is hopefully not enormous.
    # Likewise, the number of PractitionerRoles is hopefully not enormous either.
    found = {}
    for row in cfs.read_multiline_json_from_dir(src_dir, "PractitionerRole"):
        for person in people:
            person_type, person_id = person

            # Grab the pointer to the related Practitioner
            try:
                pract_type, pract_id = unref_resource(row.get("practitioner"))
                if pract_type != "Practitioner":
                    pract_id = None
            except ValueError:
                pract_type, pract_id = None, None

            # Does this PractitionerRole match our person?
            match person_type:
                case "PractitionerRole":
                    is_match = person_id == row.get("id")
                case "Practitioner":
                    is_match = person_id == pract_id
                case _:
                    is_match = False

            if is_match:
                # Find some display texts for this Role, folding it in to existing data
                # (Since there could be multiple Roles for the same Practitioner)
                found_data = found.setdefault(person, {})
                found_data["pract_id"] = pract_id
                found_role = found_data.setdefault("role", set())
                found_role |= get_concept_list_user_text(row.get("code"))
                found_specialty = found_data.setdefault("specialty", set())
                found_specialty |= get_concept_list_user_text(row.get("specialty"))
                break

    # Insert any Practitioners we know about but didn't find as Roles
    for person in people:
        person_type, person_id = person
        if person_type == "Practitioner" and person not in found:
            found[person] = {"pract_id": person_id}

    # Now go over Practitioners to find the actual human name to use
    for row in cfs.read_multiline_json_from_dir(src_dir, "Practitioner"):
        for person, data in found.items():
            if data["pract_id"] == row.get("id"):
                data["name"] = _get_human_name(row.get("name"))
                break

    # Now combine all the metadata we found into a giant string
    info = title
    if not people_sorted:
        info += " unknown"
    for person in people_sorted:
        found_data = found.get(person, {})
        name = found_data.get("name") or f"{person[0]}/{person[1]}"
        roles = found_data.get("role")
        specialties = found_data.get("specialty")

        info += f"\n* {name}"
        for role in sorted(roles or []):
            info += f"\n  - Role: {role}"
        for specialty in sorted(specialties or []):
            info += f"\n  - Specialty: {specialty}"

    return info
