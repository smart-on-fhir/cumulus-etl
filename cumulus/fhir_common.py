"""FHIR utility methods"""

from typing import List, Optional, Union
import datetime

from fhirclient.models.fhirreference import FHIRReference
from fhirclient.models.coding import Coding
from fhirclient.models.codeableconcept import CodeableConcept
from fhirclient.models.fhirdate import FHIRDate
from fhirclient.models.period import Period
from fhirclient.models.range import Range


###############################################################################
# Standard FHIR References are ResourceType/id
###############################################################################

def ref_resource(resource_type: str, resource_id: str) -> Optional[FHIRReference]:
    """
    Reference the FHIR proper way
    :param resource_type: Name of resource, like "Patient"
    :param resource_id: ID for resource (isa REF can be UUID)
    :return: FHIRReference as Resource/$id
    """
    if not resource_id:
        raise ValueError('Missing resource ID')
    return FHIRReference({'reference': f'{resource_type}/{resource_id}'})

def ref_subject(subject_id: str) -> Optional[FHIRReference]:
    """
    Patient Reference the FHIR proper way
    :param subject_id: ID for patient (isa REF can be UUID)
    :return: FHIRReference as Patient/$id
    """
    return ref_resource('Patient', subject_id)

def ref_encounter(encounter_id: str) -> Optional[FHIRReference]:
    """
    Encounter Reference the FHIR proper way
    :param encounter_id: ID for encounter (isa REF can be UUID)
    :return: FHIRReference as Encounter/$id
    """
    return ref_resource('Encounter', encounter_id)

def ref_document(docref_id: str) -> Optional[FHIRReference]:
    """
    Encounter Reference the FHIR proper way
    :param docref_id: ID for encounter (isa REF can be UUID)
    :return: FHIRReference as Encounter/$id
    """
    return ref_resource('DocumentReference', docref_id)

###############################################################################
# FHIR Coding and CodeableConcept
###############################################################################

def fhir_concept(text: str, coded: List[Coding], extension=None) -> CodeableConcept:
    """
    Helper function, simplifies FHIR semantics for when to use types/json
    :param text: NLP MatchText.text
    :param coded: FHIR list of coded replies (from NLP)
    :param extension: optional FHIR extension for additional metadata
    :return: Concept including human readable 'text' and list of codes
    """
    as_json = [c.as_json() for c in coded]
    concept = CodeableConcept({'text': text, 'coding': as_json})

    if extension:
        concept.extension = [extension]

    return concept

def fhir_coding(vocab: str, code: str, display=None) -> Coding:
    """
    Helper function, simplifies FHIR semantics for when to use types/json
    :param vocab: Coding "System" is NLP Vocab, see also URL.umls_system
    :param code: code in source vocabulary (usually a UMLS codingScheme)
    :param display: optional string label, NLP may just use the match text label.
    :return: FHIR Coding for the NLP coded response.
    """
    if display:
        return Coding({'system': vocab, 'code': code, 'display': display})
    else:
        return Coding({'system': vocab, 'code': code})


###############################################################################
# FHIR Dates, Periods, and Ranges
###############################################################################

def fhir_date_now() -> FHIRDate:
    """
    :return: current time (UTC)
    """
    return FHIRDate(str(datetime.datetime.now(datetime.timezone.utc)))


def parse_fhir_date(yyyy_mm_dd: Union[str, FHIRDate]) -> Optional[FHIRDate]:
    """
    :param yyyy_mm_dd: YEAR Month Date
    :return: FHIR Date with only the date part.
    """
    if yyyy_mm_dd and isinstance(yyyy_mm_dd, FHIRDate):
        return yyyy_mm_dd
    if yyyy_mm_dd and isinstance(yyyy_mm_dd, str):
        yyyy_mm_dd = yyyy_mm_dd[:10]  # ignore the time portion
        return FHIRDate(yyyy_mm_dd)


def parse_fhir_date_isostring(yyyy_mm_dd) -> Optional[str]:
    """
    :param yyyy_mm_dd:
    :return: str version of the
    """
    parsed = parse_fhir_date(yyyy_mm_dd)
    return parsed.isostring if parsed else None


def parse_fhir_period(start_date, end_date) -> Period:
    if isinstance(start_date, str):
        start_date = parse_fhir_date(start_date)
    if isinstance(end_date, str):
        end_date = parse_fhir_date(end_date)

    p = Period()
    p.start = start_date
    p.end = end_date
    return p


def parse_fhir_range(low, high) -> Range:
    r = Range()
    r.low = low
    r.high = high
    return r
