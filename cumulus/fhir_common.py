"""FHIR utility methods"""

from typing import List, Optional, Union
import datetime

from fhirclient.models.fhirreference import FHIRReference
from fhirclient.models.coding import Coding
from fhirclient.models.codeableconcept import CodeableConcept
from fhirclient.models.fhirdate import FHIRDate
from fhirclient.models.period import Period
from fhirclient.models.range import Range
from fhirclient.models.quantity import Quantity

###############################################################################
# Standard FHIR References are ResourceType/id
###############################################################################

def ref_resource(resource_type: str, resource_id: str) -> FHIRReference:
    """
    Reference the FHIR proper way
    :param resource_type: Name of resource, like "Patient"
    :param resource_id: ID for resource (isa REF can be UUID)
    :return: FHIRReference as Resource/$id
    """
    if not resource_id:
        raise ValueError('Missing resource ID')
    return FHIRReference({'reference': f'{resource_type}/{resource_id}'})


def ref_subject(subject_id: str) -> FHIRReference:
    """
    Patient Reference the FHIR proper way
    :param subject_id: ID for patient (isa REF can be UUID)
    :return: FHIRReference as Patient/$id
    """
    return ref_resource('Patient', subject_id)


def ref_encounter(encounter_id: str) -> FHIRReference:
    """
    Encounter Reference the FHIR proper way
    :param encounter_id: ID for encounter (isa REF can be UUID)
    :return: FHIRReference as Encounter/$id
    """
    return ref_resource('Encounter', encounter_id)


def ref_document(docref_id: str) -> FHIRReference:
    """
    Encounter Reference the FHIR proper way
    :param docref_id: ID for encounter (isa REF can be UUID)
    :return: FHIRReference as Encounter/$id
    """
    return ref_resource('DocumentReference', docref_id)


def unref_resource(ref: FHIRReference) -> (str, str):
    """
    Returns the type & ID for the target of the reference

    Examples:
    - reference=Patient/ABC -> (Patient, ABC)
    - reference=ABC, type=Patient -> (Patient, ABC)

    Raises ValueError if the reference could not be understood
    """
    # FIXME: Support contained resources like '#p1' and absolute resources like
    #        http://fhir.hl7.org/svc/StructureDefinition/c8973a22-2b5b-4e76-9c66-00639c99e61b
    if not ref.reference or ref.reference.count('/') > 2 or ref.reference.startswith('#'):
        raise ValueError(f'Unrecognized reference: "{ref.reference}"')

    tokens = ref.reference.split('/')
    if len(tokens) == 2:
        return tokens[0], tokens[1]

    if not ref.type:
        raise ValueError(f'Reference does not have a type: "{ref.reference}"')
    return ref.type, tokens[0]


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


def fhir_date_is_before(fhir_date1: Union[str, FHIRDate],
                        fhir_date2: Union[str, FHIRDate]) -> Optional[bool]:
    fhir_date1 = parse_fhir_date(fhir_date1)
    fhir_date2 = parse_fhir_date(fhir_date2)

    if fhir_date1 and fhir_date2:
        return fhir_date1.isostring < fhir_date2.isostring


def fhir_date_is_equal(fhir_date1: Union[str, FHIRDate],
                       fhir_date2: Union[str, FHIRDate]) -> Optional[bool]:
    fhir_date1 = parse_fhir_date(fhir_date1)
    fhir_date2 = parse_fhir_date(fhir_date2)

    if fhir_date1 and fhir_date2:
        return fhir_date1.isostring == fhir_date2.isostring


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


def parse_fhir_period(start_date=None, end_date=None) -> Period:
    """
    "Date Range", usually for an observation, encounter, or document reference.
    Range may have start_date or end_date, but defaults are none.

    https://build.fhir.org/datatypes.html#Period
    https://hl7.org/fhir/encounter-definitions.html#Encounter.period
    https://hl7.org/fhir/observation-definitions.html#Observation.effective_x_
    https://hl7.org/fhir/documentreference-definitions.html#DocumentReference.context.period

    In some contexts, the "Period" may refer to the selection of a population.
    See also cumulus class "CohortSelection"
    https://build.fhir.org/population.html

    @param start_date: None means no START date is required or known.
    @param end_date:None means no END date is required or known.
    @return: FHIR Period start/end optionally either start/end can be "unbound"
    """
    if isinstance(start_date, str):
        start_date = parse_fhir_date(start_date)
    if isinstance(end_date, str):
        end_date = parse_fhir_date(end_date)

    p = Period()
    p.start = start_date
    p.end = end_date
    return p


def _fhir_range(low=None, high=None) -> Range:
    """
    Specify a low<--->high range, for example LAB value reference range.
    Many different types of range can be specified.

    For *specific* date ranges, FHIR Period is usually more appropriate except
    when date range is count like LOS (Length Of Stay in days, hours, etc).

    https://www.hl7.org/fhir/datatypes.html#Range

    @param low: lower bound of range
    @param high: higher bound of rage
    @return: FHIR Range with low/high optionally specified.
    """
    r = Range()
    r.low = low
    r.high = high
    return r

def parse_fhir_range_duration(low=None, high=None, units='d') -> Range:
    """
    FHIR Range duration from milliseconds to years
    http://hl7.org/fhir/ValueSet/duration-units

    @param low: start/begin duration
    @param high: stop/end duration
    @param units: [ms,s,min,h,d,wk,mo,a] unit of time (see duration-units)
    @return: FHIR Range with FHIR Duration and FHIR Quantity nested
    """
    ucum = 'http://hl7.org/fhir/ValueSet/duration-units'
    _low = Quantity()
    _low.value = low
    _low.system = ucum
    _low.unit = units

    _high = Quantity()
    _high.value = high
    _high.system = ucum
    _high.unit = units

    return _fhir_range(_low, _high)









