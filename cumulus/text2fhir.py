"""NLP extension using ctakes"""
import uuid
from enum import Enum
from typing import List
import pkg_resources
import datetime

from fhirclient.models.coding import Coding
from fhirclient.models.codeableconcept import CodeableConcept
from fhirclient.models.extension import Extension
from fhirclient.models.fhirdate import FHIRDate
from fhirclient.models.fhirreference import FHIRReference

from fhirclient.models.domainresource import DomainResource
from fhirclient.models.observation import Observation
from fhirclient.models.medicationstatement import MedicationStatement
from fhirclient.models.procedure import Procedure
from fhirclient.models.condition import Condition
from fhirclient.models.bundle import Bundle, BundleEntry

from ctakesclient import client
from ctakesclient.typesystem import CtakesJSON
from ctakesclient.typesystem import Polarity, Span, MatchText
from ctakesclient.typesystem import UmlsTypeMention, UmlsConcept

###############################################################################
# NLP Extensions: Enumerate URL and expected Value types
###############################################################################

class URL(Enum):
    # pylint: disable=line-too-long
    nlp_algorithm = 'http://fhir-registry.smarthealthit.org/StructureDefinition/nlp-algorithm'
    nlp_version = 'http://fhir-registry.smarthealthit.org/StructureDefinition/nlp-version'
    nlp_text_position = 'http://fhir-registry.smarthealthit.org/StructureDefinition/nlp-text-position'
    nlp_date_processed = 'http://fhir-registry.smarthealthit.org/StructureDefinition/nlp-date-processed'
    nlp_polarity = 'http://fhir-registry.smarthealthit.org/StructureDefinition/nlp-polarity'
    umls_system = 'http://terminology.hl7.org/CodeSystem/umls'

class Value(Enum):
    valueCodeableConcept = CodeableConcept()
    valueDate = FHIRDate()
    valueInteger = int()
    valueString = str()
    valueBoolean = None

###############################################################################
# Basic FHIR Extension Helper Methods
###############################################################################
def ref_subject(subject_id:str) -> FHIRReference:
    """
    :param subject_id: ID for patient (isa REF can be UUID)
    :return: FHIRReference as Patient/$id
    """
    if subject_id and len(subject_id) > 3:
        return FHIRReference({'reference': f'Patient/{subject_id}'})

def ref_encounter(encounter_id:str) -> FHIRReference:
    """
    :param encounter_id: ID for encounter (isa REF can be UUID)
    :return: FHIRReference as Encounter/$id
    """
    if encounter_id and len(encounter_id) > 3:
        return FHIRReference({'reference': f'Encounter/{encounter_id}'})

def fhir_date_now() -> FHIRDate:
    """
    :return: FHIRDate using local datetime.now()
    """
    return FHIRDate(str(datetime.datetime.now()))

def fhir_concept(text:str, coded: List[Coding], extension=None) -> CodeableConcept:
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
# NLP Extension methods :
#
#   nlp_algorithm
#   nlp_modifier
#   nlp_polarity
#   nlp_text_position
#   nlp_version
#   nlp_version_client
#   nlp_date_processed
#
###############################################################################

def nlp_algorithm(version:Extension, processed= fhir_date_now()) -> Extension:
    """
    :param version: version info the NLP algorithm.
    :param processed: defines when the NLP algorithm date is effective
    :return: Extension
    """
    return Extension({'url': URL.nlp_algorithm.value,
                      'extension': [version.as_json(),
                                    nlp_date_processed(processed).as_json()]})

def nlp_modifier(polarity: Polarity, version=None):
    """
    :param polarity: pos= concept is true, neg=concept is negated ("patient denies cough")
    :param version: nlp-version(...)
    :return: FHIR resource.modifierExtension
    """
    if version is None:
        version = nlp_version_client()

    return [nlp_algorithm(version), nlp_polarity(polarity)]


def nlp_polarity(polarity: Polarity) -> Extension:
    """
    :param polarity: Positive or Negative
    :return: FHIR Extension for "nlp-polarity"
    """
    positive = True if polarity == Polarity.pos else False

    return Extension({'url': URL.nlp_polarity.value,
                      Value.valueBoolean.name: positive})

def nlp_text_position(pos_begin: int, pos_end: int) -> Extension:
    """
    FHIR Extension for the NLP Match Text position (ctakes client MatchText.pos())
    :param pos_begin: character position START
    :param pos_end: character position STOP
    :return:
    """
    ext_begin = Extension({Value.valueInteger.name: pos_begin, 'url': 'begin'})
    ext_end = Extension({Value.valueInteger.name: pos_end, 'url': 'end'})
    return Extension({'url': URL.nlp_text_position.value,
                      'extension': [ext_begin.as_json(),
                                    ext_end.as_json()]
    })

def nlp_version(nlp_system: str, version_code: str, version_display: str) -> Extension:
    """
    :param nlp_system: NLP System, such as URL to denote which NLP was used.
    :param version_code: NLP Version MACHINE readable code
    :param version_display: NLP Version HUMAN readable display
    :return: FHIR extension for "nlp-version"
    """
    _version_ = fhir_coding(nlp_system, version_code, version_display).as_json()

    return Extension({'url': URL.nlp_version.value,
                      Value.valueCodeableConcept.name: {'text': 'NLP Version', 'coding': [_version_]}})

def nlp_version_client() -> Extension:
    """
    :return: FHIR Extension defining the default NLP Client (this program)
    """
    tag = 'https://github.com/Machine-Learning-for-Medical-Language/ctakes-client-py/releases/tag/v1.0.3'
    pkg = 'ctakesclient'
    ver = pkg_resources.get_distribution(pkg).version
    return nlp_version(tag, ver, f'{pkg}={ver}')

def nlp_date_processed(processed= fhir_date_now()) -> Extension:
    """
    :param processed: date processed, default is date_now()
    :return Extension for "processedDate"
    """
    return Extension({'url': URL.nlp_date_processed.value,
                      Value.valueDate.name: processed.isostring})

###############################################################################
# NLP conversion functions : simplify creation of FHIR resources for user.
#
#   nlp_fhir() -> List of FHIR resources
#
#   nlp_concept
#   nlp_condition
#   nlp_observation
#   nlp_medication
#   nlp_procedure
#
###############################################################################

def nlp_concept(match: MatchText) -> CodeableConcept:
    """
    NLP match --> FHIR CodeableConcept with text position
    :param match: everything needed to make CodeableConcept
    :return: FHIR CodeableConcept with both UMLS CUI and source vocab CODE
    """
    coded = list()
    for concept in match.conceptAttributes:
        coded.append(fhir_coding(vocab=concept.codingScheme, code=concept.code))
        coded.append(fhir_coding(vocab=URL.umls_system.value, code=concept.cui))
    return fhir_concept(match.text, coded, nlp_text_position(match.begin, match.end))

def nlp_condition(subject_id: str, encounter_id: str, nlp_match: MatchText, version=None) -> Condition:
    """
    :param subject_id: ID for patient (isa REF can be UUID)
    :param encounter_id: ID for visit (isa REF can be UUID)
    :param nlp_match: response from cTAKES or other NLP Client
    :param version: NLP Version information, if none is provided use version of ctakesclient.
    :return: FHIR Observation
    """
    condition = Condition()

    # id linkage
    condition.id = str(uuid.uuid4())
    condition.subject = ref_subject(subject_id)
    condition.encounter = ref_encounter(encounter_id)

    # status is unconfirmed - NLP is not perfect.
    status = fhir_coding('http://terminology.hl7.org/CodeSystem/condition-ver-status', 'unconfirmed')
    condition.verificationStatus = fhir_concept(text='Unconfirmed', coded=[status])

    # nlp extensions
    condition.code = nlp_concept(nlp_match)
    condition.modifierExtension = nlp_modifier(nlp_match.polarity, version)

    return condition

def nlp_observation(subject_id: str, encounter_id: str, nlp_match: MatchText, version=None) -> Observation:
    """
    :param subject_id: ID for patient (isa REF can be UUID)
    :param encounter_id: ID for visit (isa REF can be UUID)
    :param nlp_match: response from cTAKES or other NLP Client
    :param version: NLP Version information, if none is provided use version of ctakesclient.
    :return: FHIR Observation
    """
    observation = Observation()

    # id linkage
    observation.id = str(uuid.uuid4())
    observation.subject = ref_subject(subject_id)
    observation.context = ref_encounter(encounter_id)
    observation.status = 'preliminary'

    # nlp extensions
    observation.code = nlp_concept(nlp_match)
    observation.modifierExtension = nlp_modifier(nlp_match.polarity, version)

    return observation

def nlp_medication(subject_id: str, encounter_id: str, nlp_match: MatchText, version=None) -> MedicationStatement:
    """
    :param subject_id: ID for patient (isa REF can be UUID)
    :param encounter_id: ID for encounter (isa REF can be UUID)
    :param nlp_match: response from cTAKES or other NLP Client
    :param version: NLP Version information, if none is provided use version of ctakesclient.
    :return: FHIR MedicationStatement
    """
    medication = MedicationStatement()

    # id linkage
    medication.id = str(uuid.uuid4())
    medication.subject = ref_subject(subject_id)
    medication.context = ref_encounter(encounter_id)
    medication.status = 'unknown'

    # nlp extensions
    medication.medicationCodeableConcept = nlp_concept(nlp_match)
    medication.modifierExtension = nlp_modifier(nlp_match.polarity, version)

    return medication

def nlp_procedure(subject_id: str, encounter_id: str, nlp_match: MatchText, version=None) -> Procedure:
    """
    :param subject_id: ID for patient (isa REF can be UUID)
    :param encounter_id: ID for encounter (isa REF can be UUID)
    :param nlp_match: response from cTAKES or other NLP Client
    :param version: NLP Version information, if none is provided use version of ctakesclient.
    :return: FHIR Procedure
    """
    procedure = Procedure()

    # id linkage
    procedure.id = str(uuid.uuid4())
    procedure.subject = ref_subject(subject_id)
    procedure.encounter = ref_encounter(encounter_id)
    procedure.status = 'unknown'

    # nlp extensions
    procedure.code = nlp_concept(nlp_match)
    procedure.modifierExtension = nlp_modifier(nlp_match.polarity, version)

    return procedure

def nlp_fhir(subject_id: str, encounter_id: str, nlp_results: CtakesJSON, version=None, polarity=Polarity.pos) -> List[DomainResource]:
    """
    FHIR Resources for a patient encounter.
    Use this method to get FHIR Observation, Condition, MedicationStatement, and Procedures for a note that is for a
    ** specific encounter **. Be advised that a single note can reference past medical history.

    :param subject_id: ID for patient (isa REF can be UUID)
    :param encounter_id: ID for encounter (isa REF can be UUID)
    :param nlp_results: response from cTAKES or other NLP Client
    :param version: NLP Version information, if none is provided use version of ctakesclient.
    :param polarity: filter only positive mentions by default
    :return: List of FHIR Resources (DomainResource)
    """
    as_fhir = list()

    for match in nlp_results.list_sign_symptom(polarity):
        as_fhir.append(nlp_observation(subject_id, encounter_id, match, version))

    for match in nlp_results.list_medication(polarity):
        as_fhir.append(nlp_medication(subject_id, encounter_id, match, version))

    for match in nlp_results.list_disease_disorder(polarity):
        as_fhir.append(nlp_condition(subject_id, encounter_id, match, version))

    for match in nlp_results.list_procedure(polarity):
        as_fhir.append(nlp_procedure(subject_id, encounter_id, match, version))

    return as_fhir


def fhir_bundle(resource_list: List[DomainResource]) -> Bundle:
    """
    Bundle up a NLP batch job as one collection "the FHIR way"
    https://build.fhir.org/valueset-bundle-type.html

    :param resource_list: FHIR Resources derived from an NLP process.
    :return: FHIR Bundle that packages up the collection
    """
    bundle = Bundle()
    bundle.type = 'collection'

    entries = list()
    for res in resource_list:
        entry = BundleEntry()
        entry.resource = res
        entries.append(entry)

    bundle.entry = entries

    return bundle
