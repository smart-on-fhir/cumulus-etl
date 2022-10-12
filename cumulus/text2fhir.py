"""NLP extension using ctakes"""
import uuid
from typing import List

from fhirclient.models.codeableconcept import CodeableConcept
from fhirclient.models.extension import Extension

from fhirclient.models.fhirdate import FHIRDate
from fhirclient.models.domainresource import DomainResource
from fhirclient.models.observation import Observation
from fhirclient.models.medicationstatement import MedicationStatement
from fhirclient.models.procedure import Procedure
from fhirclient.models.condition import Condition

import ctakesclient
from ctakesclient.typesystem import CtakesJSON
from ctakesclient.typesystem import Polarity, MatchText

from cumulus.fhir_common import ref_subject, ref_encounter
from cumulus.fhir_common import fhir_date_now
from cumulus.fhir_common import fhir_coding, fhir_concept

###############################################################################
# NLP Extensions: Enumerate URL and expected Value types
###############################################################################

NLP_ALGORITHM_URL = 'http://fhir-registry.smarthealthit.org/StructureDefinition/nlp-algorithm'
NLP_VERSION_URL = 'http://fhir-registry.smarthealthit.org/StructureDefinition/nlp-version'
NLP_TEXT_POSITION_URL = 'http://fhir-registry.smarthealthit.org/StructureDefinition/nlp-text-position'
NLP_DATE_PROCESSED_URL = 'http://fhir-registry.smarthealthit.org/StructureDefinition/nlp-date-processed'
NLP_POLARITY_URL = 'http://fhir-registry.smarthealthit.org/StructureDefinition/nlp-polarity'
UMLS_SYSTEM_URL = 'http://terminology.hl7.org/CodeSystem/umls'

VALUE_CODEABLE_CONCEPT = 'valueCodeableConcept'
VALUE_DATE = 'valueDate'
VALUE_INTEGER = 'valueInteger'
VALUE_STRING = 'valueString'
VALUE_BOOLEAN = 'valueBoolean'


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

def nlp_algorithm(version: Extension, processed: FHIRDate = None) -> Extension:
    """
    :param version: version info the NLP algorithm.
    :param processed: defines when the NLP algorithm date is effective
    :return: Extension
    """
    processed = processed or fhir_date_now()
    return Extension({'url': NLP_ALGORITHM_URL,
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
    positive = polarity == Polarity.pos

    return Extension({'url': NLP_POLARITY_URL,
                      VALUE_BOOLEAN: positive})


def nlp_text_position(pos_begin: int, pos_end: int) -> Extension:
    """
    FHIR Extension for the NLP Match Text position (ctakes client MatchText.pos())
    :param pos_begin: character position START
    :param pos_end: character position STOP
    :return:
    """
    ext_begin = Extension({VALUE_INTEGER: pos_begin, 'url': 'begin'})
    ext_end = Extension({VALUE_INTEGER: pos_end, 'url': 'end'})
    return Extension({
        'url': NLP_TEXT_POSITION_URL,
        'extension': [ext_begin.as_json(), ext_end.as_json()]
    })


def nlp_version(nlp_system: str, version_code: str, version_display: str) -> Extension:
    """
    :param nlp_system: NLP System, such as URL to denote which NLP was used.
    :param version_code: NLP Version MACHINE readable code
    :param version_display: NLP Version HUMAN readable display
    :return: FHIR extension for "nlp-version"
    """
    full_version = fhir_coding(nlp_system, version_code, version_display).as_json()

    return Extension({'url': NLP_VERSION_URL,
                      VALUE_CODEABLE_CONCEPT: {'text': 'NLP Version', 'coding': [full_version]}})


def nlp_version_client() -> Extension:
    """
    :return: FHIR Extension defining the default NLP Client (this program)
    """
    pkg = 'ctakesclient'
    ver = ctakesclient.__version__
    tag = f'https://github.com/Machine-Learning-for-Medical-Language/ctakes-client-py/releases/tag/v{ver}'
    return nlp_version(tag, ver, f'{pkg}=={ver}')


def nlp_date_processed(processed=fhir_date_now()) -> Extension:
    """
    :param processed: date processed, default is date_now()
    :return Extension for "processedDate"
    """
    return Extension({'url': NLP_DATE_PROCESSED_URL,
                      VALUE_DATE: processed.isostring})


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
    coded = []
    for concept in match.conceptAttributes:
        coded.append(fhir_coding(vocab=concept.codingScheme, code=concept.code))
        coded.append(fhir_coding(vocab=UMLS_SYSTEM_URL, code=concept.cui))
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


def nlp_fhir(subject_id: str, encounter_id: str, nlp_results: CtakesJSON,
             version=None, polarity=Polarity.pos) -> List[DomainResource]:
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
    as_fhir = []

    for match in nlp_results.list_sign_symptom(polarity):
        as_fhir.append(nlp_observation(subject_id, encounter_id, match, version))

    for match in nlp_results.list_medication(polarity):
        as_fhir.append(nlp_medication(subject_id, encounter_id, match, version))

    for match in nlp_results.list_disease_disorder(polarity):
        as_fhir.append(nlp_condition(subject_id, encounter_id, match, version))

    for match in nlp_results.list_procedure(polarity):
        as_fhir.append(nlp_procedure(subject_id, encounter_id, match, version))

    return as_fhir
