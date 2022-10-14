"""NLP extension using ctakes"""

from typing import List

from fhirclient.models.resource import Resource
from fhirclient.models.domainresource import DomainResource
from fhirclient.models.extension import Extension
from fhirclient.models.fhirreference import FHIRReference

from fhirclient.models.codeableconcept import CodeableConcept
from fhirclient.models.observation import Observation
from fhirclient.models.medicationstatement import MedicationStatement
from fhirclient.models.procedure import Procedure
from fhirclient.models.condition import Condition

import ctakesclient
from ctakesclient.typesystem import CtakesJSON, Span
from ctakesclient.typesystem import Polarity, MatchText

from cumulus import common
from cumulus.fhir_common import ref_subject, ref_encounter, ref_document
from cumulus.fhir_common import fhir_coding, fhir_concept

###############################################################################
# URLs to qualify Extensions:
#   FHIR DerivationReference
#   SMART-on-FHIR StructureDefinition
###############################################################################
FHIR_DERIVATION_REF_URL = 'http://hl7.org/fhir/StructureDefinition/derivation-reference'

NLP_SOURCE_URL = 'http://fhir-registry.smarthealthit.org/StructureDefinition/nlp-source'
NLP_POLARITY_URL = 'http://fhir-registry.smarthealthit.org/StructureDefinition/nlp-polarity'

UMLS_SYSTEM_URL = 'http://terminology.hl7.org/CodeSystem/umls'  # TODO: refactor


###############################################################################
# Common extension helper methods
#
###############################################################################
def value_string(url: str, value: str) -> Extension:
    """
    :param url: either URL or simple "key"
    :param value: valueString to associate with URL
    :return: Extension with simple url:valueString
    """
    return Extension({'url': url, 'valueString': value})


def value_integer(url: str, value: str) -> Extension:
    """
    :param url: either URL or simple "key"
    :param value: valueInteger to associate with URL
    :return: Extension with simple url:valueInteger
    """
    return Extension({'url': url, 'valueInteger': value})


def value_boolean(url: str, value: bool) -> Extension:
    """
    :param url: either URL or simple "key"
    :param value: valueString to associate with URL
    :return: Extension with simple url:valueBoolean
    """
    return Extension({'url': url, 'valueBoolean': value})


def value_reference(value: FHIRReference) -> Extension:
    """
    :param value: valueString to associate with URL
    :return: Extension with reference:FHIRReference like 'Patient/123456789'
    """
    return Extension({'url': 'reference', 'valueReference': value.as_json()})


def value_list(url: str, values: List) -> Extension:
    """
    :param url: either URL or simple "key"
    :param values: nested list of extensions
    :return: Extension with nested content extension:List
    """
    return Extension({'url': url, 'extension': as_json(values)})


def as_json(fhirdata):
    if isinstance(fhirdata, Extension):
        return fhirdata.as_json()
    elif isinstance(fhirdata, Resource):
        return fhirdata.as_json()
    elif isinstance(fhirdata, dict):
        return fhirdata
    elif isinstance(fhirdata, list):
        cons = []
        for r in fhirdata:
            if r:
                cons.append(as_json(r))
        return cons


###############################################################################
# modifierExtension
#   "nlp-source" is Required
#   "nlp-polarity" is Optional
#
###############################################################################

def nlp_modifier(source=None, polarity=None) -> List[Extension]:
    """
    :param source: default = "ctakesclient" with version tag.
    :param polarity: pos= concept is true, neg=concept is negated ("patient denies cough")
    :return: FHIR resource.modifierExtension
    """
    if source is None:
        source = nlp_source()

    if polarity is None:
        return [source]
    else:
        return [source, nlp_polarity(polarity)]


def nlp_source(algorithm=None, version=None) -> Extension:
    """
    :param algorithm: optional, default = "ctakesclient".
    :param version: optional, default = "ctakesclient" version.
    """
    values = [nlp_algorithm(algorithm).as_json(), nlp_version(version).as_json()]
    return value_list(NLP_SOURCE_URL, values)


def nlp_algorithm(algorithm=None) -> Extension:
    """
    :param algorithm: optional, default = "ctakesclient".
    """
    if not algorithm:
        algorithm = ctakesclient.__package__

    return value_string('algorithm', algorithm)


def nlp_version(version=None) -> Extension:
    """
    :param version: optional, default = "ctakesclient" version.
    """
    if version is None:
        release = ctakesclient.__version__
        version = f'https://github.com/Machine-Learning-for-Medical-Language/ctakes-client-py/releases/tag/v{release}'

    return value_string('version', version)


def nlp_polarity(polarity: Polarity) -> Extension:
    """
    :param polarity: pos= concept is true, neg=concept is negated ("patient denies cough")
    """
    positive = polarity == Polarity.pos
    return value_boolean(NLP_POLARITY_URL, positive)


###############################################################################
#  DerivationReference
#
#   "reference" is Required
#   "offset" is Optional
#   "length" is Optional
#
###############################################################################

def nlp_derivation_span(docref_id, span: Span) -> Extension:
    return nlp_derivation(docref_id=docref_id, offset=span.begin, length=(span.end - span.begin))


def nlp_derivation(docref_id, offset=None, length=None) -> Extension:
    """
    README: http://build.fhir.org/extension-derivation-reference.html

    :param docref_id: ID for the DocumentReference from which NLP resource was derived.
    :param offset: optional character *offset in document* (integer!)
    :param length: optional character *length from offset* of the matching text span.
    :return: Extension for DerivationReference defining which document and text position was derived using NLP
    """
    values = [value_reference(ref_document(docref_id))]

    if offset:
        values.append(value_integer('offset', offset))

    if length:
        values.append(value_integer('length', length))

    values = [v.as_json() for v in values]

    return Extension({'url': FHIR_DERIVATION_REF_URL, 'extension': values})


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

    return fhir_concept(match.text, coded)


def nlp_extensions(fhir_resource: Resource, docref_id: str, nlp_match: MatchText, source=None) -> None:
    """
    apply "extensions" and "modiferExtension" to provided $fhir_resource

    :param fhir_resource: passed by reference
    :param docref_id: ID for DocumentReference (isa REF can be UUID)
    :param nlp_match: response from cTAKES or other NLP Client
    :param source: NLP Version information, if none is provided use version of ctakesclient.
    """
    fhir_resource.modifierExtension = nlp_modifier(source, nlp_match.polarity)
    fhir_resource.extension = [nlp_derivation_span(docref_id, nlp_match.span())]


def nlp_condition(subject_id: str, encounter_id: str, docref_id: str, nlp_match: MatchText, source=None) -> Condition:
    """
    :param subject_id: ID for patient (isa REF can be UUID)
    :param encounter_id: ID for visit (isa REF can be UUID)
    :param docref_id: ID for DocumentReference (isa REF can be UUID)
    :param nlp_match: response from cTAKES or other NLP Client
    :param source: NLP Version information, if none is provided use version of ctakesclient.
    :return: FHIR Condition (such as DiseaseDisorder type)
    """
    condition = Condition()

    # id linkage
    condition.id = common.fake_id()
    condition.subject = ref_subject(subject_id)
    condition.encounter = ref_encounter(encounter_id)

    # status is unconfirmed - NLP is not perfect.
    status = fhir_coding('http://terminology.hl7.org/CodeSystem/condition-ver-status', 'unconfirmed')
    condition.verificationStatus = fhir_concept(text='Unconfirmed', coded=[status])

    # NLP
    nlp_extensions(condition, docref_id, nlp_match, source)
    condition.code = nlp_concept(nlp_match)

    return condition


def nlp_observation(
        subject_id: str,
        encounter_id: str,
        docref_id: str,
        nlp_match: MatchText,
        source=None,
) -> Observation:
    """
    :param subject_id: ID for patient (isa REF can be UUID)
    :param encounter_id: ID for visit (isa REF can be UUID)
    :param nlp_match: response from cTAKES or other NLP Client
    :param source: NLP Version information, if none is provided use version of ctakesclient.
    :return: FHIR Observation
    """
    observation = Observation()

    # id linkage
    observation.id = common.fake_id()
    observation.subject = ref_subject(subject_id)
    observation.encounter = ref_encounter(encounter_id)
    observation.status = 'preliminary'

    # NLP
    nlp_extensions(observation, docref_id, nlp_match, source)
    observation.code = nlp_concept(nlp_match)

    return observation


def nlp_medication(
        subject_id: str,
        encounter_id: str,
        docref_id: str,
        nlp_match: MatchText,
        source=None,
) -> MedicationStatement:
    """
    :param subject_id: ID for patient (isa REF can be UUID)
    :param encounter_id: ID for encounter (isa REF can be UUID)
    :param nlp_match: response from cTAKES or other NLP Client
    :param source: NLP Version information, if none is provided use version of ctakesclient.
    :return: FHIR MedicationStatement
    """
    medication = MedicationStatement()

    # id linkage
    medication.id = common.fake_id()
    medication.subject = ref_subject(subject_id)
    medication.context = ref_encounter(encounter_id)
    medication.status = 'unknown'

    # NLP
    nlp_extensions(medication, docref_id, nlp_match, source)
    medication.medicationCodeableConcept = nlp_concept(nlp_match)

    return medication


def nlp_procedure(subject_id: str, encounter_id: str, docref_id: str, nlp_match: MatchText, source=None) -> Procedure:
    """
    :param subject_id: ID for Patient (isa REF can be UUID)
    :param encounter_id: ID for visit (isa REF can be UUID)
    :param docref_id: ID for DocumentReference (isa REF can be UUID)
    :param nlp_match: response from cTAKES or other NLP Client
    :param source: NLP Version information, if none is provided use version of ctakesclient.
    :return: FHIR Procedure
    """
    procedure = Procedure()

    # id linkage
    procedure.id = common.fake_id()
    procedure.subject = ref_subject(subject_id)
    procedure.encounter = ref_encounter(encounter_id)
    procedure.status = 'unknown'

    # NLP
    nlp_extensions(procedure, docref_id, nlp_match, source)
    procedure.code = nlp_concept(nlp_match)

    return procedure


def nlp_fhir(subject_id: str, encounter_id: str, docref_id: str, nlp_results: CtakesJSON,
             source=None, polarity=Polarity.pos) -> List[DomainResource]:
    """
    FHIR Resources for a patient encounter.
    Use this method to get FHIR Observation, Condition, MedicationStatement, and Procedures for a note that is for a
    ** specific encounter **. Be advised that a single note can reference past medical history.

    :param subject_id: ID for Patient (isa REF can be UUID)
    :param encounter_id: ID for visit (isa REF can be UUID)
    :param docref_id: ID for DocumentReference (isa REF can be UUID)
    :param nlp_results: response from cTAKES or other NLP Client
    :param source: NLP Version information, if none is provided use version of ctakesclient.
    :param polarity: filter only positive mentions by default
    :return: List of FHIR Resources (DomainResource)
    """
    as_fhir = []

    for match in nlp_results.list_sign_symptom(polarity):
        as_fhir.append(nlp_observation(subject_id, encounter_id, docref_id, match, source))

    for match in nlp_results.list_medication(polarity):
        as_fhir.append(nlp_medication(subject_id, encounter_id, docref_id, match, source))

    for match in nlp_results.list_disease_disorder(polarity):
        as_fhir.append(nlp_condition(subject_id, encounter_id, docref_id, match, source))

    for match in nlp_results.list_procedure(polarity):
        as_fhir.append(nlp_procedure(subject_id, encounter_id, docref_id, match, source))

    return as_fhir
