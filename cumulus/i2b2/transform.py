import enum
import logging
import math
import base64

from fhirclient.models.identifier import Identifier
from fhirclient.models.fhirreference import FHIRReference
from fhirclient.models.fhirdate import FHIRDate
from fhirclient.models.range import Range
from fhirclient.models.meta import Meta
from fhirclient.models.period import Period
from fhirclient.models.duration import Duration
from fhirclient.models.coding import Coding
from fhirclient.models.extension import Extension
from fhirclient.models.patient import Patient
from fhirclient.models.encounter import Encounter
from fhirclient.models.condition import Condition
from fhirclient.models.observation import Observation
from fhirclient.models.documentreference import DocumentReference
from fhirclient.models.documentreference import DocumentReferenceContext, DocumentReferenceContent
from fhirclient.models.attachment import Attachment
from fhirclient.models.codeableconcept import CodeableConcept

from ctakes.client import CtakesJSON

from cumulus import common, fhir_template
from cumulus.i2b2.schema import PatientDimension, VisitDimension, ObservationFact

#######################################################################################################################
#
# Transform: to_fhir_{resource_type}
#
#######################################################################################################################

def to_fhir_patient(patient: PatientDimension) -> Patient:
    """    
    :param patient: i2b2 Patient Dimension record 
    :return: https://www.hl7.org/fhir/patient.html
    """
    subject = Patient(fhir_template.fhir_patient())
    subject.id = patient.patient_num
    subject.identifier = [Identifier({'value': str(patient.patient_num)})]

    if patient.birth_date:
        subject.birthDate = parse_fhir_date(patient.birth_date)

    if patient.death_date:
        subject.deceasedDateTime = parse_fhir_date(patient.death_date)

    if patient.sex_cd:
        subject.gender = parse_gender(patient.sex_cd)

    if patient.zip_cd:
        subject.address[0].postalCode = parse_zip_code(patient.zip_cd)

    if patient.race_cd:
        race_code = parse_race(patient.race_cd)
        if race_code:
            race_ext = Extension(fhir_template.extension_race(race_code, patient.race_cd))
            subject.extension = list()
            subject.extension.append(race_ext)

    return subject

def to_fhir_encounter(visit: VisitDimension) -> Encounter:
    """
    :param visit: i2b2 Visit Dimension Record
    :return: https://www.hl7.org/fhir/encounter.html
    """
    encounter = Encounter(fhir_template.fhir_encounter())
    encounter.id = str(visit.encounter_num)
    encounter.identifier = [Identifier({'value': str(visit.encounter_num)})]
    encounter.subject = FHIRReference({'reference': visit.patient_num})

    if visit.inout_cd == 'Inpatient':
        encounter.class_fhir.code = 'IMP'
    elif visit.inout_cd == 'Emergency':
        encounter.class_fhir.code = 'EMER'
    else:
        logging.warning(f'skipping encounter.class_fhir.code for i2b2 INOUT_CD : {visit.inout_cd}')

    if visit.length_of_stay: # days
        encounter.length = Duration({'unit':'d', 'value': parse_fhir_duration(visit.length_of_stay)})

    encounter.period = Period(
        {'start': parse_fhir_date_isostring(visit.start_date),
         'end': parse_fhir_date_isostring(visit.end_date)})

    return encounter

def to_fhir_documentreference(obsfact: ObservationFact) -> DocumentReference:
    """
    :param obsfact: i2b2 observation fact containing the I2b2 NOTE as OBSERVATION_BLOB
    :return: https://www.hl7.org/fhir/documentreference.html
    """
    docref = DocumentReference()
    docref.indexed = FHIRDate()

    docref.subject = FHIRReference({'reference': str(obsfact.patient_num)})
    docref.context = DocumentReferenceContext()
    docref.context.encounter = [FHIRReference({'reference': str(obsfact.encounter_num)})]

    docref.type = CodeableConcept({'text': str(obsfact.concept_cd)}) # i2b2 Note Type
    docref.created = FHIRDate(parse_fhir_date_isostring(obsfact.start_date))
    docref.status = 'superseded'

    # TODO: Content Warning: Philter DEID should be used on all notes that are sent to Cumulus.
    content = DocumentReferenceContent()
    content.attachment = Attachment()
    content.attachment.contentType = 'text/plain'
    # content.attachment.data = str(base64.b64encode(str(obsfact.observation_blob).encode()))
    docref.content = [content]

    return docref

def to_fhir_observation(obsfact: ObservationFact) -> Observation:
    """
    :param obsfact: base "FHIR Observation" from base "I2B2 ObservationFact"
    :return: https://www.hl7.org/fhir/observation.html
    """
    observation = Observation()
    observation.id = common.fake_id()
    observation.subject = FHIRReference({'reference': str(obsfact.patient_num)})
    observation.context = FHIRReference({'reference': str(obsfact.encounter_num)})
    observation.effectiveDateTime = FHIRDate(parse_fhir_date_isostring(obsfact.start_date))

    return observation

def to_fhir_observation_lab(obsfact: ObservationFact, loinc= fhir_template.LOINC) -> Observation:
    """
    :param obsfact: i2b2 observation fact containing the LAB NAME AND VALUE
    :return: https://www.hl7.org/fhir/observation.html
    """
    observation = to_fhir_observation(obsfact)
    observation.status = 'final'

    if obsfact.concept_cd in loinc.keys():
        _code = loinc[obsfact.concept_cd]
        _system = 'http://loinc.org'
    else:
        _code = obsfact.concept_cd
        _system = 'https://childrenshospital.org/'

    observation.code = CodeableConcept()
    observation.code.coding = [Coding({'code': _code, 'system': _system})]

    # lab result
    lab_result = obsfact.tval_char

    concept = CodeableConcept()

    if lab_result in fhir_template.LAB_RESULT.keys():
        concept.coding = [Coding({'code': fhir_template.LAB_RESULT[lab_result],
                                  'display': obsfact.tval_char})]
    else:
        concept.coding = [Coding({'code': fhir_template.LAB_RESULT['Absent'],
                                  'display': 'Absent'})]

    observation.valueCodeableConcept = concept

    return observation

def to_fhir_observation_note(obsfact: ObservationFact, ctakes_json:CtakesJSON) -> Observation:
    """
    :param obsfact: i2b2 observation fact containing the LAB NAME AND VALUE
    :return: https://www.hl7.org/fhir/documentreference.html
    """
    observation = to_fhir_observation(obsfact)

    return observation

def to_fhir_condition(obsfact: ObservationFact) -> Condition:
    """
    :param obsfact: i2b2 observation fact containing ICD9, ICD10, or SNOMED diagnosis
    :return: https://www.hl7.org/fhir/condition.html
    """
    condition = Condition()
    condition.id = common.fake_id()

    condition.subject = FHIRReference({'reference': str(obsfact.patient_num)})
    condition.context = FHIRReference({'reference': str(obsfact.encounter_num)})

    condition.meta = Meta({'profile': ['http://hl7.org/fhir/us/core/StructureDefinition/us-core-condition']})

    condition.clinicalStatus = CodeableConcept({'text': 'active'})
    condition.verificationStatus = CodeableConcept({'text': 'unconfirmed'})

    # Category
    category = Coding()
    category.system = 'http://terminology.hl7.org/CodeSystem/condition-category'
    category.code = 'encounter-diagnosis'
    category.display = 'Encounter Diagnosis'

    condition.category = [CodeableConcept()]
    condition.category[0].coding = [category]

    # Code
    _i2b2_sys, _code = obsfact.concept_cd.split(':')

    if _i2b2_sys in ['ICD10', 'ICD-10'] :
        _i2b2_sys = 'http://hl7.org/fhir/sid/icd-10-cm'
    elif _i2b2_sys in ['ICD9', 'ICD-9']:
        _i2b2_sys = 'http://hl7.org/fhir/sid/icd-9-cm'
    elif _i2b2_sys in ['SNOMED', 'SNOMED-CT', 'SNOMEDCT', 'SCT']:
        _i2b2_sys = 'http://snomed.info/sct'
    else:
        logging.warning('Unknown System')
        _i2b2_sys = '???'

    code = Coding()
    code.code = _code
    code.system = _i2b2_sys

    condition.code = CodeableConcept()
    condition.code.coding = [code]

    return condition

# http://fhir-registry.smarthealthit.org/StructureDefinition/nlp-text-position


#######################################################################################################################
#
# parse i2b2 inputs to FHIR types
#
#######################################################################################################################

def parse_zip_code(i2b2_zip_code) -> str:
    """
    :param i2b2_zip_code:
    :return: Patient Address ZipCode (3-9 digits)
    """
    if i2b2_zip_code and isinstance(i2b2_zip_code, str):
        if 3 <= len(i2b2_zip_code) <= 9:
            return i2b2_zip_code


def parse_gender(i2b2_sex_cd) -> str:
    """
    :param i2b2_sex_cd:
    :return: M,F,T,U, NB
    """
    if i2b2_sex_cd and isinstance(i2b2_sex_cd, str):
        if i2b2_sex_cd in fhir_template.GENDER.keys():
            return fhir_template.GENDER[i2b2_sex_cd]
        else:
            logging.warning(f'i2b2_sex_cd unknown code  {i2b2_sex_cd}')
    logging.warning(f'i2b2_sex_cd missing: {i2b2_sex_cd}')


def parse_race(i2b2_race_cd) -> str:
    """
    :param i2b2_race_cd:
    :return: CDC R5 Race codes or None
    """
    if i2b2_race_cd and isinstance(i2b2_race_cd, str):
        if i2b2_race_cd in fhir_template.RACE.keys():
            return fhir_template.RACE[i2b2_race_cd]


def parse_fhir_duration(i2b2_length_of_stay) -> float:
    """
    :param i2b2_length_of_stay: usually an integer like "days"
    :return: FHIR Duration float "time"
    """
    if i2b2_length_of_stay:
        if isinstance(i2b2_length_of_stay, str):
            return float(i2b2_length_of_stay)
        if isinstance(i2b2_length_of_stay, int):
            return float(i2b2_length_of_stay)
        if isinstance(i2b2_length_of_stay, float):
            return i2b2_length_of_stay

def parse_fhir_date(i2b2_date_string) -> FHIRDate:
    """
    :param i2b2_date_string:
    :return: FHIR Date with only the date part.
    """
    if i2b2_date_string and isinstance(i2b2_date_string, FHIRDate):
        return i2b2_date_string
    if i2b2_date_string and isinstance(i2b2_date_string, str):
        if len(i2b2_date_string) >= 10:
            i2b2_date_string = i2b2_date_string[:10]
            return FHIRDate(i2b2_date_string)


def parse_fhir_date_isostring(i2b2_date_string) -> str:
    """
    :param i2b2_date_string:
    :return: str version of the
    """
    parsed = parse_fhir_date(i2b2_date_string)
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
