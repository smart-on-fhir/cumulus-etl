import logging
import base64

from fhirclient.models.identifier import Identifier
from fhirclient.models.fhirreference import FHIRReference
from fhirclient.models.fhirdate import FHIRDate
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

from etl import common, fhir_template
from etl.i2b2.schema import PatientDimension, VisitDimension, ObservationFact

def to_fhir_patient(patient: PatientDimension) -> Patient:
    """    
    :param patient: i2b2 Patient Dimension record 
    :return: https://www.hl7.org/fhir/patient.html
    """
    subject = Patient(fhir_template.fhir_patient())
    subject.id = patient.patient_num
    subject.identifier = [Identifier({'value': str(patient.patient_num)})]

    if patient.birth_date:
        subject.birthDate = FHIRDate(patient.birth_date)

    if patient.death_date:
        subject.deceasedDateTime = FHIRDate(patient.death_date)

    if patient.sex_cd:
        subject.gender = fhir_template.GENDER[patient.sex_cd]

    if len(patient.zip_cd) >= 3:
        subject.address[0].postalCode = patient.zip_cd

    if patient.race_cd:
        if patient.race_cd in fhir_template.RACE.keys():

            race_code = fhir_template.RACE[patient.race_cd]
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
        encounter.length = Duration({'unit':'d', 'value':visit.length_of_stay})

    encounter.period = Period({'start': visit.start_date, 'end': visit.end_date})

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
    docref.context.encounter = FHIRReference({'reference': str(obsfact.encounter_num)})

    docref.type = CodeableConcept({'text': str(obsfact.concept_cd)}) # i2b2 Note Type
    docref.created = FHIRDate(obsfact.start_date)
    docref.status = 'superseded'

    content = DocumentReferenceContent()
    content.attachment = Attachment()
    content.attachment.contentType = 'text/plain'
    content.attachment.data = str(base64.b64encode(str(obsfact.observation_blob).encode()))

    docref.content = [content]

    return docref

def to_fhir_observation_lab(obsfact: ObservationFact, loinc= fhir_template.LOINC) -> Observation:
    """
    :param obsfact: i2b2 observation fact containing the LAB NAME AND VALUE
    :return: https://www.hl7.org/fhir/documentreference.html
    """
    observation = Observation(fhir_template.fhir_observation())
    observation.id = common.fake_id()
    observation.subject = FHIRReference({'reference': str(obsfact.patient_num)})
    observation.encounter = FHIRReference({'reference': str(obsfact.encounter_num)})

    if obsfact.concept_cd in loinc.keys():
        _code = loinc[obsfact.concept_cd]
        _system = 'http://loinc.org'
    else:
        _code = obsfact.concept_cd
        _system = 'https://childrenshospital.org/'

    observation.code.coding[0].code = _code
    observation.code.coding[0].system = _system

    lab_result = obsfact.tval_char

    if lab_result in fhir_template.LAB_RESULT.keys():
        observation.valueCodeableConcept.coding[0].display = obsfact.tval_char
        observation.valueCodeableConcept.coding[0].code = fhir_template.LAB_RESULT[lab_result]
    else:
        observation.valueCodeableConcept.coding[0].display = 'Absent'
        observation.valueCodeableConcept.coding[0].code = fhir_template.LAB_RESULT['Absent']

    observation.effectiveDateTime = FHIRDate(obsfact.start_date)

    return observation


def to_fhir_condition(obsfact: ObservationFact) -> Condition:
    """
    :param obsfact: i2b2 observation fact containing ICD9, ICD10, or SNOMED diagnosis
    :return: https://www.hl7.org/fhir/condition.html
    """
    condition = Condition()
    condition.id = common.fake_id()

    condition.subject = FHIRReference({'reference': str(obsfact.patient_num)})
    condition.encounter = FHIRReference({'reference': str(obsfact.encounter_num)})

    condition.meta = Meta({'profile': ['http://hl7.org/fhir/us/core/StructureDefinition/us-core-condition']})

    # TODO: fhirclient out of date? Should be type CodableConcept
    # http://terminology.hl7.org/CodeSystem/condition-clinical
    # https://terminology.hl7.org/3.1.0/CodeSystem-condition-ver-status.html
    condition.clinicalStatus = 'active'
    condition.verificationStatus = 'unconfirmed'

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

