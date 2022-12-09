"""Transformations from i2b2 to FHIR"""

import base64
import logging
from typing import Optional

from fhirclient.models.fhirdate import FHIRDate
from fhirclient.models.meta import Meta
from fhirclient.models.period import Period
from fhirclient.models.duration import Duration
from fhirclient.models.extension import Extension
from fhirclient.models.patient import Patient
from fhirclient.models.encounter import Encounter
from fhirclient.models.condition import Condition
from fhirclient.models.observation import Observation
from fhirclient.models.documentreference import DocumentReference
from fhirclient.models.documentreference import DocumentReferenceContext, DocumentReferenceContent
from fhirclient.models.attachment import Attachment
from fhirclient.models.codeableconcept import CodeableConcept

from cumulus import fhir_common
from cumulus.loaders.i2b2 import fhir_template
from cumulus.loaders.i2b2.schema import PatientDimension, VisitDimension, ObservationFact


###############################################################################
#
# Transform: to_fhir_{resource_type}
#
###############################################################################

def to_fhir_patient(patient: PatientDimension) -> Patient:
    """
    :param patient: i2b2 Patient Dimension record
    :return: https://www.hl7.org/fhir/patient.html
    """
    subject = Patient(fhir_template.fhir_patient())
    subject.id = patient.patient_num

    if patient.birth_date:
        subject.birthDate = fhir_common.parse_fhir_date(patient.birth_date)

    if patient.death_date:
        subject.deceasedDateTime = fhir_common.parse_fhir_date(patient.death_date)

    if patient.sex_cd:
        subject.gender = parse_gender(patient.sex_cd)

    if patient.zip_cd:
        # pylint: disable-next=unsubscriptable-object
        subject.address[0].postalCode = parse_zip_code(patient.zip_cd)

    if patient.race_cd:
        race_code = parse_race(patient.race_cd)
        if race_code:
            race_ext = Extension(
                fhir_template.extension_race(race_code, patient.race_cd))
            subject.extension = []
            subject.extension.append(race_ext)

    return subject


def to_fhir_encounter(visit: VisitDimension) -> Encounter:
    """
    :param visit: i2b2 Visit Dimension Record
    :return: https://www.hl7.org/fhir/encounter.html
    """
    encounter = Encounter(fhir_template.fhir_encounter())
    encounter.id = str(visit.encounter_num)
    encounter.subject = fhir_common.ref_subject(visit.patient_num)

    if visit.inout_cd == 'Inpatient':
        encounter.class_fhir.code = 'IMP'
    elif visit.inout_cd == 'Emergency':
        encounter.class_fhir.code = 'EMER'
    else:
        logging.warning(
            'skipping encounter.class_fhir.code for i2b2 '
            'INOUT_CD : %s', visit.inout_cd)

    if visit.length_of_stay:  # days
        encounter.length = Duration({
            'unit': 'd',
            'value': parse_fhir_duration(visit.length_of_stay)
        })

    encounter.period = Period({
        'start': fhir_common.parse_fhir_date_isostring(visit.start_date),
        'end': fhir_common.parse_fhir_date_isostring(visit.end_date)
    })

    return encounter


def to_fhir_observation(obsfact: ObservationFact) -> Observation:
    """
    :param obsfact: base "FHIR Observation" from base "I2B2 ObservationFact"
    :return: https://www.hl7.org/fhir/observation.html
    """
    observation = Observation()
    observation.id = obsfact.instance_num
    observation.subject = fhir_common.ref_subject(obsfact.patient_num)
    observation.encounter = fhir_common.ref_encounter(obsfact.encounter_num)
    observation.effectiveDateTime = FHIRDate(
        fhir_common.parse_fhir_date_isostring(obsfact.start_date))

    return observation


def to_fhir_observation_lab(obsfact: ObservationFact) -> Observation:
    """
    :param obsfact: i2b2 observation fact containing the LAB NAME AND VALUE
    :return: https://www.hl7.org/fhir/observation.html
    """
    observation = to_fhir_observation(obsfact)
    observation.status = 'final'

    if obsfact.concept_cd in fhir_template.LOINC:
        obs_code = fhir_template.LOINC[obsfact.concept_cd]
        obs_system = 'http://loinc.org'
    else:
        obs_code = obsfact.concept_cd
        obs_system = None

    observation.code = make_concept(obs_code, obs_system)

    # lab result
    if obsfact.tval_char in fhir_template.LAB_RESULT:
        lab_result = obsfact.tval_char
    else:
        lab_result = 'Absent'

    observation.valueCodeableConcept = make_concept(fhir_template.LAB_RESULT[lab_result], 'http://snomed.info/sct',
                                                    display=lab_result)

    return observation


def to_fhir_condition(obsfact: ObservationFact) -> Condition:
    """
    :param obsfact: i2b2 observation fact containing ICD9, ICD10, or SNOMED
                    diagnosis
    :return: https://www.hl7.org/fhir/condition.html
    """
    condition = Condition()
    condition.id = obsfact.instance_num

    condition.subject = fhir_common.ref_subject(obsfact.patient_num)
    condition.encounter = fhir_common.ref_encounter(obsfact.encounter_num)

    condition.meta = Meta({
        'profile': [
            'http://hl7.org/fhir/us/core/StructureDefinition/us-core-condition'
        ]
    })

    condition.clinicalStatus = make_concept('active', 'http://terminology.hl7.org/CodeSystem/condition-clinical')
    condition.verificationStatus = make_concept('unconfirmed',
                                                'http://terminology.hl7.org/CodeSystem/condition-ver-status')

    # Category
    condition.category = [make_concept('encounter-diagnosis',
                                       'http://terminology.hl7.org/CodeSystem/condition-category',
                                       display='Encounter Diagnosis')]

    # Code
    i2b2_sys, i2b2_code = obsfact.concept_cd.split(':')

    if i2b2_sys in ['ICD10', 'ICD-10']:
        i2b2_sys = 'http://hl7.org/fhir/sid/icd-10-cm'
    elif i2b2_sys in ['ICD9', 'ICD-9']:
        i2b2_sys = 'http://hl7.org/fhir/sid/icd-9-cm'
    elif i2b2_sys in ['SNOMED', 'SNOMED-CT', 'SNOMEDCT', 'SCT']:
        i2b2_sys = 'http://snomed.info/sct'
    else:
        logging.warning('Unknown System')
        i2b2_sys = '???'

    condition.code = make_concept(i2b2_code, i2b2_sys)

    return condition


###############################################################################
#
# Physician Notes and NLP
#
###############################################################################

def to_fhir_documentreference(obsfact: ObservationFact) -> DocumentReference:
    """
    :param obsfact: i2b2 observation fact containing the I2b2 NOTE as
                    OBSERVATION_BLOB
    :return: https://www.hl7.org/fhir/documentreference.html
    """
    docref = DocumentReference()
    docref.indexed = FHIRDate()

    docref.id = obsfact.instance_num
    docref.subject = fhir_common.ref_subject(obsfact.patient_num)
    docref.context = DocumentReferenceContext()
    docref.context.encounter = [fhir_common.ref_encounter(obsfact.encounter_num)]

    docref.type = CodeableConcept({'text': str(obsfact.concept_cd)})  # i2b2 Note Type
    docref.created = FHIRDate(fhir_common.parse_fhir_date_isostring(obsfact.start_date))
    docref.status = 'superseded'

    blob = obsfact.observation_blob or ''
    content = DocumentReferenceContent()
    content.attachment = Attachment()
    content.attachment.contentType = 'text/plain'
    content.attachment.data = base64.standard_b64encode(blob.encode('utf8')).decode('ascii')
    docref.content = [content]

    return docref


###############################################################################
#
# parse i2b2 inputs to FHIR types
#
###############################################################################

def parse_zip_code(i2b2_zip_code) -> str:
    """
    :param i2b2_zip_code:
    :return: Patient Address ZipCode (3-9 digits)
    """
    if i2b2_zip_code and isinstance(i2b2_zip_code, str):
        if 3 <= len(i2b2_zip_code) <= 9:
            return i2b2_zip_code


def parse_gender(i2b2_sex_cd) -> Optional[str]:
    """
    :param i2b2_sex_cd:
    :return: FHIR AdministrativeGender code
    """
    if i2b2_sex_cd and isinstance(i2b2_sex_cd, str):
        return fhir_template.GENDER.get(i2b2_sex_cd, 'other')


def parse_race(i2b2_race_cd) -> str:
    """
    :param i2b2_race_cd:
    :return: CDC R5 Race codes or None
    """
    if i2b2_race_cd and isinstance(i2b2_race_cd, str):
        if i2b2_race_cd in fhir_template.RACE:
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


def make_concept(code: str, system: str, display: str = None) -> CodeableConcept:
    """Syntactic sugar to make a codeable concept"""
    return CodeableConcept({'coding': [{'code': code, 'system': system, 'display': display}]})
