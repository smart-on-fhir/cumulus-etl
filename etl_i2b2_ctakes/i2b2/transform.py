import logging

from fhirclient.models.identifier import Identifier
from fhirclient.models.fhirreference import FHIRReference
from fhirclient.models.period import Period
from fhirclient.models.duration import Duration
from fhirclient.models.patient import Patient
from fhirclient.models.encounter import Encounter
from fhirclient.models.condition import Condition
from fhirclient.models.observation import Observation
from fhirclient.models.extension import Extension
from fhirclient.models.fhirdate import FHIRDate
from fhirclient.models.codeableconcept import CodeableConcept
from fhirclient.models.coding import Coding

from i2b2.i2b2_schema import PatientDimension, VisitDimension, ObservationFact
import fhir_template


def to_fhir_patient(patient: PatientDimension) -> Patient:
    """    
    :param patient: i2b2 Patient Dimension record 
    :return: https://www.hl7.org/fhir/patient.html
    """
    subject = Patient(fhir_template.fhir_patient())
    subject.id = patient.patient_num

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
    encounter.identifier = [Identifier({'value': str(visit.encounter_num)})]
    encounter.subject = FHIRReference({'reference': visit.patient_num})

    if visit.inout_cd == 'Inpatient':
        encounter.class_fhir.code = 'IMP'
    elif visit.inout_cd == 'Emergency':
        encounter.class_fhir.code = 'EMER'
    else:
        logging.debug(f'skipping encounter.class_fhir.code for i2b2 INOUT_CD : {visit.inout_cd}')

    if visit.length_of_stay: # days
        encounter.length = Duration({'unit':'d', 'value':visit.length_of_stay})

    encounter.period = Period({'start': visit.start_date, 'end': visit.end_date})

    return encounter

def to_fhir_observation(obsfact: ObservationFact) -> Observation:
    pass

def to_fhir_condition(obsfact: ObservationFact) -> Condition:
    pass
