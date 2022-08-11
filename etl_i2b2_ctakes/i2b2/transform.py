from i2b2_schema import PatientDimension, VisitDimension, ObservationFact
from fhirclient.models.patient import Patient
from fhirclient.models.encounter import Encounter
from fhirclient.models.condition import Condition
from fhirclient.models.observation import Observation
from fhirclient.models.codeableconcept import CodeableConcept
from fhirclient.models.coding import Coding
import fhir_template

def map_race(display:str) -> str:
    race = {"White": "2106-3",
            "Black or African American": "2054-5",
            "American Indian or Alaska Native": "1002-5",
            "Asian": "2028-9",
            "Native Hawaiian or Other Pacific Islander": "2076-8",
            "Hispanic or Latino": "2135-2",
            "Not Hispanic or Latino": "2186-5"}

    if display and (display in race):
        return race[display]

def to_fhir_patient(pat: PatientDimension) -> Patient:
    subject = Patient(fhir_template.fhir_patient())
    subject.id = pat.patient_num
    subject.gender = pat.sex_cd # TODO: Map

    return subject

def to_fhir_encounter(visit: VisitDimension) -> Encounter:
    pass

def to_fhir_observation(obsfact: ObservationFact) -> Observation:
    pass

def to_fhir_condition(obsfact: ObservationFact) -> Condition:
    pass
