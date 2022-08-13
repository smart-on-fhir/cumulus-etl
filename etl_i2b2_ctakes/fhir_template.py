import os
import json
from enum import Enum
from fhirclient.models.patient import Patient
from fhirclient.models.encounter import Encounter
from fhirclient.models.condition import Condition
from fhirclient.models.observation import Observation
from fhirclient.models.codeableconcept import CodeableConcept
from fhirclient.models.coding import Coding

#######################################################################################################################
#
# JSON File Template (What BIN previously used)
#
#######################################################################################################################

class FHIRTemplate(Enum):
    fhir_patient = 'fhir_patient_template.json'
    fhir_encounter = 'fhir_encounter_template.json'
    fhir_condition = 'fhir_condition_template.json'
    fhir_condition_example = 'fhir_condition_example.json'
    fhir_documentreference = 'fhir_documentreference_template.json'
    fhir_observation = 'fhir_observation_template.json'
    fhir_covid_classifier = 'fhir_covid_classifier_template.json'

def template(template:FHIRTemplate):
    """
    https://stackoverflow.com/questions/1395593/managing-resources-in-a-python-project
    :param template:
    :return:
    """
    jsonfile = os.path.join(os.path.dirname(__file__), '..', 'resources', template.value)

    if not os.path.exists(jsonfile):
        raise Exception(f'{jsonfile} does not exist')

    with open(jsonfile, 'r') as f:
        return json.load(f)

def fhir_patient() -> dict:
    return template(FHIRTemplate.fhir_patient)

def fhir_encounter() -> dict:
    return template(FHIRTemplate.fhir_encounter)

def fhir_condition() -> dict:
    return template(FHIRTemplate.fhir_condition)

def fhir_observation() -> dict:
    return template(FHIRTemplate.fhir_observation)

def fhir_documentreference() -> dict:
    return template(FHIRTemplate.fhir_documentreference)

def fhir_covid_classifier() -> dict:
    return template(FHIRTemplate.fhir_covid_classifier)

#######################################################################################################################
#
# Extensions
#
#######################################################################################################################
RACE = {"White": "2106-3",
        "Black or African American": "2054-5",
        "American Indian or Alaska Native": "1002-5",
        "Asian": "2028-9",
        "Native Hawaiian or Other Pacific Islander": "2076-8",
        "Hispanic or Latino": "2135-2",
        "Not Hispanic or Latino": "2186-5"}

GENDER = {'F': 'female',
          'M': 'male',
          'T': 'transgender',
          'U': 'Unknown',
          'NB': 'non-binary'}


def extension_race(code:str, display:str) -> dict:
    """
    :param code:
    :param display:
    :return:
    """
    return {"url":"http://hl7.org/fhir/us/core/StructureDefinition/us-core-race",
            "extension": [{ "url":"ombCategory",
                            "valueCoding": {
                                "system": "urn:oid:2.16.840.1.113883.6.238",
                                "code": code, "display": display}}]}
