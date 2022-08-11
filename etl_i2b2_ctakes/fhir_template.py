import os
import json
from enum import Enum

class FHIRTemplate(Enum):
    fhir_patient = 'fhir_patient_template.json'
    fhir_encounter = 'fhir_encounter_template.json'
    fhir_condition = 'fhir_condition_template.json'
    fhir_condition_example = 'fhir_condition_example.json'
    fhir_documentreference = 'fhir_documentreference_template.json'
    fhir_observation = 'fhir_observation_template.json'
    fhir_covid_classifier = 'fhir_covid_classifier_template.json'


def fhir_template(template:FHIRTemplate):
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

def fhir_patient():
    return fhir_template(FHIRTemplate.fhir_patient)

def fhir_encounter():
    return fhir_template(FHIRTemplate.fhir_encounter)

def fhir_condition():
    return fhir_template(FHIRTemplate.fhir_condition)

def fhir_observation():
    return fhir_template(FHIRTemplate.fhir_observation)

def fhir_documentreference():
    return fhir_template(FHIRTemplate.fhir_documentreference)

def fhir_covid_classifier():
    return fhir_template(FHIRTemplate.fhir_covid_classifier)