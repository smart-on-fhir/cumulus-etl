"""JSON file templates"""

import os

from cumulus import common


###############################################################################
#
# JSON File Template (What BIN previously used)
#
###############################################################################

def template(name: str) -> dict:
    """
    https://stackoverflow.com/questions/1395593/managing-resources-in-a-python-project
    :param name: FHIR resource from saved JSON definition (Note: usage will be *deprecated*)
    :return: JSON of FHIR resource
    """
    return common.read_json(os.path.join(os.path.dirname(__file__), 'resources', name))


def fhir_patient() -> dict:
    return template('fhir_patient_template.json')


def fhir_encounter() -> dict:
    return template('fhir_encounter_template.json')


###############################################################################
#
# MAPPINGS - will re refactored into individual module for use in many hospital
# settings. See also 'Cumulus Library'.
#
###############################################################################
RACE = {
    'White': '2106-3',
    'Black or African American': '2054-5',
    'American Indian or Alaska Native': '1002-5',
    'Asian': '2028-9',
    'Native Hawaiian or Other Pacific Islander': '2076-8',
    'Hispanic or Latino': '2135-2',
    'Not Hispanic or Latino': '2186-5'
}

# FHIR AdministrativeGender code (not a full gender spectrum, but quite limited)
# https://www.hl7.org/fhir/valueset-administrative-gender.html
# Anything not in this dictionary maps should map to 'other'
GENDER = {
    'F': 'female',
    'M': 'male',
    'U': 'unknown',
}

LOINC = {
    'LAB:1043473617': '94500-6',
    'LAB:1044804335': '94500-6',
    'LAB:1044704735': '94500-6',
    'LAB:1134792565': '95406-5',
    'LAB:1148157467': '95406-5',
    'LAB:467288722': '85477-8',
    'LAB:152831642': '85476-0',
    'LAB:467288694': '85478-6',
    'LAB:467288700': '85479-4',
    'LAB:13815125': '62462-7'
}

LAB_RESULT = {
    'Positive': '10828004',
    'Negative': '260385009',
    'Absent': '272519000'
}


def extension_race(code: str, display: str) -> dict:
    """
    :param code:
    :param display:
    :return:
    """
    return {
        'url':
            'http://hl7.org/fhir/us/core/StructureDefinition/us-core-race',
        'extension': [{
            'url': 'ombCategory',
            'valueCoding': {
                'system': 'urn:oid:2.16.840.1.113883.6.238',
                'code': code,
                'display': display
            }
        }]
    }
