# This file contains a mapping of various external coding systems to the concepts they represent


# PHIN VADS 1000-9, Race & Ethnicity - CDC
# https://phinvads.cdc.gov/vads/ViewCodeSystemConcept.action?oid=2.16.840.1.113883.6.238&code=1000-9
CDC_RACE = {
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
FHIR_GENDER = {
    'F': 'female',
    'M': 'male',
    'U': 'unknown',
}


# BCH internal lab codes mapping to international covid-19 codes
# system: http://loinc.org
LOINC_COVID_LAB_TESTS = {
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


# PHIN VADS General adjectival modifier (qualifier value) {106234000 , SNOMED-CT }
# Subset of codes related to evaluating lab results
# system: http://snomed.info/sct
SNOMED_LAB_RESULT = {
    'Positive': '10828004',
    'Negative': '260385009',
    'Absent': '272519000'
}


# PHIN VADS Admission statuses {308277006, SNOMED-CT }
# Subset of codes related to means of admition
# system: http://snomed.info/sct
SNOMED_ADMISSION = {
    'Inpatient': 'IMP',
    'Emergency': 'EMER'
}
