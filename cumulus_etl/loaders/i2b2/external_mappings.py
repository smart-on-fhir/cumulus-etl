"""Mappings of various external coding systems to the concepts they represent"""

# PHIN VADS 1000-9, Race & Ethnicity - CDC
# https://phinvads.cdc.gov/vads/ViewCodeSystemConcept.action?oid=2.16.840.1.113883.6.238&code=1000-9
# https://hl7.org/fhir/us/core/StructureDefinition-us-core-race.html
CDC_RACE = {
    "White": ("urn:oid:2.16.840.1.113883.6.238", "2106-3"),
    "Black or African American": ("urn:oid:2.16.840.1.113883.6.238", "2054-5"),
    "American Indian or Alaska Native": ("urn:oid:2.16.840.1.113883.6.238", "1002-5"),
    "Asian": ("urn:oid:2.16.840.1.113883.6.238", "2028-9"),
    "Native Hawaiian or Other Pacific Islander": ("urn:oid:2.16.840.1.113883.6.238", "2076-8"),
    "Other": ("urn:oid:2.16.840.1.113883.6.238", "2131-1"),
    "Declined to Answer": ("http://terminology.hl7.org/CodeSystem/v3-NullFlavor", "ASKU"),
    "Unable to Answer": ("http://terminology.hl7.org/CodeSystem/v3-NullFlavor", "ASKU"),
    "Unknown": ("http://terminology.hl7.org/CodeSystem/v3-NullFlavor", "UNK"),
}

# https://hl7.org/fhir/us/core/StructureDefinition-us-core-ethnicity.html
CDC_ETHNICITY = {
    "Hispanic or Latino": ("urn:oid:2.16.840.1.113883.6.238", "2135-2"),
    "Not Hispanic or Latino": ("urn:oid:2.16.840.1.113883.6.238", "2186-5"),
    "Declined to Answer": ("http://terminology.hl7.org/CodeSystem/v3-NullFlavor", "ASKU"),
    "Unable to Answer": ("http://terminology.hl7.org/CodeSystem/v3-NullFlavor", "ASKU"),
    "Unknown": ("http://terminology.hl7.org/CodeSystem/v3-NullFlavor", "UNK"),
}

# FHIR AdministrativeGender code (not a full gender spectrum, but quite limited)
# https://www.hl7.org/fhir/valueset-administrative-gender.html
# Anything not in this dictionary maps should map to 'other'
FHIR_GENDER = {
    "F": "female",
    "M": "male",
    "U": "unknown",
}


# BCH internal lab codes mapping to international covid-19 codes
# system: http://loinc.org
LOINC_COVID_LAB_TESTS = {
    "LAB:1043473617": "94500-6",
    "LAB:1044804335": "94500-6",
    "LAB:1044704735": "94500-6",
    "LAB:1134792565": "95406-5",
    "LAB:1148157467": "95406-5",
    "LAB:467288722": "85477-8",
    "LAB:152831642": "85476-0",
    "LAB:467288694": "85478-6",
    "LAB:467288700": "85479-4",
    "LAB:13815125": "62462-7",
}


# PHIN VADS General adjectival modifier (qualifier value) {106234000 , SNOMED-CT }
# Subset of codes related to evaluating lab results
# system: http://snomed.info/sct
SNOMED_LAB_RESULT = {
    "positive": "10828004",
    "negative": "260385009",
    "absent": "272519000",
}


# PHIN VADS Admission statuses {308277006, SNOMED-CT }
# Subset of codes related to means of admission
# system: http://snomed.info/sct
# https://terminology.hl7.org/5.0.0/ValueSet-v3-ActEncounterCode.html
SNOMED_ADMISSION = {
    "Day Surgery": "AMB",
    "Emergency": "EMER",
    "Inpatient": "IMP",
    "Observation": "OBSENC",
    "Outpatient": "AMB",
    "Recurring Outpatient Series": "AMB",
}


# From observed i2b2 units to UCUM
# System: http://unitsofmeasure.org
# Commonly used in FHIR for observations: http://hl7.org/fhir/R4/valueset-ucum-vitals-common.html
UCUM_CODES = {
    "%": "%",
    "bpm": "/min",  # beats per minute (heart rate)
    "br/min": "/min",  # breaths per minute (respiratory rate)
    "cm": "cm",
    "DegC": "Cel",
    "DegF": "[degF]",
    "in": "[in_i]",
    "kg": "kg",
    "kg/m2": "kg/m2",
    "lb": "[lb_av]",
    "mmHg": "mm[Hg]",
    "NOT DEFINED IN SOURCE": "",  # typical "no units" string, can ignore
}


# From i2b2 comparison codes to FHIR comparator values
# i2b2: https://community.i2b2.org/wiki/display/ServerSideDesign/Value+Columns
# FHIR: http://hl7.org/fhir/R4/valueset-quantity-comparator.html
COMPARATOR = {
    "L": "<",
    "LE": "<=",
    "GE": ">=",
    "G": ">",
    "E": "",  # equal, can ignore
}
