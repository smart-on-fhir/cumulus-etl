"""Transformations from i2b2 to FHIR"""

import base64
import logging

from cumulus_etl import fhir
from cumulus_etl.loaders.i2b2 import external_mappings
from cumulus_etl.loaders.i2b2.schema import ObservationFact, PatientDimension, VisitDimension

###############################################################################
#
# Transform: to_fhir_{resource_type}
#
###############################################################################


def to_fhir_patient(patient: PatientDimension) -> dict:
    """
    :param patient: i2b2 Patient Dimension record
    :return: https://www.hl7.org/fhir/patient.html
    """
    subject = {
        "resourceType": "Patient",
        "id": str(patient.patient_num),
        "meta": {"profile": ["http://hl7.org/fhir/us/core/StructureDefinition/us-core-patient"]},
    }

    if patient.birth_date:
        subject["birthDate"] = chop_to_date(patient.birth_date)

    if patient.death_date:
        subject["deceasedDateTime"] = chop_to_date(patient.death_date)

    if patient.sex_cd:
        subject["gender"] = external_mappings.FHIR_GENDER.get(patient.sex_cd, "other")

    # TODO: verify that i2b2 always has a single patient address, always in US
    if patient.zip_cd:
        subject["address"] = [{"country": "US", "postalCode": patient.zip_cd}]

    if patient.race_cd:
        # race_cd can be either a race or an ethnicity. In FHIR, those are two different extensions.
        race_code = external_mappings.CDC_RACE.get(patient.race_cd)
        if race_code is not None:
            subject["extension"] = [
                {
                    "url": "http://hl7.org/fhir/us/core/StructureDefinition/us-core-race",
                    "extension": [
                        {
                            "url": "ombCategory",
                            "valueCoding": {
                                "system": race_code[0],
                                "code": race_code[1],
                                "display": patient.race_cd,
                            },
                        },
                    ],
                },
            ]

        ethnicity_code = external_mappings.CDC_ETHNICITY.get(patient.race_cd)
        if ethnicity_code is not None:
            subject["extension"] = [
                {
                    "url": "http://hl7.org/fhir/us/core/StructureDefinition/us-core-ethnicity",
                    "extension": [
                        {
                            "url": "ombCategory",
                            "valueCoding": {
                                "system": ethnicity_code[0],
                                "code": ethnicity_code[1],
                                "display": patient.race_cd,
                            },
                        },
                    ],
                },
            ]

    return subject


def to_fhir_encounter(visit: VisitDimension) -> dict:
    """
    :param visit: i2b2 Visit Dimension Record
    :return: https://www.hl7.org/fhir/encounter.html
    """
    encounter = {
        "resourceType": "Encounter",
        "id": str(visit.encounter_num),
        "subject": fhir.ref_resource("Patient", visit.patient_num),
        "meta": {"profile": ["http://hl7.org/fhir/us/core/StructureDefinition/us-core-encounter"]},
        "status": "unknown",
        "period": {"start": chop_to_date(visit.start_date), "end": chop_to_date(visit.end_date)},
        # Most generic encounter type possible, only included because the 'type' field is required in us-core
        "type": [
            make_concept("308335008", "http://snomed.info/sct", "Patient encounter procedure")
        ],
    }

    if visit.length_of_stay:  # days
        encounter["length"] = {
            "unit": "d",
            "value": visit.length_of_stay and float(visit.length_of_stay),
        }

    class_fhir = external_mappings.SNOMED_ADMISSION.get(visit.inout_cd)
    if not class_fhir:
        logging.debug("unknown encounter.class_fhir.code for i2b2 INOUT_CD : %s", visit.inout_cd)
        class_fhir = "?"  # bogus value, but FHIR demands *some* class value

    encounter["class"] = {
        "system": "http://terminology.hl7.org/CodeSystem/v3-ActCode",
        "code": class_fhir,
    }

    return encounter


def to_fhir_observation_lab(obsfact: ObservationFact) -> dict:
    """
    Turns an observation fact into a Observation with the Laboratory Result profile.

    See https://www.hl7.org/fhir/us/core/StructureDefinition-us-core-observation-lab.html

    :param obsfact: i2b2 observation fact containing the LAB NAME AND VALUE
    :return: Observation resource dictionary
    """
    observation = {
        "resourceType": "Observation",
        "id": str(obsfact.instance_num),
        "subject": fhir.ref_resource("Patient", obsfact.patient_num),
        "encounter": fhir.ref_resource("Encounter", obsfact.encounter_num),
        "category": [
            make_concept("laboratory", "http://terminology.hl7.org/CodeSystem/observation-category")
        ],
        "effectiveDateTime": chop_to_date(obsfact.start_date),
        "status": "unknown",
    }

    if obsfact.concept_cd in external_mappings.LOINC_COVID_LAB_TESTS:
        obs_code = external_mappings.LOINC_COVID_LAB_TESTS[obsfact.concept_cd]
        obs_system = "http://loinc.org"
    else:
        obs_code = obsfact.concept_cd
        obs_system = "http://cumulus.smarthealthit.org/i2b2"
    observation["code"] = make_concept(obs_code, obs_system)

    # lab result
    if lab_result := external_mappings.SNOMED_LAB_RESULT.get(obsfact.tval_char.lower()):
        lab_result_system = "http://snomed.info/sct"
    else:
        lab_result = obsfact.tval_char
        lab_result_system = "http://cumulus.smarthealthit.org/i2b2"
    observation["valueCodeableConcept"] = make_concept(
        lab_result, lab_result_system, display=obsfact.tval_char
    )

    return observation


def to_fhir_observation_vitals(obsfact: ObservationFact) -> dict:
    """
    Turns an observation fact into a Observation with the Vital Signs profile.

    See https://hl7.org/fhir/us/core/StructureDefinition-us-core-vital-signs.html

    :param obsfact: i2b2 observation fact containing vital signs
    :return: Observation resource dictionary
    """
    observation = {
        "resourceType": "Observation",
        "id": str(obsfact.instance_num),
        "status": "unknown",
        "category": [
            make_concept(
                "vital-signs", "http://terminology.hl7.org/CodeSystem/observation-category"
            )
        ],
        "code": make_concept(obsfact.concept_cd, "http://cumulus.smarthealthit.org/i2b2"),
        "subject": fhir.ref_resource("Patient", obsfact.patient_num),
        "encounter": fhir.ref_resource("Encounter", obsfact.encounter_num),
        "effectiveDateTime": chop_to_date(obsfact.start_date),
    }

    observation.update(get_observation_value(obsfact))

    return observation


def to_fhir_condition(obsfact: ObservationFact, display_codes: dict) -> dict:
    """
    :param obsfact: i2b2 observation fact containing ICD9, ICD10, or SNOMED
                    diagnosis
    :return: https://www.hl7.org/fhir/condition.html
    """
    condition = {
        "resourceType": "Condition",
        "id": str(obsfact.instance_num),
        "subject": fhir.ref_resource("Patient", obsfact.patient_num),
        "encounter": fhir.ref_resource("Encounter", obsfact.encounter_num),
        "meta": {"profile": ["http://hl7.org/fhir/us/core/StructureDefinition/us-core-condition"]},
        "category": [
            make_concept(
                "encounter-diagnosis",
                "http://terminology.hl7.org/CodeSystem/condition-category",
                display="Encounter Diagnosis",
            )
        ],
        "recordedDate": chop_to_date(obsfact.start_date),
        "clinicalStatus": make_concept(
            "active", "http://terminology.hl7.org/CodeSystem/condition-clinical"
        ),
        "verificationStatus": make_concept(
            "unconfirmed", "http://terminology.hl7.org/CodeSystem/condition-ver-status"
        ),
    }

    # Code
    i2b2_sys, i2b2_code = obsfact.concept_cd.split(":")

    if i2b2_sys in ["ICD10", "ICD-10"]:
        i2b2_sys = "http://hl7.org/fhir/sid/icd-10-cm"
    elif i2b2_sys in ["ICD10PROC"]:
        i2b2_sys = "http://hl7.org/fhir/sid/icd-10-pcs"
    elif i2b2_sys in ["ICD9", "ICD-9"]:
        i2b2_sys = "http://hl7.org/fhir/sid/icd-9-cm"
    elif i2b2_sys in ["ICD9PROC"]:
        i2b2_sys = "http://hl7.org/fhir/sid/icd-9-pcs"
    elif i2b2_sys in ["SNOMED", "SNOMED-CT", "SNOMEDCT", "SCT"]:
        i2b2_sys = "http://snomed.info/sct"
    else:
        logging.warning("Condition: unknown system %s", i2b2_sys)
        i2b2_sys = "http://cumulus.smarthealthit.org/i2b2"
        i2b2_code = obsfact.concept_cd
    condition["code"] = make_concept(i2b2_code, i2b2_sys, display_codes=display_codes)

    return condition


def to_fhir_medicationrequest(obsfact: ObservationFact) -> dict:
    """
    :param obsfact: i2b2 observation fact containing medication information
    :return: https://www.hl7.org/fhir/medicationrequest.html
    """
    return {
        "resourceType": "MedicationRequest",
        "id": str(obsfact.instance_num),
        "status": "unknown",
        "intent": "order",
        "medicationCodeableConcept": make_concept(
            obsfact.concept_cd, "http://cumulus.smarthealthit.org/i2b2", display=obsfact.concept_cd
        ),
        "subject": fhir.ref_resource("Patient", obsfact.patient_num),
        "encounter": fhir.ref_resource("Encounter", obsfact.encounter_num),
        "authoredOn": chop_to_date(obsfact.start_date),
    }


###############################################################################
#
# Clinical Notes and NLP
#
###############################################################################


def to_fhir_documentreference(obsfact: ObservationFact) -> dict:
    """
    :param obsfact: i2b2 observation fact containing the I2b2 NOTE as
                    OBSERVATION_BLOB
    :return: https://www.hl7.org/fhir/documentreference.html
    """
    blob = obsfact.observation_blob or ""

    return {
        "resourceType": "DocumentReference",
        "id": str(obsfact.instance_num),
        "subject": fhir.ref_resource("Patient", obsfact.patient_num),
        "context": {
            "encounter": [fhir.ref_resource("Encounter", obsfact.encounter_num)],
            "period": {
                "start": chop_to_date(obsfact.start_date),
                "end": chop_to_date(obsfact.end_date),
            },
        },
        # It would be nice to get a real mapping for the "NOTE:" concept CD types to a real system.
        # But for now, use this custom (and the URL isn't even valid) system to note these i2b2 concepts.
        "type": make_concept(
            obsfact.concept_cd, "http://cumulus.smarthealthit.org/i2b2", obsfact.tval_char
        ),
        "status": "current",
        "content": [
            {
                "attachment": {
                    "contentType": "text/plain",
                    "data": base64.standard_b64encode(blob.encode("utf8")).decode("ascii"),
                }
            },
        ],
    }


###############################################################################
#
# parse i2b2 inputs to FHIR types
#
###############################################################################


def chop_to_date(yyyy_mm_dd: str | None) -> str | None:
    """
    To be less sensitive to how i2b2 datetimes are formatted, chop to just the day/date part.

    :param yyyy_mm_dd: YEAR Month Date
    :return: only the date part.
    """
    if yyyy_mm_dd and isinstance(yyyy_mm_dd, str):
        return yyyy_mm_dd[:10]  # ignore the time portion


def get_observation_value(obsfact: ObservationFact) -> dict:
    """
    Transform an I2B2 observation fact to a valid FHIR Observation.value field

    References:
    https://community.i2b2.org/wiki/display/ServerSideDesign/Value+Columns
    https://www.hl7.org/fhir/observation.html
    http://hl7.org/fhir/R4/datatypes.html#Quantity
    """
    if obsfact.valtype_cd == "T":
        return {
            "valueCodeableConcept": make_concept(
                obsfact.tval_char, "http://cumulus.smarthealthit.org/i2b2"
            )
        }
    elif obsfact.valtype_cd == "B":
        return {
            "valueCodeableConcept": make_concept(
                obsfact.observation_blob, "http://cumulus.smarthealthit.org/i2b2"
            )
        }
    elif obsfact.valtype_cd == "@":  # no value
        return {}
    elif obsfact.valtype_cd != "N":
        # "NLP" will fall into this path (xml in observation blob)
        logging.warning("Observation: unhandled valtype_cd '%s'", obsfact.valtype_cd)
        return {}

    # OK we're a numeric type. This one is slightly trickier. We're going to define a valueQuantity.
    quantity = {
        "value": float(obsfact.nval_num),
        "unit": obsfact.units_cd,
    }

    # Convert unit string to a unit code if possible
    ucum_code = external_mappings.UCUM_CODES.get(obsfact.units_cd)
    if ucum_code:
        quantity["system"] = "http://unitsofmeasure.org"
        quantity["code"] = ucum_code
    elif ucum_code is None:
        logging.warning("Observation: unhandled units_cd '%s'", obsfact.units_cd)

    # Handle comparisons
    comparison = external_mappings.COMPARATOR.get(obsfact.tval_char)
    if comparison:
        quantity["comparator"] = comparison
    elif comparison is None:
        logging.warning("Observation: unhandled tval_char '%s'", obsfact.tval_char)
        return {}

    return {"valueQuantity": quantity}


def make_concept(
    code: str, system: str | None, display: str | None = None, display_codes: dict | None = None
) -> dict:
    """Syntactic sugar to make a codeable concept"""
    coding = {"code": code, "system": system}
    if display:
        coding["display"] = display
    elif display_codes:
        if system in display_codes:
            coding["display"] = display_codes[system].get(code, "Unknown")
    return {"coding": [coding]}
