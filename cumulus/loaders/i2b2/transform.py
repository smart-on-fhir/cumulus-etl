"""Transformations from i2b2 to FHIR"""

import base64
import logging
from typing import Optional

from cumulus import fhir_common
from cumulus.loaders.i2b2 import external_mappings
from cumulus.loaders.i2b2.schema import PatientDimension, VisitDimension, ObservationFact


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
        "subject": fhir_common.ref_resource("Patient", visit.patient_num),
        "meta": {"profile": ["http://hl7.org/fhir/us/core/StructureDefinition/us-core-encounter"]},
        "status": "unknown",
        "period": {"start": chop_to_date(visit.start_date), "end": chop_to_date(visit.end_date)},
        # Most generic encounter type possible, only included because the 'type' field is required in us-core
        "type": [make_concept("308335008", "http://snomed.info/sct", "Patient encounter procedure")],
    }

    if visit.length_of_stay:  # days
        encounter["length"] = {"unit": "d", "value": visit.length_of_stay and float(visit.length_of_stay)}

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
    :param obsfact: i2b2 observation fact containing the LAB NAME AND VALUE
    :return: https://www.hl7.org/fhir/observation.html
    """
    observation = {
        "resourceType": "Observation",
        "id": str(obsfact.instance_num),
        "subject": fhir_common.ref_resource("Patient", obsfact.patient_num),
        "encounter": fhir_common.ref_resource("Encounter", obsfact.encounter_num),
        "category": [make_concept("laboratory", "http://terminology.hl7.org/CodeSystem/observation-category")],
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
    if obsfact.tval_char in external_mappings.SNOMED_LAB_RESULT:
        lab_result = external_mappings.SNOMED_LAB_RESULT[obsfact.tval_char]
        lab_result_system = "http://snomed.info/sct"
    else:
        lab_result = obsfact.tval_char
        lab_result_system = "http://cumulus.smarthealthit.org/i2b2"
    observation["valueCodeableConcept"] = make_concept(lab_result, lab_result_system, display=obsfact.tval_char)

    return observation


def to_fhir_condition(obsfact: ObservationFact) -> dict:
    """
    :param obsfact: i2b2 observation fact containing ICD9, ICD10, or SNOMED
                    diagnosis
    :return: https://www.hl7.org/fhir/condition.html
    """
    condition = {
        "resourceType": "Condition",
        "id": str(obsfact.instance_num),
        "subject": fhir_common.ref_resource("Patient", obsfact.patient_num),
        "encounter": fhir_common.ref_resource("Encounter", obsfact.encounter_num),
        "meta": {"profile": ["http://hl7.org/fhir/us/core/StructureDefinition/us-core-condition"]},
        "category": [
            make_concept(
                "encounter-diagnosis",
                "http://terminology.hl7.org/CodeSystem/condition-category",
                display="Encounter Diagnosis",
            )
        ],
        "recordedDate": chop_to_date(obsfact.start_date),
        "clinicalStatus": make_concept("active", "http://terminology.hl7.org/CodeSystem/condition-clinical"),
        "verificationStatus": make_concept("unconfirmed", "http://terminology.hl7.org/CodeSystem/condition-ver-status"),
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

    condition["code"] = make_concept(i2b2_code, i2b2_sys)

    return condition


###############################################################################
#
# Physician Notes and NLP
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
        "subject": fhir_common.ref_resource("Patient", obsfact.patient_num),
        "context": {
            "encounter": [fhir_common.ref_resource("Encounter", obsfact.encounter_num)],
            "period": {"start": chop_to_date(obsfact.start_date), "end": chop_to_date(obsfact.end_date)},
        },
        # It would be nice to get a real mapping for the "NOTE:" concept CD types to a real system.
        # But for now, use this custom (and the URL isn't even valid) system to note these i2b2 concepts.
        "type": make_concept(obsfact.concept_cd, "http://cumulus.smarthealthit.org/i2b2", obsfact.tval_char),
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


def chop_to_date(yyyy_mm_dd: Optional[str]) -> Optional[str]:
    """
    To be less sensitive to how i2b2 datetimes are formatted, chop to just the day/date part.

    :param yyyy_mm_dd: YEAR Month Date
    :return: only the date part.
    """
    if yyyy_mm_dd and isinstance(yyyy_mm_dd, str):
        return yyyy_mm_dd[:10]  # ignore the time portion


def make_concept(code: str, system: Optional[str], display: str = None) -> dict:
    """Syntactic sugar to make a codeable concept"""
    return {"coding": [{"code": code, "system": system, "display": display}]}
