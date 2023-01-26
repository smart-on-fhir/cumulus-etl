"""Mock data used by i2b2 test runners"""

from cumulus.loaders.i2b2 import transform


def patient_dim() -> transform.PatientDimension:
    return transform.PatientDimension(
        {
            "PATIENT_NUM": str(12345),
            "BIRTH_DATE": "2005-06-07",
            "DEATH_DATE": "2008-09-10",
            "SEX_CD": "F",
            "RACE_CD": "Black or African American",
            "ZIP_CD": "02115",
        }
    )


def patient() -> dict:
    return transform.to_fhir_patient(patient_dim())


def encounter_dim() -> transform.VisitDimension:
    return transform.VisitDimension(
        {
            "ENCOUNTER_NUM": 67890,
            "PATIENT_NUM": "12345",
            "START_DATE": "2016-01-01T11:44:32+00:00",
            "END_DATE": "2016-01-04T12:45:33+00:00",
            "INOUT_CD": "Inpatient",
            "LENGTH_OF_STAY": 3,
        }
    )


def encounter() -> dict:
    return transform.to_fhir_encounter(encounter_dim())


def condition_dim() -> transform.ObservationFact:
    return transform.ObservationFact(
        {
            "INSTANCE_NUM": "4567",
            "PATIENT_NUM": str(12345),
            "ENCOUNTER_NUM": 67890,
            "CONCEPT_CD": "ICD10:U07.1",  # COVID19 Diagnosis
            "START_DATE": "2016-01-01",
        }
    )


def condition() -> dict:
    return transform.to_fhir_condition(condition_dim())


def documentreference_dim() -> transform.ObservationFact:
    return transform.ObservationFact(
        {
            "INSTANCE_NUM": "345",
            "PATIENT_NUM": str(12345),
            "ENCOUNTER_NUM": 67890,
            "CONCEPT_CD": "NOTE:149798455",  # emergency room type
            "START_DATE": "2016-01-01",
            "OBSERVATION_BLOB": "Chief complaint: fever and chills. Denies cough.",
            "TVAL_CHAR": "Emergency note",
        }
    )


def documentreference() -> dict:
    return transform.to_fhir_documentreference(documentreference_dim())


def observation_dim() -> transform.ObservationFact:
    return transform.ObservationFact(
        {
            "PATIENT_NUM": str(12345),
            "ENCOUNTER_NUM": 67890,
            "CONCEPT_CD": "LAB:1043473617",  # COVID19 PCR Test
            "START_DATE": "2021-01-02",
            "END_DATE": "2021-01-02",
            "VALTYPE_CD": "T",
            "TVAL_CHAR": "Negative",
        }
    )


def observation() -> dict:
    return transform.to_fhir_observation_lab(observation_dim())
