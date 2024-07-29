"""Tests for converting data models from i2b2 to FHIR"""

import ddt

from cumulus_etl.loaders.i2b2 import schema, transform
from tests import i2b2_mock_data
from tests.utils import AsyncTestCase


@ddt.ddt
class TestI2b2Transform(AsyncTestCase):
    """Test case for converting from i2b2 to FHIR"""

    def test_to_fhir_patient(self):
        subject = i2b2_mock_data.patient()
        # print(json.dumps(subject, indent=4))

        self.assertEqual(str(12345), subject["id"])
        self.assertEqual("2005-06-07", subject["birthDate"])
        self.assertEqual("female", subject["gender"])
        self.assertEqual("02115", subject["address"][0]["postalCode"])

    @ddt.data(
        ("Black or African American", "race", "urn:oid:2.16.840.1.113883.6.238", "2054-5"),
        ("Hispanic or Latino", "ethnicity", "urn:oid:2.16.840.1.113883.6.238", "2135-2"),
        (
            "Declined to Answer",
            "ethnicity",
            "http://terminology.hl7.org/CodeSystem/v3-NullFlavor",
            "ASKU",
        ),
    )
    @ddt.unpack
    def test_patient_race_vs_ethnicity(self, race_cd, url, system, code):
        i2b2_patient = i2b2_mock_data.patient_dim()
        i2b2_patient.race_cd = race_cd
        patient = transform.to_fhir_patient(i2b2_patient)

        self.assertEqual(
            {
                "url": f"http://hl7.org/fhir/us/core/StructureDefinition/us-core-{url}",
                "extension": [
                    {
                        "url": "ombCategory",
                        "valueCoding": {
                            "system": system,
                            "code": code,
                            "display": race_cd,
                        },
                    },
                ],
            },
            patient["extension"][0],
        )

    def test_to_fhir_encounter(self):
        encounter = i2b2_mock_data.encounter()
        # print(json.dumps(encounter, indent=4))

        self.assertEqual("67890", encounter["id"])
        self.assertEqual("Patient/12345", encounter["subject"]["reference"])
        self.assertEqual("2016-01-01", encounter["period"]["start"])
        self.assertEqual("2016-01-04", encounter["period"]["end"])
        self.assertEqual(3, encounter["length"]["value"])

    def test_to_fhir_condition(self):
        condition = i2b2_mock_data.condition()
        # print(json.dumps(condition, indent=4))

        self.assertEqual("Patient/12345", condition["subject"]["reference"])
        self.assertEqual("Encounter/67890", condition["encounter"]["reference"])
        self.assertEqual("U07.1", condition["code"]["coding"][0]["code"])
        self.assertEqual(
            "http://hl7.org/fhir/sid/icd-10-cm", condition["code"]["coding"][0]["system"]
        )
        self.assertEqual("COVID-19", condition["code"]["coding"][0]["display"])

    def test_to_fhir_documentreference(self):
        docref = i2b2_mock_data.documentreference()
        # print(json.dumps(docref, indent=4))

        self.assertEqual("Patient/12345", docref["subject"]["reference"])
        self.assertEqual(1, len(docref["context"]["encounter"]))
        self.assertEqual("Encounter/67890", docref["context"]["encounter"][0]["reference"])
        self.assertEqual("NOTE:149798455", docref["type"]["coding"][0]["code"])
        self.assertEqual("Emergency note", docref["type"]["coding"][0]["display"])

    def test_to_fhir_observation_lab(self):
        lab_fhir = i2b2_mock_data.observation()
        # print(json.dumps(lab_fhir, indent=4))

        self.assertEqual("Patient/12345", lab_fhir["subject"]["reference"])
        self.assertEqual("Encounter/67890", lab_fhir["encounter"]["reference"])

        self.assertEqual("94500-6", lab_fhir["code"]["coding"][0]["code"])
        self.assertEqual("http://loinc.org", lab_fhir["code"]["coding"][0]["system"])

        self.assertEqual("260385009", lab_fhir["valueCodeableConcept"]["coding"][0]["code"])
        self.assertEqual("Negative", lab_fhir["valueCodeableConcept"]["coding"][0]["display"])

        self.assertEqual("2021-01-02", lab_fhir["effectiveDateTime"])

    def test_to_fhir_observation_lab_case_does_not_matter(self):
        dim = i2b2_mock_data.observation_dim()
        dim.tval_char = "POSitiVE"
        lab_fhir = transform.to_fhir_observation_lab(dim)

        self.assertEqual("10828004", lab_fhir["valueCodeableConcept"]["coding"][0]["code"])
        self.assertEqual("POSitiVE", lab_fhir["valueCodeableConcept"]["coding"][0]["display"])
        self.assertEqual(
            "http://snomed.info/sct", lab_fhir["valueCodeableConcept"]["coding"][0]["system"]
        )

    def test_to_fhir_observation_lab_unknown_tval(self):
        dim = i2b2_mock_data.observation_dim()
        dim.tval_char = "Nope"
        lab_fhir = transform.to_fhir_observation_lab(dim)

        self.assertEqual("Nope", lab_fhir["valueCodeableConcept"]["coding"][0]["code"])
        self.assertEqual("Nope", lab_fhir["valueCodeableConcept"]["coding"][0]["display"])
        self.assertEqual(
            "http://cumulus.smarthealthit.org/i2b2",
            lab_fhir["valueCodeableConcept"]["coding"][0]["system"],
        )

    def test_to_fhir_medicationrequest(self):
        medicationrequest = i2b2_mock_data.medicationrequest()
        # print(json.dumps(medicationrequest, indent=4))

        self.assertEqual(
            {
                "authoredOn": "2021-01-02",
                "encounter": {"reference": "Encounter/67890"},
                "id": "345",
                "intent": "order",
                "medicationCodeableConcept": {
                    "coding": [
                        {
                            "code": "ADMINMED:1234",
                            "display": "ADMINMED:1234",
                            "system": "http://cumulus.smarthealthit.org/i2b2",
                        },
                    ],
                },
                "resourceType": "MedicationRequest",
                "status": "unknown",
                "subject": {"reference": "Patient/12345"},
            },
            medicationrequest,
        )

    def test_to_fhir_observation_vitals(self):
        vitals_dim = schema.ObservationFact(
            {
                "INSTANCE_NUM": 3,
                "PATIENT_NUM": 12345,
                "ENCOUNTER_NUM": 67890,
                "CONCEPT_CD": "VITAL:1234",
                "START_DATE": "2020-10-30T04:03:12",
                "TVAL_CHAR": "Left Leg",
                "VALTYPE_CD": "T",
            }
        )
        vitals = transform.to_fhir_observation_vitals(vitals_dim)
        self.assertEqual(
            {
                "resourceType": "Observation",
                "id": "3",
                "subject": {"reference": "Patient/12345"},
                "encounter": {"reference": "Encounter/67890"},
                "category": [
                    {
                        "coding": [
                            {
                                "code": "vital-signs",
                                "system": "http://terminology.hl7.org/CodeSystem/observation-category",
                            }
                        ]
                    }
                ],
                "code": {
                    "coding": [
                        {"code": "VITAL:1234", "system": "http://cumulus.smarthealthit.org/i2b2"}
                    ]
                },
                "valueCodeableConcept": {
                    "coding": [
                        {"code": "Left Leg", "system": "http://cumulus.smarthealthit.org/i2b2"}
                    ]
                },
                "effectiveDateTime": "2020-10-30",
                "status": "unknown",
            },
            vitals,
        )
