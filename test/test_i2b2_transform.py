import unittest
import os
import json
import base64
from datetime import date
from enum import Enum

from fhirclient.models.fhirdate import FHIRDate
from fhirclient.models.patient import Patient
from fhirclient.models.encounter import Encounter
from fhirclient.models.observation import Observation
from fhirclient.models.condition import Condition
from fhirclient.models.extension import Extension
from fhirclient.models.documentreference import DocumentReference

from cumulus import fhir_template
from cumulus.i2b2 import transform as T


class TestI2b2Transform(unittest.TestCase):

    def example_fhir_patient(self) -> Patient:
        pat_i2b2 = T.PatientDimension({
            'PATIENT_NUM': str(12345),
            'BIRTH_DATE': '2005-06-07',
            'DEATH_DATE': '2008-09-10',
            'SEX_CD': 'F',
            'RACE_CD': 'Black or African American',
            'ZIP_CD': '02115'
        })

        return T.to_fhir_patient(pat_i2b2)

    def test_to_fhir_patient(self):
        subject = self.example_fhir_patient()

        # print(json.dumps(pat_fhir.as_json(), indent=4))

        self.assertEqual(str(12345), subject.id)
        self.assertEqual('2005-06-07', subject.birthDate.isostring)
        self.assertEqual('female', subject.gender)
        self.assertEqual('02115', subject.address[0].postalCode)


    def example_fhir_encounter(self) -> Encounter:

        visit_i2b2 = T.VisitDimension({
            'ENCOUNTER_NUM': 67890,
            'PATIENT_NUM': '12345',
            'START_DATE': '2016-01-01T11:44:32+00:00',
            'END_DATE': '2016-01-04T12:45:33+00:00',
            'INOUT_CD': 'Inpatient',
            'LENGTH_OF_STAY': 3
        })

        return T.to_fhir_encounter(visit_i2b2)

    def test_to_fhir_encounter(self):
        encounter = self.example_fhir_encounter()
        # print(json.dumps(encounter.as_json(), indent=4))

        self.assertEqual(str(12345), encounter.subject.reference)
        self.assertEqual(str(67890), encounter.identifier[0].value)
        self.assertEqual('2016-01-01', encounter.period.start.isostring)
        self.assertEqual('2016-01-04', encounter.period.end.isostring)
        self.assertEqual(3, encounter.length.value)

    def example_fhir_condition(self):
        diagnosis = T.ObservationFact({
            'PATIENT_NUM': str(12345),
            'ENCOUNTER_NUM': 67890,
            'CONCEPT_CD': 'ICD10:U07.1', # COVID19 Diagnosis
            'START_DATE': '2016-01-01'
        })

        return T.to_fhir_condition(diagnosis)

    def test_to_fhir_condition(self):
        condition = self.example_fhir_condition()

        # print(json.dumps(condition.as_json(), indent=4))
        self.assertEqual(str(12345), condition.subject.reference)
        self.assertEqual(str(67890), condition.context.reference)
        self.assertEqual(str('U07.1'), condition.code.coding[0].code)
        self.assertEqual(str('http://hl7.org/fhir/sid/icd-10-cm'), condition.code.coding[0].system)

    def example_fhir_documentreference(self) -> DocumentReference:
        note_i2b2 = T.ObservationFact({
            'PATIENT_NUM': str(12345),
            'ENCOUNTER_NUM': 67890,
            'CONCEPT_CD': 'NOTE:103933779',  # Admission Note Type
            'START_DATE': '2016-01-01',
            'OBSERVATION_BLOB': 'Chief complaint: fever and chills. Denies cough.'
        })
        return T.to_fhir_documentreference(note_i2b2)

    def test_to_fhir_documentreference(self):
        docref = self.example_fhir_documentreference()

        # print(json.dumps(docref.as_json(), indent=4))

        self.assertEqual(str(12345), docref.subject.reference)
        self.assertEqual(1, len(docref.context.encounter))
        self.assertEqual(str(67890), docref.context.encounter[0].reference)
        self.assertEqual(str('NOTE:103933779'), docref.type.text)

    def example_fhir_observation_lab(self):
        lab_i2b2 = T.ObservationFact({
            'PATIENT_NUM': str(12345),
            'ENCOUNTER_NUM': 67890,
            'CONCEPT_CD': 'LAB:1043473617',  # COVID19 PCR Test
            'START_DATE': '2021-01-02',
            'END_DATE': '2021-01-02',
            'VALTYPE_CD': 'T',
            'TVAL_CHAR': 'Negative'})

        return T.to_fhir_observation_lab(lab_i2b2)

    def test_to_fhir_observation_lab(self):
        lab_fhir = self.example_fhir_observation_lab()

        # print(json.dumps(lab_i2b2.__dict__, indent=4))
        # print(json.dumps(lab_fhir.as_json(), indent=4))

        self.assertEqual(str(12345), lab_fhir.subject.reference)
        self.assertEqual(str(67890), lab_fhir.context.reference)

        self.assertEqual('94500-6', lab_fhir.code.coding[0].code)
        self.assertEqual('http://loinc.org', lab_fhir.code.coding[0].system)

        self.assertEqual('260385009', lab_fhir.valueCodeableConcept.coding[0].code)
        self.assertEqual('Negative', lab_fhir.valueCodeableConcept.coding[0].display)

        self.assertEqual(FHIRDate('2021-01-02').date, lab_fhir.effectiveDateTime.date)

    def test_parse_fhir_date(self):

        timestamp = '2020-01-02 12:00:00.000'
        timestamp = timestamp[:10]

        self.assertEqual('2020-01-02', FHIRDate(timestamp).isostring)

        timestamp = '2020-01-02 12:00:00.000'

        self.assertEqual('2020-01-02', T.parse_fhir_date(timestamp).isostring)

        timezone = '2020-01-02T16:00:00+00:00'

        self.assertEqual('2020-01-02', T.parse_fhir_date(timezone).isostring)

        datepart = '2020-01-02'

        self.assertEqual('2020-01-02', T.parse_fhir_date(datepart).isostring)


