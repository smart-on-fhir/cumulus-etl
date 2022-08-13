import unittest
import os
import json
import base64
from enum import Enum

from fhirclient.models.fhirdate import FHIRDate
from fhirclient.models.patient import Patient
from fhirclient.models.encounter import Encounter
from fhirclient.models.observation import Observation
from fhirclient.models.condition import Condition
from fhirclient.models.extension import Extension

import fhir_template
from i2b2 import transform as T



class TestI2b2Transform(unittest.TestCase):

    def test_to_fhir_patient(self):

        pat_i2b2 = T.PatientDimension({
            'PATIENT_NUM': str(12345),
            'BIRTH_DATE': '2005-06-07',
            'DEATH_DATE': '2008-09-10',
            'SEX_CD': 'F',
            'RACE_CD': 'Black or African American',
            'ZIP_CD': '02115'
        })

        subject = T.to_fhir_patient(pat_i2b2)

        # print(json.dumps(pat_fhir.as_json(), indent=4))

        self.assertEqual(str(12345), subject.id)
        self.assertEqual('2005-06-07', subject.birthDate.isostring)
        self.assertEqual('female', subject.gender)
        self.assertEqual('02115', subject.address[0].postalCode)

    def test_to_fhir_encounter(self):

        visit_i2b2 = T.VisitDimension({
            'ENCOUNTER_NUM': 67890,
            'PATIENT_NUM': '12345',
            'START_DATE': '2016-01-01T11:44:32+00:00',
            'END_DATE': '2016-01-04T12:45:33+00:00',
            'INOUT_CD': 'Inpatient',
            'LENGTH_OF_STAY': 3
        })

        encounter = T.to_fhir_encounter(visit_i2b2)
        print(json.dumps(encounter.as_json(), indent=4))

        self.assertEqual(str(67890), encounter.identifier[0].value)
        self.assertEqual(str(12345), encounter.subject.reference)
        self.assertEqual(3, encounter.length.value)

    def test_to_fhir_documentreference(self):

        note_i2b2 = T.ObservationFact({
            'PATIENT_NUM': str(12345),
            'ENCOUNTER_NUM': 67890,
            'CONCEPT_CD': 'NOTE:103933779', # Admission Note Type
            'START_DATE': '2016-01-01',
            'OBSERVATION_BLOB': 'Chief complaint: fever and chills. Denies cough.'
        })

        docref = T.to_fhir_documentreference(note_i2b2)

        print(json.dumps(docref.as_json(), indent=4))



