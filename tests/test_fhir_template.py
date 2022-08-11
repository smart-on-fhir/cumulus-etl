import unittest
import os
import json
from enum import Enum
import fhir_template

from fhirclient.models.patient import Patient
from fhirclient.models.encounter import Encounter
from fhirclient.models.observation import Observation
from fhirclient.models.condition import Condition

class TestResourcesFhirTemplates(unittest.TestCase):
    def test_patient(self):
        expected = fhir_template.fhir_patient()
        actual = Patient(expected).as_json()

        self.assertDictEqual(expected, actual)

    def test_encounter(self):
        expected = fhir_template.fhir_encounter()
        actual = Encounter(expected).as_json()

        self.assertDictEqual(expected, actual)

    def test_observation(self):
        expected = fhir_template.fhir_observation()
        actual = Observation(expected).as_json()

        self.assertDictEqual(expected, actual)


    def ignore_test_condition(self):
        """
        "OK" test fails verification because fhirclient __init__ does not support
        * clinicalStatus
        * verificationStatus
        in the same way as the FHIR documentation and examples.
        """
        expected = fhir_template.fhir_condition()
        actual = Condition(expected).as_json()
        self.assertDictEqual(expected, actual)

    def ignore_test_condition_example(self):
        """
        Confirms fhirclient library is not able to JSON serialize
        * clinicalStatus
        * verificationStatus

        https://build.fhir.org/condition-example.json.html
        """
        expected = fhir_template.fhir_template(fhir_template.FHIRTemplate.fhir_condition_example)
        actual = Condition(jsondict=expected, strict=False).as_json()
        self.assertDictEqual(expected, actual)

