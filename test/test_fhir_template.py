"""Tests for loading FHIR templates"""

import unittest
from fhirclient.models.patient import Patient
from fhirclient.models.encounter import Encounter
from fhirclient.models.observation import Observation
from fhirclient.models.condition import Condition
from fhirclient.models.documentreference import DocumentReference

from cumulus import fhir_template


class TestResourcesFhirTemplates(unittest.TestCase):
    """Test case for loading FHIR templates"""

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

    def ignore_test_documentreference(self):
        """
        Confirms fhirclient library is not able to JSON serialize
        * indexed
        * context/encounter
        """
        expected = fhir_template.fhir_documentreference()
        actual = DocumentReference(expected, strict=False).as_json()

        self.assertDictEqual(expected, actual)

    def ignore_test_condition(self):
        """
        "OK" test fails verification because fhirclient __init__ does not
        support
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
        expected = fhir_template.template(
            fhir_template.FHIRTemplate.FHIR_CONDITION_EXAMPLE)
        actual = Condition(jsondict=expected, strict=False).as_json()
        self.assertDictEqual(expected, actual)