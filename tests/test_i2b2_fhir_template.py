"""Tests for loading FHIR templates"""

import unittest
from fhirclient.models.patient import Patient
from fhirclient.models.encounter import Encounter

from cumulus.loaders.i2b2 import fhir_template


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
