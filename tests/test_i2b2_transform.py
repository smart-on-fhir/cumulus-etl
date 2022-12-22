"""Tests for converting data models from i2b2 to FHIR"""

import unittest

from fhirclient.models.condition import Condition
from fhirclient.models.fhirdate import FHIRDate
from fhirclient.models.patient import Patient
from fhirclient.models.encounter import Encounter
from fhirclient.models.documentreference import DocumentReference
from fhirclient.models.observation import Observation

from cumulus import fhir_common
from cumulus.loaders.i2b2 import transform 
from tests import i2b2_mock_data


class TestI2b2Transform(unittest.TestCase):
    """Test case for converting from i2b2 to FHIR"""

    # Pylint doesn't like subscripting some lists in our created objects, not sure why yet.
    # pylint: disable=unsubscriptable-object

    def test_to_fhir_patient(self):
        subject = i2b2_mock_data.patient()

        # print(json.dumps(pat_fhir.as_json(), indent=4))

        self.assertEqual(str(12345), subject.id)
        self.assertEqual('2005-06-07', subject.birthDate.isostring)
        self.assertEqual('female', subject.gender)
        # pylint: disable-next=unsubscriptable-object
        self.assertEqual('02115', subject.address[0].postalCode)

    def test_to_fhir_encounter(self):
        encounter = i2b2_mock_data.encounter()
        # print(json.dumps(encounter.as_json(), indent=4))

        self.assertEqual('67890', encounter.id)
        self.assertEqual('Patient/12345', encounter.subject.reference)
        self.assertEqual('2016-01-01', encounter.period.start.isostring)
        self.assertEqual('2016-01-04', encounter.period.end.isostring)
        self.assertEqual(3, encounter.length.value)

    def test_to_fhir_condition(self):
        condition = i2b2_mock_data.condition()

        # print(json.dumps(condition.as_json(), indent=4))
        self.assertEqual('Patient/12345', condition.subject.reference)
        self.assertEqual('Encounter/67890', condition.encounter.reference)
        self.assertEqual(str('U07.1'), condition.code.coding[0].code)
        self.assertEqual(str('http://hl7.org/fhir/sid/icd-10-cm'),
                         condition.code.coding[0].system)

    def test_to_fhir_documentreference(self):
        docref = i2b2_mock_data.documentreference()

        # print(json.dumps(docref.as_json(), indent=4))

        self.assertEqual('Patient/12345', docref.subject.reference)
        self.assertEqual(1, len(docref.context.encounter))
        self.assertEqual('Encounter/67890', docref.context.encounter[0].reference)
        self.assertEqual(str('NOTE:103933779'), docref.type.text)

    def test_to_fhir_observation_lab(self):
        lab_fhir = i2b2_mock_data.observation()

        # print(json.dumps(lab_i2b2.__dict__, indent=4))
        # print(json.dumps(lab_fhir.as_json(), indent=4))

        self.assertEqual('Patient/12345', lab_fhir.subject.reference)
        self.assertEqual('Encounter/67890', lab_fhir.encounter.reference)

        self.assertEqual('94500-6', lab_fhir.code.coding[0].code)
        self.assertEqual('http://loinc.org', lab_fhir.code.coding[0].system)

        self.assertEqual('260385009',
                         lab_fhir.valueCodeableConcept.coding[0].code)
        self.assertEqual('Negative',
                         lab_fhir.valueCodeableConcept.coding[0].display)

        self.assertEqual(
            FHIRDate('2021-01-02').date, lab_fhir.effectiveDateTime.date)

    def test_parse_fhir_date(self):

        timestamp = '2020-01-02 12:00:00.000'
        timestamp = timestamp[:10]

        self.assertEqual('2020-01-02', FHIRDate(timestamp).isostring)

        timestamp = '2020-01-02 12:00:00.000'

        self.assertEqual('2020-01-02', fhir_common.parse_fhir_date(timestamp).isostring)

        timezone = '2020-01-02T16:00:00+00:00'

        self.assertEqual('2020-01-02', fhir_common.parse_fhir_date(timezone).isostring)

        datepart = '2020-01-02'

        self.assertEqual('2020-01-02', fhir_common.parse_fhir_date(datepart).isostring)
