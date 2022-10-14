"""Tests for the API-level Codebook class"""
import unittest
from uuid import UUID
from cumulus.common import print_json
from cumulus.codebook import Codebook
from .test_i2b2_transform import TestI2b2Transform


class TestCodebookFHIR(unittest.TestCase):
    """Test case for the Codebook class"""

    def test_patient(self):
        patient = TestI2b2Transform().example_fhir_patient()

        self.assertEqual('12345', patient.id)

        codebook = Codebook()
        #
        self.assertEqual(0, len(codebook.db.mrn.keys()),
                         'codebook should be empty')

        print_json(patient)

        patient = codebook.fhir_patient(patient)

        self.assertTrue('12345' in codebook.db.mrn.keys(),
                        'CodebookDB entry should be cached')
        self.assertEqual(patient.id, codebook.db.patient('12345')['deid'])

        self.assertNotEqual(
            '12345', patient.id,
            'mrn should not still be in the transformed patient object')
        self.assertEqual(patient.id, patient.identifier[0].value,
                         'in this example, MRN is also FHIR Resource ID')

        # Attempt to cast patient identifier to UUID
        UUID(patient.id)
        UUID(patient.identifier[0].value)

    def test_encounter(self):
        encounter = TestI2b2Transform().example_fhir_encounter()

        self.assertEqual('Patient/12345', encounter.subject.reference)
        self.assertEqual('67890', str(encounter.id))

        mrn = encounter.subject.reference.split('/')[-1]
        visit = encounter.id

        codebook = Codebook()
        #
        encounter = codebook.fhir_encounter(encounter)

        self.assertEqual(
            '2016-01-01',
            codebook.db.mrn[mrn]['encounter'][visit]['period_start'],
            'codebook period_start did not match')

        self.assertTrue(mrn in codebook.db.mrn.keys())
        self.assertEqual(encounter.id,
                         codebook.db.encounter('12345', '67890')['deid'])

    def test_condition(self):
        condition = TestI2b2Transform().example_fhir_condition()

        print_json(condition)

        self.assertEqual('Patient/12345', condition.subject.reference)
        self.assertEqual('Encounter/67890', condition.encounter.reference)

        mrn = condition.subject.reference.split('/')[-1]
        visit = condition.encounter.reference.split('/')[-1]

        codebook = Codebook()
        #
        condition = codebook.fhir_condition(condition)

        self.assertEqual(condition.subject.reference, f"Patient/{codebook.db.patient(mrn)['deid']}")
        self.assertEqual(condition.encounter.reference, f"Encounter/{codebook.db.encounter(mrn, visit)['deid']}")

    def test_observation(self):
        observation = TestI2b2Transform().example_fhir_observation_lab()

        print_json(observation)

        self.assertEqual('Patient/12345', observation.subject.reference)
        self.assertEqual('Encounter/67890', observation.encounter.reference)

        mrn = observation.subject.reference.split('/')[-1]
        visit = observation.encounter.reference.split('/')[-1]

        codebook = Codebook()
        #
        observation = codebook.fhir_observation(observation)

        self.assertEqual(observation.subject.reference, f"Patient/{codebook.db.patient(mrn)['deid']}")
        self.assertEqual(observation.encounter.reference, f"Encounter/{codebook.db.encounter(mrn, visit)['deid']}")

    def test_documentreference(self):
        docref = TestI2b2Transform().example_fhir_documentreference()

        print_json(docref)

        self.assertEqual('Patient/12345', docref.subject.reference)
        self.assertEqual(1, len(docref.context.encounter))
        self.assertEqual('Encounter/67890', docref.context.encounter[0].reference)

        mrn = docref.subject.reference.split('/')[-1]
        visit = docref.context.encounter[0].reference.split('/')[-1]

        codebook = Codebook()
        #
        docref = codebook.fhir_documentreference(docref)

        # TODO: docref date?
        # self.assertEqual('2016-01-01', docref.context.encounter)

        self.assertEqual(docref.subject.reference, f"Patient/{codebook.db.patient(mrn)['deid']}")
        self.assertEqual(docref.context.encounter[0].reference,
                         f"Encounter/{codebook.db.encounter(mrn, visit)['deid']}")

    def test_missing_db_file(self):
        """Ensure we gracefully handle a saved db file that doesn't exist yet"""
        codebook = Codebook('/missing-codebook-file.json')
        self.assertEqual({}, codebook.db.mrn)
