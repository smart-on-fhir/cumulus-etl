import unittest
import json
from uuid import UUID
from cumulus.common import print_fhir
from cumulus.codebook import Codebook, CodebookDB
from tests.test_i2b2_transform import TestI2b2Transform

class TestCodebookFHIR(unittest.TestCase):

    def test_patient(self):
        patient = TestI2b2Transform().example_fhir_patient()

        self.assertEqual('12345', patient.id)

        codebook = Codebook()
        #
        self.assertEqual(0, len(codebook.db.mrn.keys()), 'codebook should be empty')

        print_fhir(patient)

        patient = codebook.fhir_patient(patient)

        self.assertTrue('12345' in codebook.db.mrn.keys(), 'CodebookDB entry should be cached')
        self.assertEqual(patient.id, codebook.db.patient('12345')['deid'])

        self.assertNotEqual('12345', patient.id, 'mrn should not still be in the transformed patient object')
        self.assertEqual(patient.id, patient.identifier[0].value, 'in this example, MRN is also FHIR Resource ID')

        # Attempt to cast patient identifier to UUID
        UUID(patient.id)
        UUID(patient.identifier[0].value)

    def test_encounter(self):
        encounter = TestI2b2Transform().example_fhir_encounter()

        self.assertEqual('12345', encounter.subject.reference)
        self.assertEqual('67890', str(encounter.id))

        mrn = encounter.subject.reference
        visit = encounter.id

        codebook = Codebook()
        #
        encounter = codebook.fhir_encounter(encounter)

        self.assertEqual('2016-01-01',
                         codebook.db.mrn[mrn]['encounter'][visit]['period_start'],
                         'codebook period_start did not match')

        self.assertTrue(mrn in codebook.db.mrn.keys())
        self.assertEqual(encounter.id, codebook.db.encounter('12345', '67890')['deid'])

    def test_condition(self):
        condition = TestI2b2Transform().example_fhir_condition()

        print_fhir(condition)

        self.assertEqual('12345', condition.subject.reference)
        self.assertEqual('67890', condition.context.reference)

        mrn = condition.subject.reference
        visit = condition.context.reference

        codebook = Codebook()
        #
        condition = codebook.fhir_condition(condition)

        self.assertEqual(condition.subject.reference, codebook.db.patient(mrn)['deid'])
        self.assertEqual(condition.context.reference, codebook.db.encounter(mrn, visit)['deid'])

        print_fhir(condition)

        UUID(condition.id)

    def test_observation(self):
        observation = TestI2b2Transform().example_fhir_observation_lab()

        print_fhir(observation)

        self.assertEqual('12345', observation.subject.reference)
        self.assertEqual('67890', observation.context.reference)

        mrn = observation.subject.reference
        visit = observation.context.reference

        codebook = Codebook()
        #
        observation = codebook.fhir_observation(observation)

        self.assertEqual(observation.subject.reference, codebook.db.patient(mrn)['deid'])
        self.assertEqual(observation.context.reference, codebook.db.encounter(mrn, visit)['deid'])

        print_fhir(observation)

        UUID(observation.id)

    def test_documentreference(self):
        docref = TestI2b2Transform().example_fhir_documentreference()

        print_fhir(docref)

        self.assertEqual('12345', docref.subject.reference)
        self.assertEqual(1, len(docref.context.encounter))
        self.assertEqual('67890', docref.context.encounter[0].reference)

        mrn = docref.subject.reference
        visit = docref.context.encounter[0].reference

        codebook = Codebook()
        #
        docref = codebook.fhir_documentreference(docref)

        # self.assertEqual('2016-01-01', docref.context.encounter) TODO: docref date?

        self.assertEqual(docref.subject.reference, codebook.db.patient(mrn)['deid'])
        self.assertEqual(docref.context.encounter[0].reference, codebook.db.encounter(mrn, visit)['deid'])

        print_fhir(docref)

        UUID(docref.id)

    def test_missing_db_file(self):
        """Ensure we gracefully handle a saved db file that doesn't exist yet"""
        codebook = Codebook('/missing-codebook-file.json')
        self.assertEqual({}, codebook.db.mrn)
