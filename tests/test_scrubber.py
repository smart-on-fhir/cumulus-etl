"""Tests for the scrubber module"""

import os
import tempfile
import unittest
from unittest import mock

from ctakesclient import text2fhir, typesystem
from fhirclient.models.extension import Extension

from cumulus.deid import Scrubber
from cumulus.deid.codebook import CodebookDB
from tests.test_i2b2_transform import ExampleResources


@mock.patch('cumulus.deid.codebook.secrets.token_bytes', new=lambda x: b'1234')  # just to not waste entropy
class TestScrubber(unittest.TestCase):
    """Test case for the Scrubber class"""

    def test_patient(self):
        """Verify a basic patient (saved ids)"""
        patient = ExampleResources.patient()
        self.assertEqual('12345', patient.id)

        scrubber = Scrubber()
        self.assertTrue(scrubber.scrub_resource(patient))
        self.assertEqual(patient.id, scrubber.codebook.fake_id('Patient', '12345'))

    def test_encounter(self):
        """Verify a basic encounter (saved ids)"""
        encounter = ExampleResources.encounter()
        self.assertEqual('Patient/12345', encounter.subject.reference)
        self.assertEqual('67890', encounter.id)

        scrubber = Scrubber()
        self.assertTrue(scrubber.scrub_resource(encounter))
        self.assertEqual(encounter.id, scrubber.codebook.fake_id('Encounter', '67890'))
        self.assertEqual(encounter.subject.reference, f"Patient/{scrubber.codebook.fake_id('Patient', '12345')}")

    def test_condition(self):
        """Verify a basic condition (hashed ids)"""
        condition = ExampleResources.condition()
        self.assertEqual('4567', condition.id)
        self.assertEqual('Patient/12345', condition.subject.reference)
        self.assertEqual('Encounter/67890', condition.encounter.reference)

        scrubber = Scrubber()
        self.assertTrue(scrubber.scrub_resource(condition))
        self.assertEqual(condition.id, scrubber.codebook.fake_id('Condition', '4567'))
        self.assertEqual(condition.subject.reference, f"Patient/{scrubber.codebook.fake_id('Patient', '12345')}")
        self.assertEqual(condition.encounter.reference, f"Encounter/{scrubber.codebook.fake_id('Encounter', '67890')}")

    def test_documentreference(self):
        """Test DocumentReference, which is interesting because of its list of encounters and attachments"""
        docref = ExampleResources.documentreference()
        self.assertEqual('345', docref.id)
        self.assertEqual('Patient/12345', docref.subject.reference)
        self.assertEqual(1, len(docref.context.encounter))
        self.assertEqual('Encounter/67890', docref.context.encounter[0].reference)
        self.assertEqual(1, len(docref.content))
        self.assertIsNotNone(docref.content[0].attachment.data)

        scrubber = Scrubber()
        self.assertTrue(scrubber.scrub_resource(docref))
        self.assertEqual(docref.id, scrubber.codebook.fake_id('DocumentReference', '345'))
        self.assertEqual(docref.subject.reference, f"Patient/{scrubber.codebook.fake_id('Patient', '12345')}")
        self.assertEqual(docref.context.encounter[0].reference,
                         f"Encounter/{scrubber.codebook.fake_id('Encounter', '67890')}")
        self.assertIsNone(docref.content[0].attachment.data)

    def test_unknown_modifier_extension(self):
        """Confirm we skip resources with unknown modifier extensions"""
        patient = ExampleResources.patient()
        scrubber = Scrubber()

        patient.modifierExtension = []
        self.assertTrue(scrubber.scrub_resource(patient))

        patient.modifierExtension = [Extension({'url': 'http://example.org/unknown-extension'})]
        self.assertFalse(scrubber.scrub_resource(patient))

    def test_nlp_extensions_allowed(self):
        """Confirm we that nlp-generated resources are allowed, with their modifier extensions"""
        match = typesystem.MatchText({'begin': 0, 'end': 1, 'polarity': 0, 'text': 'f', 'type': 'SignSymptomMention'})
        observation = text2fhir.nlp_observation('1', '2', '3', match)

        scrubber = Scrubber()
        self.assertTrue(scrubber.scrub_resource(observation))
        self.assertGreater(len(observation.modifierExtension), 0)

    def test_load_and_save(self):
        """Verify that loading from and saving to a file works"""
        # Sanity check that save() doesn't blow up if we call it without a file
        scrubber = Scrubber()
        scrubber.save()

        with tempfile.TemporaryDirectory() as tmpdir:
            path = os.path.join(tmpdir, 'cb.json')

            # Start with one encounter in db
            db = CodebookDB()
            db.encounter('1')
            db.save(path)

            # Confirm we loaded that encounter correctly
            scrubber = Scrubber(path)
            encounter = ExampleResources.encounter()  # patient is 12345
            encounter.id = '1'
            self.assertTrue(scrubber.scrub_resource(encounter))
            self.assertEqual(encounter.id, db.encounter('1'))

            # Save back to disk and confirm that we kept the same IDs
            scrubber.save()
            db2 = CodebookDB(path)
            self.assertEqual(db.encounter('1'), db2.encounter('1'))
            self.assertEqual(encounter.subject.reference, f"Patient/{db2.patient('12345')}")
