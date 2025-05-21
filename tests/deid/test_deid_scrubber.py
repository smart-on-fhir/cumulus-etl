"""Tests for the scrubber module"""

import tempfile
from unittest import mock

import ddt
from ctakesclient import text2fhir, typesystem

from cumulus_etl import common
from cumulus_etl.deid import Scrubber
from cumulus_etl.deid.codebook import CodebookDB
from tests import i2b2_mock_data, utils

# Used in a few tests - just define once for cleanliness
MASKED_EXTENSION = {
    "extension": [
        {
            "url": "http://hl7.org/fhir/StructureDefinition/data-absent-reason",
            "valueCode": "masked",
        }
    ]
}


@ddt.ddt
class TestScrubber(utils.AsyncTestCase):
    """Test case for the Scrubber class"""

    def test_patient(self):
        """Verify a basic patient (saved ids)"""
        patient = i2b2_mock_data.patient()
        self.assertEqual("12345", patient["id"])

        scrubber = Scrubber()
        self.assertTrue(scrubber.scrub_resource(patient))
        self.assertEqual(patient["id"], scrubber.codebook.fake_id("Patient", "12345"))

    def test_encounter(self):
        """Verify a basic encounter (saved ids)"""
        encounter = i2b2_mock_data.encounter()
        self.assertEqual("Patient/12345", encounter["subject"]["reference"])
        self.assertEqual("67890", encounter["id"])

        scrubber = Scrubber()
        self.assertTrue(scrubber.scrub_resource(encounter))
        self.assertEqual(encounter["id"], scrubber.codebook.fake_id("Encounter", "67890"))
        self.assertEqual(
            encounter["subject"]["reference"],
            f"Patient/{scrubber.codebook.fake_id('Patient', '12345')}",
        )

    def test_condition(self):
        """Verify a basic condition (hashed ids)"""
        condition = i2b2_mock_data.condition()
        self.assertEqual("4567", condition["id"])
        self.assertEqual("Patient/12345", condition["subject"]["reference"])
        self.assertEqual("Encounter/67890", condition["encounter"]["reference"])

        scrubber = Scrubber()
        self.assertTrue(scrubber.scrub_resource(condition))
        self.assertEqual(condition["id"], scrubber.codebook.fake_id("Condition", "4567"))
        self.assertEqual(
            condition["subject"]["reference"],
            f"Patient/{scrubber.codebook.fake_id('Patient', '12345')}",
        )
        self.assertEqual(
            condition["encounter"]["reference"],
            f"Encounter/{scrubber.codebook.fake_id('Encounter', '67890')}",
        )

    def test_diagnosticreport(self):
        """Verify a basic DiagnosticReport has attachments stripped"""
        report = {
            "resourceType": "DiagnosticReport",
            "id": "dr1",
            "presentedForm": [
                {
                    "data": "blarg",
                    "language": "en",
                    "size": 5,
                },
                {
                    "url": "https://example.com/",
                    "contentType": "text/plain",
                },
            ],
        }

        scrubber = Scrubber()
        self.assertTrue(scrubber.scrub_resource(report))
        self.assertEqual(
            report,
            {
                "resourceType": "DiagnosticReport",
                "id": scrubber.codebook.fake_id("DiagnosticReport", "dr1"),
                "presentedForm": [
                    {
                        "_data": MASKED_EXTENSION,
                        "language": "en",
                        "size": 5,
                    },
                    {
                        "_url": MASKED_EXTENSION,
                        "contentType": "text/plain",
                    },
                ],
            },
        )

    def test_documentreference(self):
        """Test DocumentReference, which is interesting because of its list of encounters and attachments"""
        docref = {
            "resourceType": "DocumentReference",
            "id": "345",
            "subject": {"reference": "Patient/12345"},
            "context": {
                "encounter": [{"reference": "Encounter/67890"}],
            },
            "content": [
                {
                    "attachment": {
                        "data": "aGVsbG8gd29ybGQ=",
                        "url": "https://example.com/hello-world",
                    },
                },
                {
                    "attachment": {
                        "data": "xxx",
                        "_data": {
                            "extension": [
                                {
                                    "url": "http://hl7.org/fhir/StructureDefinition/data-absent-reason",
                                    "valueCode": "error",
                                }
                            ],
                        },
                        "url": "https://example.com/hello-world",
                    },
                },
            ],
        }

        scrubber = Scrubber()
        self.assertTrue(scrubber.scrub_resource(docref))
        self.assertEqual(docref["id"], scrubber.codebook.fake_id("DocumentReference", "345"))
        self.assertEqual(
            docref["subject"]["reference"],
            f"Patient/{scrubber.codebook.fake_id('Patient', '12345')}",
        )
        self.assertEqual(
            docref["context"]["encounter"][0]["reference"],
            f"Encounter/{scrubber.codebook.fake_id('Encounter', '67890')}",
        )
        self.assertEqual(
            docref["content"][0]["attachment"],
            {
                "_data": MASKED_EXTENSION,
                "_url": MASKED_EXTENSION,
            },
        )
        self.assertEqual(
            docref["content"][1]["attachment"],
            {
                "_data": {
                    "extension": [
                        {
                            "url": "http://hl7.org/fhir/StructureDefinition/data-absent-reason",
                            "valueCode": "error",  # we left this reason in place
                        }
                    ]
                },
                "_url": MASKED_EXTENSION,
            },
        )

    def test_value_string_is_masked(self):
        """Verify that Observation.*.valueString is masked"""
        obs = {
            "resourceType": "Observation",
            "valueString": "Hello Alice!",
            "component": [
                {
                    "valueString": "Also heyo Bob!",
                },
            ],
        }
        self.assertTrue(Scrubber().scrub_resource(obs))
        self.assertEqual(
            obs,
            {
                "resourceType": "Observation",
                "_valueString": MASKED_EXTENSION,
                "component": [{"_valueString": MASKED_EXTENSION}],
            },
        )

    @ddt.data(
        (None, "Bad Display", True, True),
        (None, None, False, False),
        ("1234", "Good Display", False, False),
        ("1234", None, False, False),
        ("text", "Bad Display", True, False),
        ("text", None, True, False),
        ("0", "Bad Display", True, True),
        ("0", None, False, False),
    )
    @ddt.unpack
    def test_epic_custom_codes_are_stripped(self, code, display, expect_mask, keep_code):
        """
        Verify that urn:oid:1.2.840.1.114350.* is stripped.
        It's a customer extension point that can (and has in the past) contain PHI.
        """
        obs = {
            "resourceType": "Observation",
            "code": {
                "coding": [
                    {
                        "system": "urn:oid:1.2.840.114350.1.2.3.4.5",
                        "code": code,
                        "display": display,
                        "version": "2.0",
                        "userSelected": True,
                    },
                ],
            },
        }
        self.assertTrue(Scrubber().scrub_resource(obs))
        if expect_mask and keep_code:
            values = {"code": code, **MASKED_EXTENSION}
        elif expect_mask:
            values = MASKED_EXTENSION
        else:
            values = {"code": code, "display": display}
        self.assertEqual(
            obs,
            {
                "resourceType": "Observation",
                "code": {
                    "coding": [
                        {
                            "system": "urn:oid:1.2.840.114350.1.2.3.4.5",
                            "version": "2.0",
                            "userSelected": True,
                            **values,
                        },
                    ],
                },
            },
        )

    def test_contained_reference(self):
        """Verify that we leave contained references contained but scrubbed"""
        scrubber = Scrubber()
        condition = i2b2_mock_data.condition()
        condition["contained"] = [
            {
                "resourceType": "Patient",
                "id": "p12",
            }
        ]
        condition["subject"]["reference"] = "#p12"

        self.assertTrue(scrubber.scrub_resource(condition))
        fake_id = "221044b59936243b79da55c551b0c60ec7278733dde4acf65f83468cbd64bd0f"
        self.assertEqual(fake_id, condition["contained"][0]["id"])
        self.assertEqual(f"#{fake_id}", condition["subject"]["reference"])

    def test_empty_resource(self):
        """Confirm we skip malformed empty dictionaries"""
        self.assertFalse(Scrubber().scrub_resource({}))

    def test_cleans_empty_containers(self):
        """Confirm we delete empty lists and dicts"""
        patient = {
            "resourceType": "Patient",
            "active": True,
            "link": [],
            "generalPractitioner": {},
        }
        self.assertTrue(Scrubber().scrub_resource(patient))
        self.assertEqual(patient, {"resourceType": "Patient", "active": True})

    def test_unknown_extension(self):
        """Confirm we strip out unknown extension values (but leaves the URL)"""
        patient = i2b2_mock_data.patient()
        scrubber = Scrubber()

        # Entirely remove extension key if array is empty at end
        patient["extension"] = [{"url": "http://example.org/unknown-extension"}]
        self.assertTrue(scrubber.scrub_resource(patient))
        self.assertNotIn("extension", patient)

        # Just removes the single extension if multiple and some are known
        patient["extension"] = [
            {"url": "http://example.org/unknown-extension"},
            {"url": "http://hl7.org/fhir/StructureDefinition/data-absent-reason"},
        ]
        self.assertTrue(scrubber.scrub_resource(patient))
        self.assertEqual(
            patient["extension"],
            [{"url": "http://hl7.org/fhir/StructureDefinition/data-absent-reason"}],
        )

    def test_unknown_modifier_extension(self):
        """Confirm we skip resources with unknown modifier extensions"""
        patient = i2b2_mock_data.patient()
        scrubber = Scrubber()

        patient["modifierExtension"] = []
        self.assertTrue(scrubber.scrub_resource(patient))

        patient["modifierExtension"] = [{"url": "http://example.org/unknown-extension"}]
        self.assertFalse(scrubber.scrub_resource(patient))

    def test_nlp_extensions_allowed(self):
        """Confirm we that nlp-generated resources are allowed, with their modifier extensions"""
        match = typesystem.MatchText(
            {"begin": 0, "end": 1, "polarity": 0, "text": "f", "type": "SignSymptomMention"}
        )
        observation = text2fhir.nlp_observation("1", "2", "3", match).as_json()

        scrubber = Scrubber()
        self.assertTrue(scrubber.scrub_resource(observation))
        self.assertGreater(len(observation["modifierExtension"]), 0)

    def test_load_and_save(self):
        """Verify that loading from and saving to a file works"""
        # Sanity check that save() doesn't blow up if we call it without a file
        scrubber = Scrubber()
        scrubber.save()

        with tempfile.TemporaryDirectory() as tmpdir:
            # Start with one encounter in db
            db = CodebookDB(tmpdir)
            db.encounter("1")
            db.save()

            # Confirm we loaded that encounter correctly
            scrubber = Scrubber(tmpdir)
            encounter = i2b2_mock_data.encounter()  # patient is 12345
            encounter["id"] = "1"
            self.assertTrue(scrubber.scrub_resource(encounter))
            self.assertEqual(encounter["id"], db.encounter("1"))

            # Save back to disk and confirm that we kept the same IDs
            scrubber.save()
            db2 = CodebookDB(tmpdir)
            self.assertEqual(db.encounter("1"), db2.encounter("1"))
            self.assertEqual(encounter["subject"]["reference"], f"Patient/{db2.patient('12345')}")

            # ensure value errors are handled inside scrub_resource:
            encounter_bad = mock.Mock()
            encounter_bad.items = mock.Mock(side_effect=ValueError(1))
            scrubber.scrub_resource(encounter_bad)

            # make sure that we raise an error on an unexpected cookbook version
            common.write_json(f"{tmpdir}/codebook.json", {"version": ".99"})
            with self.assertRaises(Exception) as context:
                Scrubber(tmpdir)
            self.assertIn(".99", str(context.exception))

    def test_meta_security_cleared(self):
        """Verify that we drop the Meta.security field"""
        scrubber = Scrubber()
        condition = i2b2_mock_data.condition()

        # With another property
        condition["meta"] = {"security": [{"code": "REDACTED"}], "versionId": "a"}
        self.assertTrue(scrubber.scrub_resource(condition))
        self.assertNotIn("security", condition["meta"])
        self.assertEqual("a", condition["meta"]["versionId"])

        # Without another property
        condition["meta"] = {"security": [{"code": "REDACTED"}]}
        self.assertTrue(scrubber.scrub_resource(condition))
        self.assertNotIn("meta", condition)
