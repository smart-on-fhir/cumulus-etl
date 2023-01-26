"""Tests for fhir_common.py"""

import unittest

import ddt

from cumulus import fhir_common


@ddt.ddt
class TestReferenceHandlers(unittest.TestCase):
    """Tests for the unref_ and ref_ methods"""

    @ddt.data(
        ({"reference": "Patient/123"}, "Patient", "123"),
        ({"reference": "123", "type": "Patient"}, "Patient", "123"),
        # Synthea style reference
        ({"reference": "Patient?identifier=http://example.com|123"}, "Patient", "identifier=http://example.com|123"),
    )
    @ddt.unpack
    def test_unref_successes(self, full_reference, expected_type, expected_id):
        parsed = fhir_common.unref_resource(full_reference)
        self.assertEqual((expected_type, expected_id), parsed)

    @ddt.data(
        None,
        "",
        "#Contained/123",
        "123",  # with no type field
        "?/123",
        "http://example.com/Patient/123",
    )
    def test_unref_failures(self, reference):
        with self.assertRaises(ValueError):
            fhir_common.unref_resource({"reference": reference})

    def test_ref_resource(self):
        self.assertEqual({"reference": "Patient/123"}, fhir_common.ref_resource("Patient", "123"))
