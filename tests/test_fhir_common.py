"""Tests for fhir_common.py"""

import ddt

from cumulus_etl import fhir_common
from tests import utils


@ddt.ddt
class TestReferenceHandlers(utils.AsyncTestCase):
    """Tests for the unref_ and ref_ methods"""

    @ddt.data(
        ({"reference": "Patient/123"}, "Patient", "123"),
        ({"reference": "123", "type": "Patient"}, "Patient", "123"),
        ({"reference": "#123"}, None, "#123"),
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
        "123",  # with no type field
        "?/123",
        "http://example.com/Patient/123",
    )
    def test_unref_failures(self, reference):
        with self.assertRaises(ValueError):
            fhir_common.unref_resource({"reference": reference})

    @ddt.data(
        ("Patient", "123", "Patient/123"),
        (None, "#123", "#123"),
    )
    @ddt.unpack
    def test_ref_resource(self, resource_type, resource_id, expected):
        self.assertEqual({"reference": expected}, fhir_common.ref_resource(resource_type, resource_id))
