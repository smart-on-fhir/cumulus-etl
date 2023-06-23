"""Tests for fhir_utils.py"""
import base64

import ddt

from cumulus_etl import fhir
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
        parsed = fhir.unref_resource(full_reference)
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
            fhir.unref_resource({"reference": reference})

    @ddt.data(
        ("Patient", "123", "Patient/123"),
        (None, "#123", "#123"),
    )
    @ddt.unpack
    def test_ref_resource(self, resource_type, resource_id, expected):
        self.assertEqual({"reference": expected}, fhir.ref_resource(resource_type, resource_id))

    @ddt.data(
        ("text/html", "<html><body>He<b>llooooo</b></html>", "Hellooooo"),  # strips html
        (  # strips xhtml
            "application/xhtml+xml",
            '<!DOCTYPE html>\n<html xmlns="http://www.w3.org/1999/xhtml"><html><body>x<b>html!</b></html>',
            "xhtml!",
        ),
        ("text/plain", "What¿up", "What up"),  # strips ¿
    )
    @ddt.unpack
    async def test_docref_note_conversions(self, mimetype, incoming_note, expected_note):
        docref = {
            "content": [
                {
                    "attachment": {
                        "contentType": mimetype,
                        "data": base64.standard_b64encode(incoming_note.encode("utf8")).decode("ascii"),
                    },
                },
            ],
        }
        resulting_note = await fhir.get_docref_note(None, docref)
        self.assertEqual(resulting_note, expected_note)
