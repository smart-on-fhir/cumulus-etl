"""Tests for philter.py"""

import unittest
from unittest import mock

import ddt

from cumulus import deid


@ddt.ddt
@mock.patch("cumulus.deid.codebook.secrets.token_hex", new=lambda x: "1234")  # just to not waste entropy
class TestPhilter(unittest.IsolatedAsyncioTestCase):
    """Test case for the Philter class and its use by Scrubber"""

    def setUp(self):
        super().setUp()
        self.scrubber = deid.Scrubber()

    @ddt.data(
        ({"CodeableConcept": {"text": "Fever at 123 Main St"}}, {"CodeableConcept": {"text": "Fever at *** **** **"}}),
        ({"Coding": {"display": "Patient 012-34-5678"}}, {"Coding": {"display": "Patient ***-**-****"}}),
        (
            {"resourceType": "Observation", "valueString": "Born on december 12 2012"},
            {"resourceType": "Observation", "valueString": "Born on ******** ** ****"},
        ),
        (
            {"resourceType": "Observation", "component": [{"valueString": "Contact at foo@bar.com"}]},
            {"resourceType": "Observation", "component": [{"valueString": "Contact at ***@***.***"}]},
        ),
    )
    @ddt.unpack
    def test_scrub_resource(self, resource, expected):
        """Verify basic philter resource scrubbing"""
        self.assertTrue(self.scrubber.scrub_resource(resource))
        self.assertEqual(expected, resource)

    def test_scrub_text(self):
        """Verify that scrub_text() exists and works"""
        self.assertEqual("Hello Mr. *****", self.scrubber.scrub_text("Hello Mr. Jones"))
