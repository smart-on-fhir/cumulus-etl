"""Tests for philter.py"""

import ddt

from cumulus import deid
from tests.utils import AsyncTestCase


@ddt.ddt
class TestPhilter(AsyncTestCase):
    """Test case for the Philter class and its use by Scrubber"""

    def setUp(self):
        super().setUp()
        self.scrubber = deid.Scrubber(use_philter=True)

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

    def test_can_disable_philter(self):
        """Verify that disabling philter works"""
        self.scrubber = deid.Scrubber()  # disabled by default
        self.assertEqual("Hello Mr. Jones", self.scrubber.scrub_text("Hello Mr. Jones"))
