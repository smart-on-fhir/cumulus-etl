"""Tests for philter.py"""

import copy

import ddt

from cumulus_etl import deid
from tests.utils import AsyncTestCase


@ddt.ddt
class TestPhilter(AsyncTestCase):
    """Test case for the Philter class and its use by Scrubber"""

    def setUp(self):
        super().setUp()
        self.scrubber = deid.Scrubber(use_philter=True)

    @ddt.data(
        (
            {"resourceType": "Encounter", "type": {"text": "Fever at 123 Main St"}},
            {"resourceType": "Encounter", "type": {"text": "Fever at *** **** **"}},
        ),
        (
            {"resourceType": "Encounter", "class": {"display": "Patient 012-34-5678"}},
            {"resourceType": "Encounter", "class": {"display": "Patient ***-**-****"}},
        ),
    )
    @ddt.unpack
    def test_scrub_resource(self, resource, expected):
        """Verify basic philter resource scrubbing"""
        self.assertTrue(self.scrubber.scrub_resource(resource))
        self.assertEqual(expected, resource)

    def test_can_disable_philter(self):
        """Verify that disabling philter works"""
        self.scrubber = deid.Scrubber()  # disabled by default
        orig = {"resourceType": "Encounter", "class": {"display": "Patient 012-34-5678"}}
        orig_copy = copy.deepcopy(orig)
        self.assertTrue(self.scrubber.scrub_resource(orig_copy))
        self.assertEqual(orig, orig_copy)
