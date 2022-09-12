import unittest
from cumulus import common
from cumulus import fhir_ctakes


class TestNlpExtension(unittest.TestCase):

    def test(self):
        expected = {
            "extension": [
                {"url": "begin", "valueInteger": 0},
                {"url": "end", "valueInteger": 7}
            ],
            "url": "http://fhir-registry.smarthealthit.org/StructureDefinition/nlp-text-position",
        }

        actual = fhir_ctakes.ext_text_position(0, 7)

        common.print_fhir(actual)

        self.assertDictEqual(expected, actual.as_json())





