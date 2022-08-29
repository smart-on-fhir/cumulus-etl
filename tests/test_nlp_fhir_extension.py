import unittest
from cumulus import common
from cumulus.deprecated import ctakes_fhir

class TestNlpExtension(unittest.TestCase):

    def test(self):
        expected = {"extension": [{"url": "http://fhir-registry.smarthealthit.org/StructureDefinition/nlp-text-position",
                                   "extension": [
                                      {"url": "begin", "valueInteger": 0},
                                      {"url": "end","valueInteger": 7}]}]}

        actual = ctakes_fhir.ext_text_position(0, 7)

        common.print_fhir(actual)



        self.assertDictEqual(expected, actual)





