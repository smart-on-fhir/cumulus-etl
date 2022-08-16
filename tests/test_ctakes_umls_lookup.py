import unittest
from etl import store
from etl.ctakes import CtakesJSON

class TestCtakesJSON(unittest.TestCase):

    def test(self, example='/your/path/to/ctakes.json'):

        if store.path_exists(example):
            from_json = store.read(example)
            reader = CtakesJSON(from_json)

            self.assertDictEqual(from_json, reader.to_json(), 'ctakes json did not match before/after serialization')

            self.assertGreaterEqual(len(reader.list_match()), 1, 'response should have at least one match')
            self.assertGreaterEqual(len(reader.list_match_text()), 1, 'response should have at least one text match')
            self.assertGreaterEqual(len(reader.list_concept()), 1, 'response should have at least one concept')
            self.assertGreaterEqual(len(reader.list_concept_cui()), 1, 'response should have at least one concept CUI')
