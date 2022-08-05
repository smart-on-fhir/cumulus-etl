import unittest
import store
from etl_i2b2_ctakes.ctakes import SemType

class Concept:
    def __init__(self, source=None):
        """
        UMLS Unified Medical Language System Concept

        * CUI   Concept Unique Identifier
        * CODE  identified by the vocabulary
        https://www.ncbi.nlm.nih.gov/books/NBK9684/#ch02.sec2.5

        * codingScheme also known as SAB for "source abbreviation" or vocabularoy
        https://www.nlm.nih.gov/research/umls/sourcereleasedocs/index.html

        * TUI Type Unique Identifier (semantic types)
        https://lhncbc.nlm.nih.gov/ii/tools/MetaMap/documentation/SemanticTypesAndGroups.html

        :param source: contains UMLS concept metadata
        """
        self.codingScheme = None
        self.code = None
        self.cui = None
        self.tui = None

        if source: self.from_json(source)

    def from_json(self, source:dict) -> None:
        """
        :param source: contains UMLS Concept source
        """
        self.codingScheme = source.get('codingScheme')
        self.code = source.get('code')
        self.cui = source.get('cui')
        self.tui = source.get('tui')

    def to_json(self) -> dict:
        return {'code':self.code, 'cui':self.cui, 'codingScheme': self.codingScheme,  'tui': self.tui}


class Match:
    def __init__(self, source=None):
        self.begin = None
        self.end = None
        self.text = None
        self.polarity = None
        self.type = None
        self.conceptAttributes = None

        if source: self.from_json(source)

    @staticmethod
    def parse_polarity(polarity) -> bool:
        """
        Polarity means "negation" like "patient denies cough".
        NegEx algorithm polularized by Wendy Chapman et al
        https://www.sciencedirect.com/science/article/pii/S1532046401910299

        :param polarity: typically 0 for positive and -1 for negated
        :return: True/False
        """
        if polarity in [0, '0', True, 'positive']:
            return True
        if polarity in [-1, '-1', False, 'negated']:
            return False

    @staticmethod
    def parse_mention(mention:str) -> SemType:
        if mention == 'IdentifiedAnnotation':
            return SemType.CustomDict
        else:
            return SemType[mention.replace('Mention', '')]

    def from_json(self, source:dict):
        self.begin = source.get('begin')
        self.end = source.get('end')
        self.text = source.get('text')
        self.type = self.parse_mention(source.get('type'))
        self.polarity = self.parse_polarity(source.get('polarity'))
        self.conceptAttributes = list()

        for c in source.get('conceptAttributes'):
            self.conceptAttributes.append(Concept(c))


    def to_json(self):
        _polarity = 0 if self.polarity else -1
        _concepts = [c.to_json() for c in self.conceptAttributes]
        return {'begin': self.begin, 'end': self.end, 'text': self.text,
                'polarity': _polarity,
                'conceptAttributes': _concepts, 'type': self.type.value}


class CtakesJSON:

    def __init__(self, source=None):
        self.mentions = dict()
        if source:
            self.from_json(source)

    def from_json(self, source:dict) -> None:
        for mention, match_list in source.items():
            semtype = Match.parse_mention(mention)

            if semtype not in self.mentions.keys():
                self.mentions[semtype] = list()

            for m in match_list:
                self.mentions[semtype].append(Match(m))

    def to_json(self):
        res = dict()
        for mention, match_list in self.mentions.items():
            match_json = [m.to_json() for m in match_list]

            res[mention.value] = match_json
        return res


class TestCtakesJSON(unittest.TestCase):

    def test(self, example='/your/path/to/ctakes.json'):
        example = '/Users/andy/phi/i2b2/processed/1069/106912947/546a048483c86e7978f607f6c73fbe9f/ctakes.json'

        if store.path_exists(example):
            from_json = store.read(example)
            to_json = CtakesJSON(from_json).to_json()

            self.assertDictEqual(from_json, to_json, 'ctakes json did not match before/after serialization')


if __name__ == '__main__':
    unittest.main()
