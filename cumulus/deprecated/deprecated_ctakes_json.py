from typing import List
from enum import Enum

#######################################################################################################################
#
# UMLS (Unified Medical Language System)
#
#######################################################################################################################
class UmlsTypeMention(Enum):
    """
    Semantic Types in the UMLS (Unified Medical Language System)
    https://lhncbc.nlm.nih.gov/ii/tools/MetaMap/documentation/SemanticTypesAndGroups.html
    Semantic type groupings here:
    """
    DiseaseDisorder = 'DiseaseDisorderMention'
    SignSymptom = 'SignSymptomMention'
    AnatomicalSite = 'AnatomicalSiteMention'
    Medication = 'MedicationMention'
    Procedure = 'ProcedureMention'
    CustomDict = 'IdentifiedAnnotation'

class UmlsConcept:
    def __init__(self, source=None):
        """
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

    def as_json(self) -> dict:
        return {'code':self.code, 'cui':self.cui, 'codingScheme': self.codingScheme,  'tui': self.tui}


#######################################################################################################################
#
# JSON Responses from CTAKES REST Server
#
#######################################################################################################################

class MatchText:
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
        if polarity in [0, 1, '0', '1', True, 'positive']:
            return True
        if polarity in [-1, '-1', False, 'negated']:
            return False

    @staticmethod
    def parse_mention(mention:str) -> UmlsTypeMention:
        if mention == 'IdentifiedAnnotation':
            return UmlsTypeMention.CustomDict
        else:
            return UmlsTypeMention[mention.replace('Mention', '')]

    def from_json(self, source:dict):
        self.begin = source.get('begin')
        self.end = source.get('end')
        self.text = source.get('text')
        self.type = self.parse_mention(source.get('type'))
        self.polarity = self.parse_polarity(source.get('polarity'))
        self.conceptAttributes = list()

        for c in source.get('conceptAttributes'):
            self.conceptAttributes.append(UmlsConcept(c))

    def as_json(self):
        _polarity = 0 if self.polarity else -1
        _concepts = [c.as_json() for c in self.conceptAttributes]
        return {'begin': self.begin, 'end': self.end, 'text': self.text,
                'polarity': _polarity,
                'conceptAttributes': _concepts, 'type': self.type.value}

#######################################################################################################################
#
# Ctakes JSON contain "MatchText" with list of "UmlsConcept"
#
#######################################################################################################################
class CtakesJSON:

    def __init__(self, source=None):
        self.mentions = dict()
        if source:
            self.from_json(source)

    def list_concept(self) -> List[UmlsConcept]:
        concat = list()
        for match in self.list_match():
            concat += match.conceptAttributes
        return concat

    def list_concept_cui(self) -> List[str]:
        return [c.cui for c in self.list_concept()]

    def list_concept_tui(self) -> List[str]:
        return [c.tui for c in self.list_concept()]

    def list_concept_code(self) -> List[str]:
        return [c.code for c in self.list_concept()]

    def list_match(self) -> List[MatchText]:
        concat = list()
        for semtype, matches in self.mentions.items():
            concat += matches
        return concat

    def list_match_text(self) -> List[str]:
        return [m.text for m in self.list_match()]

    def list_sign_symptom(self) -> List[MatchText]:
        return self.mentions[UmlsTypeMention.SignSymptom]

    def list_disease_disorder(self) -> List[MatchText]:
        return self.mentions[UmlsTypeMention.DiseaseDisorder]

    def list_medication(self) -> List[MatchText]:
        return self.mentions[UmlsTypeMention.Medication]

    def list_procedure(self) -> List[MatchText]:
        return self.mentions[UmlsTypeMention.Procedure]

    def list_anatomical_site(self) -> List[MatchText]:
        return self.mentions[UmlsTypeMention.AnatomicalSite]

    def list_identified_annotation(self) -> List[MatchText]:
        return self.mentions[UmlsTypeMention.CustomDict]

    def from_json(self, source: dict) -> None:
        for mention, match_list in source.items():
            semtype = MatchText.parse_mention(mention)

            if semtype not in self.mentions.keys():
                self.mentions[semtype] = list()

            for m in match_list:
                self.mentions[semtype].append(MatchText(m))

    def as_json(self):
        res = dict()
        for mention, match_list in self.mentions.items():
            match_json = [m.as_json() for m in match_list]

            res[mention.value] = match_json
        return res
