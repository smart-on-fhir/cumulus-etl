import os
import json
from cumulus import common, store
from ctakesclient.typesystem import Polarity, MatchText, CtakesJSON, UmlsTypeMention

COVID_SYMPTOMS_BSV = os.path.join(os.getcwd(), '../resources/covid_symptoms.bsv')

#######################################################################################################################
#
# Helper Functions
#
#######################################################################################################################
def merge_cohort(filepath) -> list:
    if not os.path.exists(filepath):
        raise Exception(f'not found! {filepath}')

    cohort = store.read_json(filepath).get('cohort')

    print(f'{filepath}')
    print(f'cohort list # {len(cohort)}')
    print(f'cohort set  # {len(set(cohort))}')

    contents = list()
    for f in set(cohort):
        contents.append(store.read_json(f))

    return contents


#######################################################################################################################
#
# LabelStudio : Document Annotation
#
#######################################################################################################################

class LabelStudio:
    def __init__(self, physician_note:str, response_ctakes, filter_cui=None, filter_semtype=UmlsTypeMention.SignSymptom.value):
        """
        LabelStudio document annotation.
        https://labelstud.io/guide/tasks.html#Basic-Label-Studio-JSON-format

        Physician note and ctakes JSON data can be very large quickly: the design of this class optionally allows for both TXT/JSON at any time.
        Use load_lazy() when ready to process.

        :param physician_note: text of physician note or /path/to/physician.txt
        :param response_ctakes: JSON result from cTAKES or /path/to/ctakes.json
        :param filter_cui: {cui:text} to select concepts for document level annotation
        :param filter_semtype: UMLS semantic type to filter by (select for)
        """
        self.note_text = physician_note
        self.note_file = None
        self.response_ctakes = response_ctakes
        self.filter_cui = filter_cui
        self.filter_semtype = filter_semtype
        self.model_version = 'ctakes-covid'
        self.result = list()

        # Physician note can be either raw text or path to a file
        if physician_note and (5 < len(physician_note) < 255):
            if os.path.exists(physician_note):
                with open(physician_note, 'r') as f:
                    self.note_text = f.read()
                    self.note_file = physician_note

        # Response from Ctakes can be either typed (CtakesJSON), dict or saved file.
        if response_ctakes and isinstance(response_ctakes, str):
            if os.path.exists(response_ctakes):
                with open(response_ctakes, 'r') as f:
                    self.response_ctakes = CtakesJSON(json.load(f))
        elif response_ctakes and isinstance(response_ctakes, dict):
            self.response_ctakes = CtakesJSON(response_ctakes)

    def load_lazy(self, polarity=Polarity.pos):
        """
        :param ctakes_said: JSON result from cTAKES
        :param cui_map: {cui:text} to select concepts for document level annotation
        :param umls_type: UMLS semantic type to filter by (select for)
        :param polarity: default POSITIVE mentions only.
        """
        whole_doc = set()

        if self.response_ctakes:
            for match in self.response_ctakes.list_match(polarity):
                for concept in match.conceptAttributes:
                    if concept.cui in self.filter_cui.keys():
                        self.add_match(match, self.filter_cui[concept.cui])
                        whole_doc.add(self.filter_cui[concept.cui])

            self.add_concept(whole_doc)

    def add_match(self, match: MatchText, labels):
        ner_spans = {'id': f'ss{len(self.result)}',
                     'from_name': 'label',
                     'to_name': 'text',
                     'type': 'labels',
                     'value': {
                        'start': match.begin,
                        'end': match.end,
                        'score': 1.0,
                        'text': match.text,
                        'labels': [labels]}}
        self.result.append(ner_spans)

    def add_concept(self, labels:set):
        whole_doc = {'id': f'ss{len(self.result)}',
                     'from_name': 'symptoms',
                     'to_name': 'text',
                     'type': 'choices',
                     'value': {'choices': [ss_name for ss_name in labels]}}

        self.result.append(whole_doc)

    def as_json(self):
        return {'data': {'text': self.note_text, 'file': self.note_file},
                'predictions': [{'model_version': self.model_version, 'result': self.result}]}


