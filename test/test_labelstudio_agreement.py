from enum import Enum
import unittest
import re
from cumulus import common
from ctakesclient.typesystem import Span

EXPORT_DIR = '/Users/andy/chip/cumulus-etl/NoteType/ED_Nov15_341pm'
MIN_JSON = f'{EXPORT_DIR}/labelstudio-dec14-626-min.json'
FULL_JSON = f'{EXPORT_DIR}/labelstudio-dec14-626-full.json'
CTAKES_JSON = f'{EXPORT_DIR}/labelstudio-dec14-626-ctakes.json'
CNLP_JSON = f'{EXPORT_DIR}/labelstudio-ED_Dec20_1044pm-ctakes.pos.cnlp.pos.json'
MERGED_JSON = f'{EXPORT_DIR}/labelstudio-merged.ctakes.pos.cnlp.pos.json'

class Annotator(Enum):
    """
    LabelStudio annotator (reviewer) ID
    """
    andy = 2
    amy = 3
    alon = 6
    ctakes = 7  # mcmurry.andy

class NoteRange(Enum):
    """
    LabelStudio list of ED Notes
    """
    corpus = range(782, 1005)
    amy = range(782, 895)
    andy = range(895, 1006)
    andy_alon = range(979, 1006)
    amy_alon = range(864, 890)

def intersect(span1: Span, span2: Span) -> set:
    """
    :param span1: 1st text Span
    :param span2: 2nd text Span
    :return: set of CHAR positions (convertible to range or Span)
    """
    range1 = range(span1.begin, span1.end)
    range2 = range(span2.begin, span2.end)
    return set(range1).intersection(set(range2))

def overlaps(span1: Span, span2: Span, min_length=2, max_length=20) -> bool:
    """
    :param span1: 1st text Span
    :param span2: 2nd text Span
    :param min_length: MIN length of comparison, default 2 chars
    :param max_length: MAX length of comparison, default 20 chars (or equals)
    :return: true/false the two spans overlap
    """
    shared = intersect(span1, span2)
    if len(shared) == len(range(span1.begin, span1.end)):
        return True
    elif (len(shared) >= min_length) and (len(shared) <= max_length):
        return True
    else:
        return False

def simplify_file_id(file_id: str) -> str:
    """
    @param file_id: labelstudio-file_id.json.optional.extension.json
    @return: simple filename like "file_id.json"
    """
    prefix = re.search('-', file_id).start()  # UUID split in LabelStudio
    suffix = re.search('.json', file_id).start()
    root = file_id[prefix+1:suffix]
    return f'{root}.json'

def reverse_map(key_vals: dict) -> dict:
    return {v: k for k, v in key_vals.items()}

def merge_simple(source: dict, append: dict) -> dict:
    """
    @param source: SOURCE of LabelStudio "note" ID mappings
    @param append: INHERIT LabelStudio "note" ID mappings
    @return:
    """
    merged = {'files': {}, 'annotations': {}}

    for file_id, note_id in source['files'].items():
        merged['files'][file_id] = int(note_id)
        merged['annotations'][int(note_id)] = source['annotations'][int(note_id)]

        append_id = append['files'][file_id]
        for annotator in append['annotations'][append_id]:
            for entry in append['annotations'][append_id][annotator]:
                if annotator not in merged['annotations'][int(note_id)].keys():
                    merged['annotations'][int(note_id)][annotator] = list()
                if entry.get('labels'):
                    merged['annotations'][int(note_id)][annotator].append(entry)
    return merged


def simplify_full(exported_json=FULL_JSON) -> dict:
    """
    LabelStudio outputs contain more info than needed for IAA and term_freq.

    * PHI raw physician note text is removed *

    @param exported_json: file output from LabelStudio
    @return: dict key= note_id
    """
    simple = {'files': {}, 'annotations': {}}
    for entry in common.read_json(exported_json):
        note_id = int(entry.get('id'))
        file_id = simplify_file_id(entry.get('file_upload'))
        simple['files'][file_id] = int(note_id)
        for annot in entry.get('annotations'):
            annotator = Annotator(annot.get('completed_by')).name
            label = None
            for result in annot.get('result'):
                if not label:
                    label = list()
                match = result.get('value')
                label.append(match)

            if note_id not in simple['annotations'].keys():
                simple['annotations'][int(note_id)] = dict()

            simple['annotations'][int(note_id)][annotator] = label
    return simple

def simplify_min(exported_json=MIN_JSON) -> dict:
    """
    LabelStudio outputs contain more info than needed for IAA and term_freq.

    * PHI raw physician note text is removed *

    @param exported_json: file output from LabelStudio
    @return: dict key= note_id
    """
    simple = dict()
    for entry in common.read_json(exported_json):
        note_id = int(entry.get('id'))
        annotator = Annotator(entry.get('annotator')).name
        label = entry.get('label')

        if not simple.get(note_id):
            simple[note_id] = dict()

        simple[note_id][annotator] = label
    return simple

def filter_note_range(simple: dict, note_range) -> dict:
    """
    @param simple: simplified() dict
    @param note_range: range of notes
    @return: dict filtered by note_range
    """
    filtered = {'files': {}, 'annotations': {}}
    for file_id, note_id in simple['files'].items():
        if int(note_id) in note_range:
            foo = simple['annotations'][str(note_id)]
            filtered['annotations'][note_id] = foo
            filtered['files'][file_id] = note_id
    return filtered

def calc_term_freq(simple: dict, annotator) -> dict:
    """
    Calculate the frequency of TERMS highlighted for each LABEL (Cough, Dyspnea, etc).
    @param simple: prepared map of files and annotations
    @param annotator: Reviewer like andy, amy, or alon
    @param note_range: default= all in corpus
    @return: dict key=TERM val= {label, list of chart_id}
    """
    term_freq = dict()
    for note_id, values in simple['annotations'].items():
        if values.get(annotator):
            for annot in values.get(annotator):
                text = annot['text'].upper()
                symptom = annot['labels'][0]
                if len(annot['labels']) > 1:
                    raise Exception(f"note_id = {note_id} \t {values}")

                if text not in term_freq.keys():
                    term_freq[text] = dict()

                if symptom not in term_freq[text].keys():
                    term_freq[text][symptom] = list()

                term_freq[text][symptom].append(note_id)
    return term_freq

def calc_term_symptom_confusion(term_freq: dict) -> dict:
    """
    @param term_freq: output of 'calc_term_freq'
    @return: dict filtered by only confusing TERMs
    """
    confusing = dict()
    for term in term_freq.keys():
        if len(term_freq[term].keys()) > 1:
            confusing[term] = term_freq[term]
    return confusing

def calc_symptom_frequency(term_freq: dict) -> dict:
    symptoms = dict()
    for term in term_freq.keys():
        for label in term_freq[term].keys():
            if label not in symptoms.keys():
                symptoms[label] = dict()
            if term not in symptoms[label].keys():
                symptoms[label][term] = list()
            for note_id in term_freq[term][label]:
                symptoms[label][term].append(note_id)
    tf = dict()
    for label in symptoms.keys():
        for term in symptoms[label].keys():
            if label not in tf.keys():
                tf[label] = dict()
            tf[label][term] = len(symptoms[label][term])
    return tf

def accuracy_mentions(simple: dict, ground_truth_ann: str, reliability_ann: str, note_range):
    """
    Calculate confusion matrix (TP, FP, TN, FN)
    @param simple: prepared map of files and annotations
    @param ground_truth_ann: annotator like andy, amy, or alon
    @param reliability_ann: annotator like andy, amy, or alon (usually alon)
    @param note_range: default= all in corpus
    @param labels_file: default = $FULL_JSON
    @return: dict}
    """
    TP = list()  # True Positive
    FN = list()  # False Negative
    id_list = list()  # list notes compared by both annotators

    for note_id, values in simple['annotations'].items():
        if int(note_id) in note_range:
            if values.get(ground_truth_ann):
                for truth_annot in values[ground_truth_ann]:
                    truth_span = Span(truth_annot['start'], truth_annot['end'])

                    if values.get(reliability_ann):
                        id_list.append(note_id)
                        found = False
                        for confirm in values[reliability_ann]:
                            if not found:
                                compare_span = Span(confirm['start'], confirm['end'])
                                if overlaps(truth_span, compare_span):
                                    if truth_annot['labels'] == confirm['labels']:
                                        found = True
                                        TP.append(confirm)
                        if not found:
                            FN.append(truth_annot)
    return {'TP': TP, 'FN': FN, 'ID': id_list}

def rollup_mentions(simple: dict, annotator, note_range) -> dict:
    """
    @param simple: prepared map of files and annotations
    @param annotator: like andy, amy, or alon
    @param note_range: default= all in corpus
    @return: dict keys=note_id, values=labels
    """
    rollup = dict()

    for note_id, values in simple['annotations'].items():
        if int(note_id) in note_range:
            if values.get(annotator):
                for annot in values[annotator]:
                    if not rollup.get(note_id):
                        rollup[note_id] = list()

                    symptom = annot['labels'][0]

                    if symptom not in rollup[note_id]:
                        rollup[note_id].append(symptom)
    return rollup

def accuracy_prevalence(simple: dict, ground_truth_ann: str, reliability_ann: str, note_range):
    """
    "prevalence" in the population of physician notes, not a true "prevalence" term.
    This is the rollup of counting each symptom only 1x, not multiple times for a single patient.
    @param simple: prepared map of files and annotations
    @param ground_truth_ann: annotator like andy, amy, or alon
    @param reliability_ann: annotator like andy, amy, or alon (usually alon)
    @param note_range: default= all in corpus
    @return: dict
    """
    ground_truth = rollup_mentions(simple, ground_truth_ann, note_range)
    reliability = rollup_mentions(simple, reliability_ann, note_range)

    TP = list()  # True Positive
    FN = list()  # False Negative
    id_list = list()  # list notes compared by both annotators

    for note_id, labels in ground_truth.items():
        for symptom in labels:
            key = {note_id: symptom}
            if note_id not in id_list:
                id_list.append(note_id)

            if reliability.get(note_id):
                if symptom in reliability[note_id]:
                    TP.append(key)
                else:
                    FN.append(key)
    return {'TP': TP, 'FN': FN, 'ID': id_list}

def score_f1(true_pos: list, false_pos: list, false_neg: list) -> dict:
    """
    Score F1 measure with specificity (PPV) and recall (sensitivity).
    F1 deliberately ignores "True Negatives" because TN inflates scoring (AUROC)

    @param true_pos: True Positives (agree on positive symptom)
    @param false_pos: False Positives (reliability said pos, annotator said none)
    @param false_neg: False Negative (annotator said pos, reliability said none)
    @return: dict with keys {'f1', 'precision', 'recall'} vals are %% percent
    """
    precision = len(true_pos) / (len(true_pos) + len(false_pos))
    recall = len(true_pos) / (len(true_pos) + len(false_neg))
    f1 = (2 * precision * recall) / (precision + recall)

    return {'f1': f1, 'precision': precision, 'recall': recall}

def score_f1_mentions(simple: dict, ground_truth_ann, reliability_ann, note_range=NoteRange.corpus.value):
    """
    Score reliability of rater at the level of all symptom *MENTIONS*
    @param simple: prepared map of files and annotations
    @param ground_truth_ann: annotator like andy, amy, or alon
    @param reliability_ann: annotator like andy, amy, or alon (usually alon)
    @param note_range: default= all in corpus
    @return: dict, keys f1, precision, recall and vals= %percent
    """
    ground_truth = accuracy_mentions(simple, ground_truth_ann, reliability_ann, note_range)
    reliability = accuracy_mentions(simple, reliability_ann, ground_truth_ann, note_range)

    true_pos = ground_truth['TP']
    false_neg = ground_truth['FN']
    false_pos = reliability['FN']

    return score_f1(true_pos, false_pos, false_neg)

def score_f1_prevalence(simple: dict, ground_truth_ann, reliability_ann, note_range):
    """
    Score reliability of rater at the level of all symptom *PREVALENCE*
    @param simple: prepared map of files and annotations
    @param ground_truth_ann: annotator like andy, amy, or alon
    @param reliability_ann: annotator like andy, amy, or alon (usually alon)
    @param note_range: default= all in corpus
    @return: dict, keys f1, precision, recall and vals= %percent
    """
    ground_truth = accuracy_prevalence(simple, ground_truth_ann, reliability_ann, note_range)
    reliability = accuracy_prevalence(simple, reliability_ann, ground_truth_ann, note_range)

    true_pos = ground_truth['TP']
    false_neg = ground_truth['FN']
    false_pos = reliability['FN']

    print(f'TP: {len(true_pos)}')
    print(f'FP: {len(false_pos)}')
    print(f'FN: {len(false_neg)}')

    return score_f1(true_pos, false_pos, false_neg)

class TestLabelstudioAgreement(unittest.TestCase):

    def test_merge_simple(self):
        truth = simplify_full(FULL_JSON)
        predicted = simplify_full(CNLP_JSON)
        merged = merge_simple(truth, predicted)
        path = CNLP_JSON
        common.write_json(path, merged, 4)
        print(path)

    def test_filter_note_range(self):
        """
        Test identity transform (no filtering)
        """
        simple = simplify_full(FULL_JSON)
        path = f'{FULL_JSON}.filter_note_range.json'
        common.write_json(path, filter_note_range(simple, NoteRange.corpus.value), 4)
        print(path)
        #identity = common.read_json(path)
        #self.assertDictEqual(simple, identity)

    def test_term_frequency(self):
        simple = common.read_json(MERGED_JSON)
        for annotator in list(Annotator):
            path = f'{MERGED_JSON}.term_freq.{annotator.name}.json'
            common.write_json(path, calc_term_freq(simple, annotator.name), 4)
            print(path)

    def test_calc_symptom_frequency(self):
        simple = common.read_json(MERGED_JSON)
        for annotator in list(Annotator):
            path = f'{MERGED_JSON}.symptom_freq.{annotator.name}.json'
            common.write_json(path, calc_symptom_frequency(calc_term_freq(simple, annotator.name)), 4)
            print(path)

    def test_calc_term_symptom_confusion(self):
        simple = common.read_json(MERGED_JSON)
        for annotator in list(Annotator):
            path = f'{MERGED_JSON}.term_confusion.{annotator.name}.json'
            common.write_json(path, calc_term_symptom_confusion(calc_term_freq(simple, annotator.name)), 4)
            print(path)

    def test_score_f1(self):
        self.write_score_f1(Annotator.alon.name, Annotator.andy.name, NoteRange.andy_alon.value)
        self.write_score_f1(Annotator.andy.name, Annotator.alon.name, NoteRange.andy_alon.value)
        self.write_score_f1(Annotator.amy.name, Annotator.alon.name, NoteRange.amy_alon.value)
        self.write_score_f1(Annotator.alon.name, Annotator.amy.name, NoteRange.amy_alon.value)
        #   ctakes scores
        self.write_score_f1(Annotator.andy.name, Annotator.ctakes.name, NoteRange.andy.value)
        self.write_score_f1(Annotator.amy.name, Annotator.ctakes.name, NoteRange.amy.value)

    def write_score_f1(self, ground_truth_ann, reliability_ann, note_range):
        simple = filter_note_range(common.read_json(MERGED_JSON), note_range)
        f1_mentions = score_f1_mentions(simple, ground_truth_ann, reliability_ann, note_range)
        f1_prevalence = score_f1_prevalence(simple, ground_truth_ann, reliability_ann, note_range)

        path = f'{MERGED_JSON}.f1_mentions.{ground_truth_ann}.{reliability_ann}.json'
        common.write_json(path, f1_mentions, 4)
        print(path)

        path = f'{MERGED_JSON}.f1_prevalence.{ground_truth_ann}.{reliability_ann}.json'
        common.write_json(path, f1_prevalence, 4)
        print(path)

    def test_simplify(self):
        full = simplify_full()
        minimal = simplify_min()

        self.assertDictEqual(full, minimal)

    def test_simplify_ctakes(self):
        full_path = f'{CTAKES_JSON}.simplify.json'
        common.write_json(full_path, simplify_full(CTAKES_JSON), 4)
        print(full_path)

    def test_simplify_full(self):
        full_path = f'{FULL_JSON}.simplify.json'
        common.write_json(full_path, simplify_full(), 4)
        print(full_path)

    def skip_test_simplify_min(self):
        min_path = f'{MIN_JSON}.simplify.json'
        common.write_json(min_path, simplify_min(), 4)
        print(min_path)

if __name__ == '__main__':
    unittest.main()
