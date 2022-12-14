from enum import Enum
import unittest
from cumulus import common
from ctakesclient.typesystem import Span

EXPORTED_JSON = '/Users/andy/Downloads/labelstudio-dec6-min.json'

class Annotator(Enum):
    """
    LabelStudio annotator (reviewer) ID
    """
    andy = 2
    amy = 3
    alon = 6

class NoteRange(Enum):
    """
    LabelStudio list of ED Notes
    """
    corpus = range(782, 1005)
    andy_alon = range(979, 1005)
    amy_alon = range(0, 0)

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

def simplify(exported_json=EXPORTED_JSON) -> dict:
    """
    LabelStudio outputs contain more info than needed for IAA and term_freq.

    * PHI raw physician note text is removed *

    @param exported_json: file output from LabelStudio
    @return: dict key= note_id
    """
    simple = dict()
    for entry in common.read_json(exported_json):
        id = entry.get('id')
        annotator = Annotator(entry.get('annotator')).name
        label = entry.get('label')

        if not simple.get(id):
            simple[id] = dict()

        simple[id][annotator] = label
    return simple

def filter_note_range(simple: dict, note_range) -> dict:
    """
    @param simple: simplified() dict
    @param note_range: range of notes
    @return: dict filtered by note_range
    """
    filtered = dict()
    for id, values in simple.items():
        if id in note_range:
            filtered[id] = values
    return filtered

def calc_term_freq(annotator=Annotator.andy.name, note_range=NoteRange.corpus.value) -> dict:
    """
    Calculate the frequency of TERMS highlighted for each LABEL (Cough, Dyspnea, etc).
    @param annotator: Reviewer like andy, amy, or alon
    @param note_range: default= all in corpus
    @return: dict key=TERM val= {label, list of chart_id}
    """
    term_freq = dict()
    for note_id, values in filter_note_range(simplify(), note_range).items():
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

                term_freq[text][symptom].append({'id': note_id})
    return term_freq

def accuracy_mentions(ground_truth_ann: str, reliability_ann: str, note_range=NoteRange.corpus.value):
    """
    Calculate the frequency of TERMS for each LABEL (Cough, Dyspnea, etc).
    @param ground_truth_ann: annotator like andy, amy, or alon
    @param reliability_ann: annotator like andy, amy, or alon (usually alon)
    @param note_range: default= all in corpus
    @return: dict
    """
    TP = list()  # True Positive
    FN = list()  # False Negative
    id_list = list()  # list notes compared by both annotators

    for note_id, values in filter_note_range(simplify(), note_range).items():
        if values.get(ground_truth_ann):
            for truth_annot in values[ground_truth_ann]:
                truth_span = Span(truth_annot['start'], truth_annot['end'])

                if values.get(reliability_ann):
                    id_list.append(note_id)
                    found = False
                    for confirm in values[reliability_ann]:
                        if not found:
                            compare_span = Span(truth_annot['start'], truth_annot['end'])
                            if overlaps(truth_span, compare_span):
                                if truth_annot['labels'] == confirm['labels']:
                                    found = True
                                    TP.append(confirm)
                    if not found:
                        FN.append(truth_annot)

    print('###############################')
    print(f" TP = {len(TP)}")
    print(f" FN = {len(FN)}")
    print(f" found = {TP}")
    print(f" missed = {FN}")

    return {'TP': TP, 'FN': FN, 'ID': id_list}

def rollup_mentions(annotator, note_range=NoteRange.corpus.value) -> dict:
    """
    @param annotator: like andy, amy, or alon
    @param note_range: default= all in corpus
    @return: dict keys=note_id, values=labels
    """
    rollup = dict()

    for note_id, values in filter_note_range(simplify(), note_range).items():
        if values.get(annotator):
            for annot in values[annotator]:
                if not rollup.get(note_id):
                    rollup[note_id] = list()

                symptom = annot['labels'][0]

                if symptom not in rollup[note_id]:
                    rollup[note_id].append(symptom)
    return rollup

def accuracy_prevalence(ground_truth_ann: str, reliability_ann: str, note_range=NoteRange.corpus.value):
    """
    "prevalence" in the population of physician notes, not a true "prevalence" term.
    This is the rollup of counting each symptom only 1x, not multiple times for a single patient.

    @param ground_truth_ann: annotator like andy, amy, or alon
    @param reliability_ann: annotator like andy, amy, or alon (usually alon)
    @param note_range: default= all in corpus
    @return: dict
    """
    ground_truth = rollup_mentions(ground_truth_ann, note_range)
    reliability = rollup_mentions(reliability_ann, note_range)

    TP = list()  # True Positive
    FN = list()  # False Negative
    id_list = list()  # list notes compared by both annotators

    for note_id, labels in ground_truth.items():
        for symptom in labels:
            key = {note_id: symptom}
            print(key)

            if note_id not in id_list:
                id_list.append(note_id)

            if reliability.get(note_id):
                if symptom in reliability[note_id]:
                    TP.append(key)
                else:
                    FN.append(key)

    print('#####################################')
    print('# Prevalence style rollup calculation')
    print(f" TP = {len(TP)}")
    print(f" FN = {len(FN)}")
    print(f" found = {TP}")
    print(f" missed = {FN}")

    return {'TP': TP, 'FN': FN, 'ID': id_list}

def score_f1(true_pos, false_pos, false_neg) -> dict:
    """
    Score F1 measure with specificity (PPV) and recall (sensitivity).
    F1 delibately ignores "True Negatives" because TN inflates scoring (AUROC)

    @param true_pos: True Positives (agree on positive symptom)
    @param false_pos: False Positives (reliability said pos, annotator said none)
    @param false_neg: False Negative (annotator said pos, reliability said none)
    @return: dict with keys {'f1', 'precision', 'recall'} vals are %% percent
    """
    precision = len(true_pos) / (len(true_pos) + len(false_pos))
    recall = len(true_pos) / (len(true_pos) + len(false_neg))
    f1 = (2 * precision * recall) / (precision + recall)

    return {'f1': f1, 'precision': precision, 'recall': recall}

def score_f1_mentions(ground_truth_ann, reliability_ann, note_range=NoteRange.corpus.value):
    """
    Score reliability of rater at the level of all symptom *MENTIONS*

    @param ground_truth_ann: annotator like andy, amy, or alon
    @param reliability_ann: annotator like andy, amy, or alon (usually alon)
    @param note_range: default= all in corpus
    @return: dict, keys f1, precision, recall and vals= %percent
    """
    ground_truth = accuracy_mentions(ground_truth_ann, reliability_ann, note_range)
    reliability = accuracy_mentions(reliability_ann, ground_truth_ann, note_range)

    true_pos = ground_truth['TP']
    false_neg = ground_truth['FN']
    false_pos = reliability['FN']

    return score_f1(true_pos, false_pos, false_neg)

def score_f1_prevalence(ground_truth_ann, reliability_ann, note_range=NoteRange.corpus.value):
    """
    Score reliability of rater at the level of all symptom *PREVALENCE*

    @param ground_truth_ann: annotator like andy, amy, or alon
    @param reliability_ann: annotator like andy, amy, or alon (usually alon)
    @param note_range: default= all in corpus
    @return: dict, keys f1, precision, recall and vals= %percent
    """
    ground_truth = accuracy_prevalence(ground_truth_ann, reliability_ann, note_range)
    reliability = accuracy_prevalence(reliability_ann, ground_truth_ann, note_range)

    true_pos = ground_truth['TP']
    false_neg = ground_truth['FN']
    false_pos = reliability['FN']

    return score_f1(true_pos, false_pos, false_neg)

class TestLabelstudioAgreement(unittest.TestCase):

    def test_simplify(self):
        path = f'{EXPORTED_JSON}.simple.json'
        common.write_json(path, simplify())

    def test_term_frequency(self):
        for annotator in list(Annotator):
            path = f'{EXPORTED_JSON}.term_freq.{annotator.name}.json'
            common.write_json(path, calc_term_freq(annotator.name))
            print(path)

    def test_score_f1_mentions(self):
        f1_score = score_f1_mentions(Annotator.andy.name, Annotator.alon.name, NoteRange.andy_alon.value)
        print(f'{f1_score}')

    def test_score_f1_prevalence(self):
        f1_score = score_f1_prevalence(Annotator.andy.name, Annotator.alon.name, NoteRange.andy_alon.value)
        print(f'{f1_score}')


if __name__ == '__main__':
    unittest.main()
