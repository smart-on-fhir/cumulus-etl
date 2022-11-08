"""Tests for i2b2.cohort"""
import logging
import unittest
import random

import os
import ctakesclient
from ctakesclient.filesystem import covid_symptoms, map_cui_pref
from ctakesclient.typesystem import *

from cumulus.ctakes import _target_filename

from cumulus import common, store
from cumulus.fhir_common import fhir_date_is_before, parse_fhir_date
from cumulus.labelstudio import LabelStudio
from cumulus.loaders.i2b2.cohort import CohortSelection
from cumulus.loaders.i2b2 import extract, transform
from cumulus.loaders.i2b2.schema import PatientDimension, VisitDimension, ObservationFact

LABELS_LIST = """
Fever or chills
Cough
Nausea or vomiting
Congestion or runny nose
Diarrhea
Sore throat
Headache
Fatigue
Dyspnea
Muscle or body aches
Loss of taste or smell
X
""".strip().splitlines()

def clean_text(physician_note: str) -> str:
    """
    Clean (replace) noisy chars like '¿' from physician note.
    Character length **Must be preserved** !
    @param physician_note: just like the doctor said, but EHR adds noise.
    @return: str replaced chars
    """
    replace_char = '¿'
    return physician_note.replace(replace_char, ' ')

def cache_ctakes(physician_note: str) -> CtakesJSON:
    """
    Write through cache -- this probably belongs in cTAKES.
    @param physician_note: optionally cleaned, will call clean_text(...)
    @return: ctakes response from cache or lazy-loaded
    """
    cleaned = clean_text(physician_note)
    path = _target_filename(cleaned)

    if os.path.exists(path):
        return CtakesJSON(common.read_json(path))
    else:
        dir_folder(path)
        res = ctakesclient.client.extract(cleaned)
        common.write_json(path, res.as_json())
        return res

def dir_folder(path: str) -> str:
    """
    TODO: Replace with fs-spec
    """
    filepart = path.split('/')[-1]
    folder = path.replace(filepart, '')
    os.makedirs(folder, exist_ok=True)
    return folder

def dir_cohort(subdir=None) -> str:
    """
    @param subdir: Symptom Preferred Name (like: "Fever or Chills")
    @return: path to dir (dynamically created dir cohort/FeverOrChills/C0085593)
    """
    if subdir:
        return dir_folder(f"cohort/{subdir.title().replace(' ', '')}")
    else:
        return dir_folder(f'cohort/')

def save_labelstudio(labelstudio: LabelStudio, note_id: str) -> str:
    """
    @param labelstudio: ChartReview file containing physician note and parsed JSON in LabelStudio format.
    @param subdir: subdirectory for files.
    @param cui: concept unique ID, not strictly needed but useful for keeping track.
    @param note_id: where to save labelstudio file.
    @return: path to labelstudio JSON file
    """
    labelstudio.load_lazy()

    #tstmp = common.timestamp_filename()
    path = f'{dir_cohort()}/{note_id}.json'

    common.write_json(path, labelstudio.as_json())
    return path

def get_chart_labels(population: dict) -> dict:
    out = dict()

    for label, chart_list in population.items():
        for chart in set(chart_list):
            if chart not in out.keys():
                out[chart] = list()
            out[chart].append(label)
    return out

def tabulate(population: dict, chart_seq: list) -> str:
    # header row
    out = ['labels:\t'] + [f"{label}\t" for label in LABELS_LIST]

    chart_label = get_chart_labels(population)

    for chart in chart_seq:
        out.append(f'\n{chart}')
        for label in LABELS_LIST:
            if label in chart_label[chart]:
                out.append('\t1')
            else:
                out.append('\t0')
    return ''.join(out)

def merge_populations(population1, population2) -> dict:
    """
    @param population1:
    @param population2:
    @return:
    """
    if not isinstance(population1, dict):
        population1 = common.read_json(population1)
    if not isinstance(population2, dict):
        population2 = common.read_json(population2)

    charts1 = get_chart_labels(population1)
    charts2 = get_chart_labels(population2)

    print(f'#charts1 - {len(charts1.keys())}')
    print(f'#charts2 = {len(charts2.keys())}')

    for chart in charts2.keys():
        if chart not in charts1.keys():
            charts1[chart] = charts2[chart]

    print(f'#merged - {len(charts1.keys())}')

    return charts1

def score_kappa(ann1: list, ann2: list):
    """
    Computes Cohen kappa for pair-wise annotators.
    https://gist.github.com/LouisdeBruijn/1db0283dc69916516e2948f0eefc3a6e#file-cohen_kappa-py

    :param ann1: annotations provided by first annotator
    :type ann1: list
    :param ann2: annotations provided by second annotator
    :type ann2: list
    :rtype: float
    :return: Cohen kappa statistic
    """
    count = 0
    for an1, an2 in zip(ann1, ann2):
        if an1 == an2:
            count += 1
    A = count / len(ann1)  # observed agreement A (Po)

    uniq = set(ann1 + ann2)
    E = 0  # expected agreement E (Pe)
    for item in uniq:
        cnt1 = ann1.count(item)
        cnt2 = ann2.count(item)
        count = ((cnt1 / len(ann1)) * (cnt2 / len(ann2)))
        E += count

    return round((A - E) / (1 - E), 4)


class TestCohortCovidSymptoms(unittest.TestCase):
    """Test case for cohort """

    def test_span_overlapping(self):
        spanA = Span(1, 2)
        spanB = Span(1, 2)
        spanC = Span(1, 3)
        spanD = Span(4, 3)

        span_list = [str(spanA), spanC, spanD]

        self.assertTrue(str(spanB) in span_list)

    def test_date_is_before(self):
        date1 = parse_fhir_date('2017-10-23 19:40:10')
        date2 = parse_fhir_date('2020-03-01')

        self.assertTrue(fhir_date_is_before(date1, date2))
        self.assertFalse(fhir_date_is_before(date2, date1))

    def test_criteria(self):
        """
        Example Cohort Selection
        """
        obs = ObservationFact()
        obs.start_date = parse_fhir_date('2020-03-01')
        obs.end_date = parse_fhir_date('2023-01-01')
        obs.concept_cd = map_cui_pref(covid_symptoms())
        obs.concept_cd['X'] = 'X'  # No Symptoms of COVID

        patient = PatientDimension()
        # patient.birth_date = fhir_common.parse_fhir_period('1992-01-01', '2023-01-01')

        visit = VisitDimension()
        # visit.start_date = fhir_common.parse_fhir_date('2020-03-01')
        # visit.end_date = fhir_common.parse_fhir_date('2023-01-01')
        # visit.length_of_stay = fhir_common.parse_fhir_range_duration(0, 365)

        # serializable to JSON
        select = CohortSelection(patient, visit, obs)
        common.print_json(select.as_json())

        return select

    def test_select_notes_no_labels(self, notes_csv, cnt_notes=30):
        selected = list()
        obsfact_list = extract.extract_csv_observation_facts(notes_csv)
        random.shuffle(obsfact_list)

        for obsfact in obsfact_list:
            selected.append(obsfact.instance_num)

            physician_note = clean_text(obsfact.observation_blob)
            if len(physician_note) > 25:
                no_labels = LabelStudio(physician_note, None)
                save_labelstudio(no_labels, f'no_labels_{obsfact.instance_num}')

            if len(selected) >= cnt_notes:
                logging.info('randomly sampled enough cases, existing.')
                break

        print(selected)

    def test_select_notes_silver_prelabel(self, notes_csv, cnt_notes=30):

        criteria = self.test_criteria()
        criteria_cui_symptom = criteria.observation.concept_cd

        print(criteria.as_json())
        print(criteria_cui_symptom)

        obsfact_list = extract.extract_csv_observation_facts(notes_csv)
        print(f'ObservationFact count is # {len(obsfact_list)}')

        random.shuffle(obsfact_list)

        seen = list()
        selected = list()
        population = {}

        for cui, symptom_pref in criteria_cui_symptom.items():
            print(f'{cui} : {symptom_pref}')
            population[symptom_pref] = []

        for obsfact in obsfact_list:
            seen.append(obsfact.instance_num)

            if fhir_date_is_before(obsfact.start_date, criteria.observation.start_date):
                print(f'Excluded date:  {obsfact.start_date}')
                continue

            # NLP
            physician_note = clean_text(obsfact.observation_blob)
            if len(physician_note) < 25:
                print(f'@note too short\t{physician_note}')
                continue

            nlp_cache = cache_ctakes(physician_note)
            nlp_cui_matches = nlp_cache.list_concept_cui(ctakesclient.typesystem.Polarity.pos)
            nlp_cui_matches = set(criteria_cui_symptom.keys()).intersection(set(nlp_cui_matches))

            labelstudio = LabelStudio(
                physician_note=physician_note,
                response_ctakes=nlp_cache,
                filter_cui=criteria_cui_symptom)

            # SelectChart: ** no ** 0 Symptoms of COVID
            criteria_sx_no = (not nlp_cui_matches) and (len(set(population['X'])) < cnt_notes)

            if criteria_sx_no:
                selected.append(obsfact.instance_num)
                population['X'].append(obsfact.instance_num)
                save_labelstudio(labelstudio, obsfact.instance_num)

            # SelectChart: ** yes ** 1+ Symptoms of COVID
            elif nlp_cui_matches:

                for cui, symptom_pref in criteria_cui_symptom.items():
                    criteria_sx_yes = (cui in nlp_cui_matches) and (len(set(population[symptom_pref])) < cnt_notes)

                    if criteria_sx_yes:
                        save_labelstudio(labelstudio, obsfact.instance_num)

                        population[symptom_pref].append(obsfact.instance_num)
                        common.write_json('cohort/population.json', population)
                        common.write_text('cohort/tabulate.tsv', tabulate(population, chart_seq=selected))

                        if obsfact.instance_num not in selected:
                            selected.append(obsfact.instance_num)
                            print(f'added note for date= {obsfact.start_date}')

                        print(f'{cui} in {nlp_cui_matches} {symptom_pref}')
            else:
                print(f'no match, len(seen) = {len(seen)}')

def test_merge_population():
    pop1 = '/Users/andy/chip/cumulus-etl/cohort_Nov5_100/population.json'
    pop2 = '/Users/andy/chip/cumulus-etl/cohort_Nov5_1051am/population.json'
    merged = merge_populations(pop1, pop2)
    print(merged)

def test_summary_counts():
    for label, chart_list in common.read_json('/Users/andy/chip/cumulus-etl/cohort/population.json').items():
        print(f'{len(set(chart_list))}\t{label}')


if __name__ == '__main__':
    runner = TestCohortCovidSymptoms()
    #runner.test_select('/Users/andy/phi/i2b2/csv_note/sample.csv')
    runner.test_select_notes_no_labels('/Users/andy/phi/i2b2/csv_note/NOTE_COHORT_202202062242.csv')
    runner.test_select_notes_silver_prelabel('/Users/andy/phi/i2b2/csv_note/NOTE_COHORT_202202062242.csv')