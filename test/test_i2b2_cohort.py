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

LABEL_STUDIO_NER = """
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
Anosmia
"""
def asciify(physician_note: str) -> str:
    return physician_note.encode('ascii', 'ignore').decode('utf-8')

def cache_ctakes(physician_note: str) -> CtakesJSON:
    ascii_text = asciify(physician_note)
    path = _target_filename(ascii_text)

    if os.path.exists(path):
        return CtakesJSON(common.read_json(path))
    else:
        dir_folder(path)
        res = ctakesclient.client.extract(ascii_text)
        common.write_json(path, res.as_json())
        return res

def dir_folder(path: str) -> str:
    """
    Replace with fs-spec
    """
    filepart = path.split('/')[-1]
    folder = path.replace(filepart, '')
    os.makedirs(folder, exist_ok=True)
    return folder

def dir_cohort(symptom: str, cui: str) -> str:
    """
    @param cui: Concept Unique ID
    @param pref: Symptom Preferred Name (like: "Fever or Chills")
    @return: path to dir (dynamically created dir cohort/FeverOrChills/C0085593)
    """
    return dir_folder(f"cohort/{symptom.title().replace(' ','')}/{cui}/")

def save_labelstudio(labelstudio: LabelStudio, symptom: str, cui: str, filename:str) -> str:

    labelstudio.load_lazy()

    cohort_dir = dir_cohort(symptom, cui)
    path = f'{cohort_dir}/{filename}'

    common.write_json(path, labelstudio.as_json())

    return path

#class TestCohortCovidSymptoms(unittest.TestCase):
class TestCohortCovidSymptoms:
    """Test case for cohort """

    def test_criteria(self):
        """
        Example Cohort Selection
        """
        obs = ObservationFact()
        obs.start_date = parse_fhir_date('2020-03-01')
        obs.end_date = parse_fhir_date('2023-01-01')
        obs.concept_cd = map_cui_pref(covid_symptoms())
        obs.concept_cd['X'] = 'X' # No Symptoms of COVID

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

    def test_select_notes(self, notes_csv, cnt_notes=20):

        criteria = self.test_criteria()
        criteria_cui_symptom = criteria.observation.concept_cd

        print(criteria.as_json())
        print(criteria_cui_symptom)

        obsfact_list = extract.extract_csv_observation_facts(notes_csv)
        print(f'ObservationFact count is # {len(obsfact_list)}')

        random.shuffle(obsfact_list)

        seen = list()
        population = {}

        for cui, symptom_pref in criteria_cui_symptom.items():
            print(f'{cui} : {symptom_pref}')
            population[symptom_pref] = []

        for obsfact in obsfact_list:
            seen.append(obsfact.instance_num)

            if fhir_date_is_before(obsfact.start_date, criteria.visit.start_date):
                print(f'before COVID study start {obsfact.start_date}')
                continue

            # NLP
            physician_note = asciify(obsfact.observation_blob)
            if len(physician_note) < 25:
                print(f'@@@ Note too short')
                print(physician_note)
                continue

            nlp_cache = cache_ctakes(physician_note)
            nlp_cui_matches = nlp_cache.list_concept_cui(ctakesclient.typesystem.Polarity.pos)
            nlp_cui_matches = set(criteria_cui_symptom.keys()).intersection(set(nlp_cui_matches))

            labelstudio = LabelStudio(
                physician_note=physician_note,
                response_ctakes=nlp_cache,
                filter_cui=criteria_cui_symptom)

            # SelectChart: ** no ** 0 Symptoms of COVID
            criteria_sx_no = (not nlp_cui_matches) and (len(population['X']) < cnt_notes)

            if criteria_sx_no:
                labelstudio_file = save_labelstudio(
                    labelstudio,
                    'No Symptoms Of Covid', 'X',
                    f'no_sx_obsfact_{obsfact.instance_num}.json')

                population['X'].append(labelstudio_file)

            # SelectChart: ** yes ** 1+ Symptoms of COVID
            elif nlp_cui_matches:

                for cui, symptom_pref in criteria_cui_symptom.items():
                    criteria_sx_yes = (cui in nlp_cui_matches) and (len(population[symptom_pref]) < cnt_notes)

                    if criteria_sx_yes:
                        print(f'{cui} in {nlp_cui_matches} {symptom_pref}')

                        labelstudio_file = save_labelstudio(
                            labelstudio,
                            symptom_pref, cui,
                            f'cui_{cui}_obsfact_{obsfact.instance_num}.json')

                        population[symptom_pref].append(labelstudio_file)
            else:
                print(f'no match, len(seen) = {len(seen)}')


if __name__ == '__main__':
    runner = TestCohortCovidSymptoms()
    #runner.test_select('/Users/andy/phi/i2b2/csv_note/sample.csv')
    runner.test_select_notes('/Users/andy/phi/i2b2/csv_note/NOTE_COHORT_202202062242.csv')