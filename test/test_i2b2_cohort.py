"""Tests for i2b2.cohort"""
import logging
import unittest

import os
import ctakesclient
from ctakesclient.filesystem import covid_symptoms, map_cui_pref
from ctakesclient.typesystem import *

from cumulus.ctakes import _target_filename

from cumulus import fhir_common, common, store
from cumulus.labelstudio import LabelStudio
from cumulus.loaders.i2b2.cohort import CohortSelection
from cumulus.loaders.i2b2 import extract, transform
from cumulus.loaders.i2b2.schema import PatientDimension, VisitDimension, ObservationFact

def dir_folder(path: str) -> str:
    filepart = path.split('/')[-1]
    folder = path.replace(filepart, '')
    os.makedirs(folder, exist_ok=True)
    return folder

def cache_ctakes(physician_note: str) -> CtakesJSON:
    path = _target_filename(physician_note)

    if os.path.exists(path):
        return CtakesJSON(common.read_json(path))
    else:
        dir_folder(path)
        res = ctakesclient.client.extract(physician_note)
        common.write_json(path, res.as_json())
        return res

#class TestCohortCovidSymptoms(unittest.TestCase):
class TestCohortCovidSymptoms:
    """Test case for cohort """

    def test_definition(self):
        """
        Example Cohort Selection
        """
        patient = PatientDimension()
        patient.birth_date = fhir_common.parse_fhir_period('1992-01-01', '2023-01-01')

        visit = VisitDimension()
        visit.start_date = fhir_common.parse_fhir_date('2020-03-01')
        visit.end_date = fhir_common.parse_fhir_date('2023-01-01')
        visit.length_of_stay = fhir_common.parse_fhir_range_duration(0, 365)

        obs = ObservationFact()
        obs.start_date = fhir_common.parse_fhir_date('2020-03-01')
        obs.end_date = fhir_common.parse_fhir_date('2023-01-01')
        obs.concept_cd = map_cui_pref(covid_symptoms())

        # serializable to JSON
        select = CohortSelection(patient, visit, obs)
        common.print_json(select.as_json())

        return select

    def test_select(self, notes_csv, cohort_size=20):

        select = self.test_definition()
        filter_cui = select.observation.concept_cd

        print(select.as_json())
        print(filter_cui)

        obsfact_list = extract.extract_csv_observation_facts(notes_csv)
        print(f'ObservationFact count is # {len(obsfact_list)}')

        candidates = []
        cohort = {}

        for key in filter_cui:
            cohort[key] = []

        for obsfact in obsfact_list:
            candidates.append(obsfact.instance_num)

            _start_date = fhir_common.parse_fhir_date(obsfact.start_date)
            if _start_date.isostring < select.visit.start_date.isostring:
                print(f'skip start_date = {_start_date.isostring}')
                continue

            if 0 == len(candidates) % 10:
                print(f'candidates: {len(candidates)}')

            physician_note = obsfact.observation_blob
            reader = cache_ctakes(physician_note)

            found_cuis = reader.list_concept_cui(ctakesclient.typesystem.Polarity.pos)

            overlap = set(filter_cui.keys()).intersection(set(found_cuis))

            if len(overlap) > 0:
                print(f'overlap= {overlap}')

                for select_cui, select_pref in filter_cui.items():
                    if select_cui not in cohort:
                        print(f'cohort[{select_cui}] init list')
                        cohort[select_cui] = []

                    if len(cohort[select_cui]) <= cohort_size:

                        if select_cui in overlap:
                            labelstudio = LabelStudio(
                                physician_note=physician_note,
                                response_ctakes=reader,
                                filter_cui=filter_cui)

                            labelstudio.load_lazy()

                            cohort_dir = f"cohort/{select_pref.title().replace(' ','')}/"
                            dir_folder(cohort_dir)

                            file_labelstudio = f'labelstudio_{obsfact.instance_num}.json'

                            common.write_json(f'{cohort_dir}/{file_labelstudio}',
                                              labelstudio.as_json())

                            cohort[select_cui].append(file_labelstudio)

                            path_cui = f'{cohort_dir}/{select_cui}.json'

                            dict_cui = {
                                'filter': select_pref,
                                'cohort': cohort[select_cui]
                            }
                            common.write_json(path_cui, dict_cui)


if __name__ == '__main__':
    runner = TestCohortCovidSymptoms()
    #runner.test_select('/Users/andy/phi/i2b2/csv_note/sample.csv')
    runner.test_select('/Users/andy/phi/i2b2/csv_note/NOTE_COHORT_202202062242.csv')