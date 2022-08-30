import os
import glob
import json
import random
import unittest

from ctakes.bsv_reader import BSV, list_bsv, bsv_to_cui_map
from ctakes.ctakes_json import MatchText, UmlsConcept, CtakesJSON, UmlsTypeMention

from cumulus import store, common
from cumulus.labelstudio import COVID_SYMPTOMS_BSV
from cumulus.labelstudio import LabelStudio

class TestLabelStudio(unittest.TestCase):

    def test_rand_dir_processed(self, dir_processed='/opt/i2b2/processed'):
        ctakes_list = common.find_by_name(dir_processed, 'ctakes_')
        random.shuffle(ctakes_list)
        return ctakes_list

    def test_select_cohort(self, dir_processed='/opt/i2b2/processed', cohort_size=20):
        filter_cui = bsv_to_cui_map(COVID_SYMPTOMS_BSV)

        candidates = list()
        cohort = dict()

        for key in filter_cui.keys():
            cohort[key] = list()

        for path_ctakes in self.test_rand_dir_processed(dir_processed):
            candidates.append(path_ctakes)

            if 0 == len(candidates) % 100:
                print(f'candidates: {len(candidates)}')

            reader = CtakesJSON(store.read_json(path_ctakes))

            path_note = path_ctakes.replace('ctakes_', 'physician_note_').replace('.json', '.txt')
            path_labelstudio = path_ctakes.replace('ctakes_', 'labelstudio_')

            if os.path.exists(path_ctakes) and os.path.exists(path_note):
                found_cuis = reader.list_concept_cui(polarity=True)

                overlap = set(filter_cui.keys()).intersection(set(found_cuis))

                if len(overlap) > 0:

                    for select_cui in filter_cui.keys():
                        if select_cui not in cohort.keys():
                            print(f'cohort[{select_cui}] init list')
                            cohort[select_cui] = list()

                        if len(cohort[select_cui]) <= cohort_size:

                            if select_cui in overlap:
                                labelstudio = LabelStudio(physician_note=path_note,
                                                          response_ctakes=reader,
                                                          filter_cui=filter_cui)

                                labelstudio.note_file = path_note
                                labelstudio.load_lazy()

                                store.write_json(path_labelstudio, labelstudio.as_json())

                                cohort[select_cui].append(path_labelstudio)

                                print(f'cohort[{select_cui}]: {len(cohort[select_cui])}')

                                path_cui = f'{dir_processed}/{select_cui}.json'
                                dict_cui = {'filter': filter_cui[select_cui], 'cohort': cohort[select_cui]}
                                store.write_json(path_cui, dict_cui)

    def test_merge_cohort(self, dir_processed='/opt/i2b2/processed'):
        merged = list()

        for select_cui in bsv_to_cui_map(COVID_SYMPTOMS_BSV).keys():
            path_cui = f'{dir_processed}/{select_cui}.json'

            if not os.path.exists(path_cui):
                raise Exception(f'not found! {path_cui}')

            found = store.read_json(path_cui)['cohort']
            merged += found

        print(merged)
        print(f'merged list # {len(merged)}')
        print(f'merged set  # {len(set(merged))}')

        contents = list()

        for f in set(merged):
            contents.append(store.read_json(f))

        store.write_json(f'{dir_processed}/labelstudio_cohort.json', contents)



