"""Tests for cumulus.labelstudio"""

import os
import random
import unittest

from ctakesclient.filesystem import covid_symptoms, map_cui_pref
from ctakesclient.typesystem import CtakesJSON, Polarity

from cumulus import store, common
from cumulus.chart_review.labelstudio import LabelStudio, merge_cohort


@unittest.skip("Not yet finished and needs access to /opt/i2b2")
class TestLabelStudio(unittest.TestCase):
    """Test case for label studio"""

    def test_rand_dir_processed(self, dir_processed="/opt/i2b2/processed"):
        ctakes_list = common.find_by_name(dir_processed, "ctakes_")
        random.shuffle(ctakes_list)
        return ctakes_list

    def test_select_cohort(self, dir_processed="/opt/i2b2/processed", cohort_size=20):
        filter_cui = map_cui_pref(covid_symptoms())

        candidates = []
        cohort = {}

        for key in filter_cui:
            cohort[key] = []

        for path_ctakes in self.test_rand_dir_processed(dir_processed):
            candidates.append(path_ctakes)

            if 0 == len(candidates) % 100:
                print(f"candidates: {len(candidates)}")

            reader = CtakesJSON(store.read_json(path_ctakes))

            path_note = path_ctakes.replace("ctakes_", "physician_note_").replace(".json", ".txt")
            path_labelstudio = path_ctakes.replace("ctakes_", "labelstudio_")

            if os.path.exists(path_ctakes) and os.path.exists(path_note):
                found_cuis = reader.list_concept_cui(Polarity.pos)

                overlap = set(filter_cui.keys()).intersection(set(found_cuis))

                if len(overlap) > 0:

                    for select_cui, text in filter_cui.items():
                        if select_cui not in cohort:
                            print(f"cohort[{select_cui}] init list")
                            cohort[select_cui] = []

                        if len(cohort[select_cui]) <= cohort_size:

                            if select_cui in overlap:
                                labelstudio = LabelStudio(
                                    physician_note=path_note, response_ctakes=reader, filter_cui=filter_cui
                                )

                                labelstudio.note_file = path_note
                                labelstudio.load_lazy()

                                store.write_json(path_labelstudio, labelstudio.as_json())

                                cohort[select_cui].append(path_labelstudio)

                                print(f"cohort[{select_cui}]: " f"{len(cohort[select_cui])}")

                                cohort_dir = f"{dir_processed}/cohort"
                                path_cui = f"{cohort_dir}/{select_cui}.json"
                                dict_cui = {"filter": text, "cohort": cohort[select_cui]}
                                store.write_json(path_cui, dict_cui)

    def test_merge_content(self, dir_processed="/opt/i2b2/processed"):
        merged = []

        for select_cui in map_cui_pref(covid_symptoms()):
            cohort_dir = f"{dir_processed}/cohort"
            path_cui_cohort = f"{cohort_dir}/{select_cui}.json"
            path_cui_content = f"{cohort_dir}/{select_cui}_content.json"

            cui_content = merge_cohort(path_cui_cohort)
            store.write_json(path_cui_content, cui_content)

            merged += cui_content

        store.write_json(f"{dir_processed}/cohort/labelstudio_content.json", merged)

    def test_merge_labels(self, dir_processed="/opt/i2b2/processed"):
        label_cui = {}

        for cui, pref in map_cui_pref(covid_symptoms()).items():
            if pref not in label_cui.values():
                label_cui[pref] = []
            label_cui[pref].append(cui)

        for pref, cuis in label_cui.items():
            label = str(pref).replace(" ", "_")
            label_cohort = []
            label_content = []

            for cui in cuis:
                path_cui_cohort = f"{dir_processed}/cohort/{cui}.json"
                cui_cohort = store.read_json(path_cui_cohort).get("cohort")

                label_cohort += cui_cohort

            for filepath in set(label_cohort):
                label_content.append(store.read_json(filepath))

            store.write_json(f"{dir_processed}/cohort/{label}.json", label_content)

    def test_print_labels(self):
        print("##############################################")
        print("LabelStudio cui_filter")
        print(map_cui_pref(covid_symptoms()))
        print("##############################################")
        print("LabelStudio labels")
        for label in set(map_cui_pref(covid_symptoms()).values()):
            print(label.strip())

    def test_labelstudio_files_cleanup(self, dir_processed="/opt/i2b2/processed", cmd_head="rm", cmd_tail=""):
        print(f"labelstudio_files_cleanup : {cmd_head} " f"{dir_processed}/**/labelstudio_* {cmd_tail}")
        bash = []
        for path in common.find_by_name(dir_processed, "labelstudio_"):
            bash.append(f"{cmd_head} {path} {cmd_tail}")
        common.write_text(f"{dir_processed}/labelstudio_bash.sh", "\n".join(bash))
