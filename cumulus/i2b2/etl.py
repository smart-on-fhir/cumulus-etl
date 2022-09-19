"""Load, transform, and write out i2b2 to FHIR"""

import json
import logging
import os
import sys
from typing import Any, Callable, List

from cumulus import common, store
from cumulus import i2b2
from cumulus.i2b2.config import JobConfig, JobSummary
from cumulus.codebook import Codebook


###############################################################################
#
# Helpers
#
###############################################################################

def _process_job_entries(
    config: JobConfig,
    job_name: str,
    csv_folder: str,
    extractor: Callable[[str], List[Any]],
    processor: Callable[[Codebook, Any], str],
):
    codebook = Codebook(config.path_codebook())

    job = JobSummary(job_name)

    for i2b2_csv in config.list_csv(csv_folder):
        i2b2_list = extractor(i2b2_csv)

        job.csv.append(i2b2_csv)

        print('###############################################################')
        print(f'{job_name}() {i2b2_csv} #records = {len(i2b2_list)}')

        for i2b2_entry in i2b2_list:
            try:
                job.attempt.append(i2b2_entry)

                path = processor(codebook, i2b2_entry)

                job.success.append(path)
                job.success_rate()

            except Exception as e:  # pylint: disable=broad-except
                logging.error('ETL exception %s', e)
                job.failed.append(i2b2_entry.as_json())

    codebook.db.save(config.path_codebook())
    return job


###############################################################################
#
# FHIR Patient
#
###############################################################################


def etl_patient(config: JobConfig) -> JobSummary:
    def process_patient(codebook, patient):
        subject = i2b2.transform.to_fhir_patient(patient)
        mrn = subject.id
        deid = codebook.fhir_patient(subject)
        path = store.path_file(config.dir_output_patient(mrn),
                               'fhir_patient.json')
        store.write_json(path, deid.as_json())
        return path

    return _process_job_entries(config, 'etl_patient', 'csv_patient',
                                i2b2.extract.extract_csv_patient,
                                process_patient)


###############################################################################
#
# FHIR Encounter
#
###############################################################################

def etl_visit(config: JobConfig) -> JobSummary:
    def process_visit(codebook, visit):
        encounter = i2b2.transform.to_fhir_encounter(visit)
        mrn = encounter.subject.reference
        enc = encounter.id
        deid = codebook.fhir_encounter(encounter)
        path = store.path_file(config.dir_output_encounter(mrn, enc),
                               'fhir_patient.json')
        store.write_json(path, deid.as_json())
        return path

    return _process_job_entries(config, 'etl_visit', 'csv_visit',
                                i2b2.extract.extract_csv_visits,
                                process_visit)


###############################################################################
#
# FHIR Observation (Lab Result)
#
###############################################################################

def etl_lab(config: JobConfig) -> JobSummary:
    def process_lab(codebook, fact):
        lab = i2b2.transform.to_fhir_observation_lab(fact)
        mrn = lab.subject.reference
        enc = lab.context.reference
        deid = codebook.fhir_observation(lab)
        path = store.path_file(config.dir_output_encounter(mrn, enc),
                               f'fhir_lab_{deid.id}.json')
        store.write_json(path, deid.as_json())
        return path

    return _process_job_entries(config, 'etl_lab', 'csv_lab',
                                i2b2.extract.extract_csv_observation_fact,
                                process_lab)


###############################################################################
#
# FHIR Condition
#
###############################################################################

def etl_diagnosis(config: JobConfig) -> JobSummary:
    def process_diagnosis(codebook, fact):
        condition = i2b2.transform.to_fhir_condition(fact)
        mrn = condition.subject.reference
        enc = condition.context.reference
        deid = codebook.fhir_condition(condition)
        path = store.path_file(config.dir_output_encounter(mrn, enc),
                               f'fhir_condition_{deid.id}.json')
        store.write_json(path, deid.as_json())
        return path

    return _process_job_entries(config, 'etl_diagnosis', 'csv_diagnosis',
                                i2b2.extract.extract_csv_observation_fact,
                                process_diagnosis)


###############################################################################
#
# FHIR DocumentReference
#
###############################################################################

def etl_notes(config: JobConfig) -> JobSummary:
    def process_note(codebook, fact):
        docref = i2b2.transform.to_fhir_documentreference(fact)
        mrn = docref.subject.reference
        deid = codebook.fhir_documentreference(docref)

        # TODO: confirm what we should do with multiple/zero encounters
        if len(docref.context.encounter) != 1:
            raise ValueError('Cumulus only supports single-encounter '
                             'notes right now')

        enc = docref.context.encounter[0].reference
        path = store.path_file(config.dir_output_encounter(mrn, enc),
                               f'fhir_docref_{deid.id}.json')
        store.write_json(path, deid.as_json())
        return path

    return _process_job_entries(config, 'etl_notes', 'csv_note',
                                i2b2.extract.extract_csv_observation_fact,
                                process_note)


def etl_notes_nlp(config: JobConfig) -> JobSummary:
    def process_note_nlp(codebook, fact):
        del codebook

        note_text = fact.observation_blob
        md5sum = common.hash_clinical_text(note_text)

        mrn = fact.patient_num
        enc = fact.encounter_num

        folder = config.dir_output_encounter(mrn, enc)
        os.makedirs(folder, exist_ok=True)

        path_text = os.path.join(folder, f'physician_note_{md5sum}.txt')
        path_ctakes = os.path.join(folder, f'ctakes_{md5sum}.json')

        if len(note_text) > 10:
            if not os.path.exists(path_text):
                store.write_text(path_text, note_text)
            if not os.path.exists(path_ctakes):
                logging.warning('cTAKES response not found')

        return path_text

    return _process_job_entries(config, 'etl_notes_nlp', 'csv_note',
                                i2b2.extract.extract_csv_observation_fact,
                                process_note_nlp)


###############################################################################
#
# Main Pipeline (run all tasks)
#
###############################################################################


def etl_job(config: JobConfig) -> List[JobSummary]:
    """
    :param config:
    :return:
    """
    summary_list = []

    task_list = [
        i2b2.etl.etl_patient,
        i2b2.etl.etl_visit,
        i2b2.etl.etl_lab,
        i2b2.etl.etl_notes,
        i2b2.etl.etl_notes_nlp,
        i2b2.etl.etl_diagnosis,
    ]

    for task in task_list:
        summary = task(config)
        summary_list.append(summary)

        path = store.path_file(config.dir_output_config(),
                               f'{summary.label}.json')
        store.write_json(path, summary.as_json())

    return summary_list


###############################################################################
#
# Main
#
###############################################################################


def main(args):
    if len(args) < 2:
        print('usage')
        print('example: /my/i2b2/input /my/i2b2/processed')
    else:
        dir_input = args[0]
        dir_output = args[1]

        logging.info('Input Directory: %s', dir_input)
        logging.info('Output Directory: %s', dir_output)

        config = JobConfig(dir_input, dir_output)
        print(json.dumps(config.as_json(), indent=4))

        store.write_json(config.path_config(), config.as_json())

        for summary in etl_job(config):
            print(json.dumps(summary.as_json(), indent=4))


if __name__ == '__main__':
    main(sys.argv[1:])
