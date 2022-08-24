import sys
import json
import logging
from typing import List

from etl import common, store
from etl import i2b2
from etl.i2b2.config import JobConfig, JobSummary
from etl.codebook import Codebook

#######################################################################################################################
#
# FHIR Patient
#
#######################################################################################################################

def etl_patient(config: JobConfig) -> JobSummary:
    codebook = Codebook(config.path_codebook())

    job = JobSummary('etl_patient')

    for i2b2_csv in config.list_csv_patient():
        i2b2_list = i2b2.extract.extract_csv_patient(i2b2_csv)

        job.csv.append(i2b2_csv)

        print('######################################################################################################')
        print(f'etl_patient() {i2b2_csv} #records = {len(i2b2_list)}')

        for i2b2_patient in i2b2_list:
            try:
                job.attempt.append(i2b2_patient)

                subject = i2b2.transform.to_fhir_patient(i2b2_patient)

                path = store.path_json(config.dir_output_patient(subject.id), 'fhir_patient.json')

                deid = codebook.fhir_patient(subject)

                job.success.append(store.write_json(path, deid.as_json()))
                job.success_rate()

            except Exception as e:
                logging.error(f'ETL exception {e}')
                job.failed.append(i2b2_patient.as_json())


    codebook.db.save(config.path_codebook())
    return job

#######################################################################################################################
#
# FHIR Encounter
#
#######################################################################################################################

def etl_visit(config:JobConfig) -> JobSummary:
    codebook = Codebook(config.path_codebook())

    job = JobSummary('etl_visit')

    for i2b2_csv in config.list_csv_visit():
        i2b2_list = i2b2.extract.extract_csv_visits(i2b2_csv)

        job.csv.append(i2b2_csv)

        print('######################################################################################################')
        print(f'etl_encounter() {i2b2_csv} #records = {len(i2b2_list)}')

        for i2b2_visit in i2b2_list:
            try:
                job.attempt.append(i2b2_visit)

                encounter = i2b2.transform.to_fhir_encounter(i2b2_visit)

                mrn = encounter.subject.reference
                path = store.path_json(config.dir_output_encounter(mrn, encounter.id), 'fhir_patient.json')

                deid = codebook.fhir_encounter(encounter)

                job.success.append(store.write_json(path, deid.as_json()))
                job.success_rate()

            except Exception as e:
                logging.error(f'ETL exception {e}')
                job.failed.append(i2b2_visit.as_json())

    codebook.db.save(config.path_codebook())
    return job

#######################################################################################################################
#
# FHIR Observation (Lab Result)
#
#######################################################################################################################

def etl_lab(config:JobConfig) -> JobSummary:
    codebook = Codebook(config.path_codebook())

    job = JobSummary('etl_lab')

    for i2b2_csv in config.list_csv_lab():
        i2b2_list = i2b2.extract.extract_csv_observation_fact(i2b2_csv)

        job.csv.append(i2b2_csv)

        print('######################################################################################################')
        print(f'etl_lab() {i2b2_csv} #records = {len(i2b2_list)}')

        for i2b2_lab in i2b2_list:
            try:
                job.attempt.append(i2b2_lab)

                lab = i2b2.transform.to_fhir_observation_lab(i2b2_lab)

                mrn = lab.subject.reference
                enc = lab.context.reference

                deid = codebook.fhir_observation(lab)

                path = store.path_json(config.dir_output_encounter(mrn, enc), f'fhir_lab_{deid.id}.json')

                job.success.append(store.write_json(path, deid.as_json()))
                job.success_rate()

            except Exception as e:
                logging.error(f'ETL exception {e}')
                job.failed.append(i2b2_lab.as_json())

    codebook.db.save(config.path_codebook())
    return job

#######################################################################################################################
#
# FHIR Condition
#
#######################################################################################################################

def etl_diagnosis(config:JobConfig) -> JobSummary:
    codebook = Codebook(config.path_codebook())

    job = JobSummary('etl_diagnosis')

    for i2b2_csv in config.list_csv_diagnosis():
        i2b2_list = i2b2.extract.extract_csv_observation_fact(i2b2_csv)

        job.csv.append(i2b2_csv)

        print('######################################################################################################')
        print(f'etl_diagnosis() {i2b2_csv} #records = {len(i2b2_list)}')

        for i2b2_observation in i2b2_list:
            try:
                job.attempt.append(i2b2_observation)

                condition = i2b2.transform.to_fhir_condition(i2b2_observation)

                mrn = condition.subject.reference
                enc = condition.context.reference

                deid = codebook.fhir_condition(condition)

                path = store.path_json(config.dir_output_encounter(mrn, enc), f'fhir_condition_{deid.id}.json')

                job.success.append(store.write_json(path, deid.as_json()))
                job.success_rate()

            except Exception as e:
                logging.error(f'ETL exception {e}')
                job.failed.append(i2b2_observation.as_json())

    codebook.db.save(config.path_codebook())
    return job

#######################################################################################################################
#
# FHIR DocumentReference
#
#######################################################################################################################

def etl_notes(config:JobConfig) -> JobSummary:
    codebook = Codebook(config.path_codebook())

    job = JobSummary('etl_notes')

    for i2b2_csv in config.list_csv_notes():
        i2b2_list = i2b2.extract.extract_csv_observation_fact(i2b2_csv)

        job.csv.append(i2b2_csv)

        print('######################################################################################################')
        print(f'etl_notes() {i2b2_csv} #physican notes = {len(i2b2_list)}')

        for i2b2_physician_note in i2b2_list:
            try:
                job.attempt.append(i2b2_physician_note)

                docref = i2b2.transform.to_fhir_documentreference(i2b2_physician_note)

                mrn = docref.subject.reference
                enc = docref.context.encounter.reference

                deid = codebook.fhir_documentreference(docref)

                path = store.path_json(config.dir_output_encounter(mrn, enc), f'fhir_docref_{deid.id}.json')

                job.success.append(store.write_json(path, deid.as_json()))
                job.success_rate()

            except Exception as e:
                logging.error(f'ETL exception {e}')
                job.failed.append(i2b2_physician_note.as_json())

    codebook.db.save(config.path_codebook())
    return job




#######################################################################################################################
#
# Main Pipeline (run all tasks)
#
#######################################################################################################################

def etl_job(config:JobConfig) -> List[JobSummary]:
    """
    :param config:
    :return:
    """
    summary_list = list()

    task_list = [
        i2b2.etl.etl_patient,
        i2b2.etl.etl_visit,
        i2b2.etl.etl_lab,
        i2b2.etl.etl_notes,
        i2b2.etl.etl_diagnosis,
    ]

    for task in task_list:
        summary = task(config)
        summary_list.append(summary)

        path = store.path_json(config.dir_output_config(), f'{summary.label}.json')
        store.write_json(path, summary.as_json())

    return summary_list

#######################################################################################################################
#
# Main
#
#######################################################################################################################

def main(args):
    if len(args) < 2:
        print('usage')
        print('example: /my/i2b2/input /my/i2b2/processed')
    else:
        dir_input = args[0]
        dir_output = args[1]

        logging.info(f"Input Directory: {dir_input}")
        logging.info(f"Output Directory: {dir_output}")

        config = JobConfig(dir_input, dir_output)
        print(json.dumps(config.as_json(), indent=4))

        store.write_json(config.path_config(), config.as_json())

        for summary in etl_job(config):
            print(json.dumps(summary.as_json(), indent=4))


if __name__ == '__main__':
    main(sys.argv[1:])

