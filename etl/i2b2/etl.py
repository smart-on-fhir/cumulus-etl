import os
import sys
import json
from typing import List
import logging

from etl import common, store, ctakes
from etl import i2b2
from etl.codebook import Codebook

#######################################################################################################################
#
# Job Defition and result summary
#
#######################################################################################################################

class JobConfig:
    def __init__(self, dir_input, dir_output, label=None):
        self.dir_input = dir_input
        self.dir_output = dir_output
        self.timestamp = common.timestamp_datetime()
        self.hostname = common.gethostname()
        self.ctakes = ctakes.get_url_ctakes()
        self.label = label if label else common.timestamp_date()

    def dir_output_patient(self, mrn:str) -> str:
        return store.path_patient_dir(self.dir_output, mrn)

    def dir_output_encounter(self, mrn:str, encounter_id) -> str:
        return store.path_encounter_dir(self.dir_output, mrn, encounter_id)

    def path_codebook(self) -> str:
        return store.path_json(self.dir_output, 'codebook.json')

    def list_csv(self, folder) -> list:
        return common.list_csv(os.path.join(self.dir_input, folder))

    def list_csv_patient(self) -> list:
        return self.list_csv('csv_patient')

    def list_csv_visit(self) -> list:
        return self.list_csv('csv_visit')

    def list_csv_lab(self) -> list:
        return self.list_csv('csv_lab')

    def list_csv_diagnosis(self) -> list:
        return self.list_csv('csv_diagnosis')

    def list_csv_notes(self) -> list:
        return self.list_csv('csv_note')

    def as_json(self):
        return {'dir_input': self.dir_input,
                'dir_output': self.dir_output,
                'label': self.label,
                'codebook': self.path_codebook(),
                'list_csv_patient': self.list_csv_patient(),
                'list_csv_visit': self.list_csv_visit(),
                'list_csv_lab': self.list_csv_lab(),
                'list_csv_notes': self.list_csv_notes(),
                'list_csv_diagnosis': self.list_csv_diagnosis()}

class JobSummary:
    def __init__(self, label=None):
        self.label = label
        self.csv = list()
        self.attempt = list()
        self.success = list()
        self.failed = list()
        self.timestamp = common.timestamp_datetime()
        self.hostname = common.gethostname()

    def progress(self, show_every=1000*10) -> float:
        """
        :param show_every: print success rate
        :return: % progress
        """
        if self.inputsize > 0:
            prct = float(len(self.attempt)) / float(self.inputsize)
            if 0 == len(self.attempt) % show_every:
                print(f'attempt = {len(self.attempt)} progress % {prct}')
            if self.inputsize == len(self.attempt):
                print(f'attempt = {len(self.attempt)} complete.')

            return prct

    def success_rate(self, show_every=1000*10) -> float:
        """
        :param show_every: print success rate
        :return: % success rate
        """
        prct = float(len(self.success)) / float(len(self.attempt))

        if 0 == len(self.attempt) % show_every:
            print(f'success = {len(self.success)} rate % {prct}')

        return prct

    def as_json(self):
        return {'csv': self.csv,
                'label': self.label,
                'attempt': len(self.attempt),
                'success': self.success,
                'failed' : self.failed,
                'success_rate': self.success_rate(),
                'timestamp': self.timestamp,
                'hostname': self.hostname}

#######################################################################################################################
#
# FHIR Patient
#
#######################################################################################################################

def etl_patient(config: JobConfig) -> JobSummary:
    codebook = Codebook(config.path_codebook())

    job = JobSummary(config.label)

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
                common.error_fhir(subject)
                raise
    codebook.db.save(config.path_codebook())
    return job

#######################################################################################################################
#
# FHIR Encounter
#
#######################################################################################################################

def etl_visit(config:JobConfig) -> JobSummary:
    codebook = Codebook(config.path_codebook())

    job = JobSummary(config.label)

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
                job.failed.append(i2b2_visit)
                logging.error(f'ETL exception {e}')
                common.error_fhir(encounter)

    codebook.db.save(config.path_codebook())
    return job

#######################################################################################################################
#
# FHIR Observation (Lab Result)
#
#######################################################################################################################

def etl_lab(config:JobConfig) -> JobSummary:
    codebook = Codebook(config.path_codebook())

    job = JobSummary(config.label)

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
                job.failed.append(i2b2_lab)
                logging.error(f'ETL exception {e}')
                common.error_fhir(lab)
                raise

    codebook.db.save(config.path_codebook())
    return job

#######################################################################################################################
#
# FHIR DocumentReference
#
#######################################################################################################################

def etl_notes(config:JobConfig) -> JobSummary:
    codebook = Codebook(config.path_codebook())

    job = JobSummary(config.label)

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
                job.failed.append(i2b2_physician_note)
                logging.error(f'ETL exception {e}')
                common.error_fhir(docref)

    codebook.db.save(config.path_codebook())
    return job


#######################################################################################################################
#
# FHIR Condition
#
#######################################################################################################################

def etl_diagnosis(config:JobConfig) -> JobSummary:
    codebook = Codebook(config.path_codebook())

    job = JobSummary(config.label)

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
                job.failed.append(i2b2_observation)
                logging.error(f'ETL exception {e}')
                common.error_fhir(condition)
                raise

    codebook.db.save(config.path_codebook())
    return job

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

        i2b2.etl.etl_patient()
        i2b2.etl.etl_visit()
        i2b2.etl.etl_lab()
        i2b2.etl.etl_notes()
        i2b2.etl.etl_diagnosis()

if __name__ == '__main__':
    main(sys.argv[1:])

