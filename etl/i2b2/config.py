import os
from etl import common, store
from etl.ctakes import ctakes_client

#######################################################################################################################
#
# ETL Job Config with Summary
#
#######################################################################################################################

class JobConfig:
    def __init__(self, dir_input, dir_output):
        self.dir_input = dir_input
        self.dir_output = dir_output
        self.timestamp = common.timestamp()
        self.hostname = common.gethostname()
        self.ctakes = ctakes_client.get_url_ctakes()

    def path_codebook(self) -> str:
        return store.path_json(self.dir_output, 'codebook.json')

    def path_config(self) -> str:
        return store.path_json(self.dir_output_config(), 'job_config.json')

    def dir_output_config(self):
        return store.path_root(self.dir_output, f'JobConfig_{self.timestamp}')

    def dir_output_patient(self, mrn:str) -> str:
        return store.path_patient_dir(self.dir_output, mrn)

    def dir_output_encounter(self, mrn:str, encounter_id) -> str:
        return store.path_encounter_dir(self.dir_output, mrn, encounter_id)

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
                'path': self.path_config(),
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
