"""ETL job config with summary"""

import os
from socket import gethostname

from cumulus import common, store


class JobConfig:
    """Configuration for an ETL job"""

    def __init__(self, dir_input, dir_output):
        self.dir_input = dir_input
        self.dir_output = dir_output
        self.timestamp = common.timestamp()
        self.hostname = gethostname()

    def path_codebook(self) -> str:
        return store.path_file(self.dir_output, 'codebook.json')

    def path_config(self) -> str:
        return store.path_file(self.dir_output_config(), 'job_config.json')

    def dir_output_config(self):
        return store.path_root(self.dir_output, f'JobConfig_{self.timestamp}')

    def dir_output_patient(self, mrn: str) -> str:
        return store.path_patient_dir(self.dir_output, mrn)

    def dir_output_note(self, mrn, md5sum: str) -> str:
        return store.path_note_dir(self.dir_output, mrn, md5sum)

    def dir_output_encounter(self, mrn: str, encounter_id) -> str:
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
        return {
            'dir_input': self.dir_input,
            'dir_output': self.dir_output,
            'path': self.path_config(),
            'codebook': self.path_codebook(),
            'list_csv_patient': self.list_csv_patient(),
            'list_csv_visit': self.list_csv_visit(),
            'list_csv_lab': self.list_csv_lab(),
            'list_csv_notes': self.list_csv_notes(),
            'list_csv_diagnosis': self.list_csv_diagnosis()
        }


class JobSummary:
    """Summary of an ETL job's results"""

    def __init__(self, label=None):
        self.label = label
        self.csv = []
        self.attempt = []
        self.success = []
        self.failed = []
        self.timestamp = common.timestamp_datetime()
        self.hostname = gethostname()

    def success_rate(self, show_every=1000 * 10) -> float:
        """
        :param show_every: print success rate
        :return: % success rate
        """
        if not self.attempt:
            return 1.0

        prct = float(len(self.success)) / float(len(self.attempt))

        if 0 == len(self.attempt) % show_every:
            print(f'success = {len(self.success)} rate % {prct}')

        return prct

    def as_json(self):
        return {
            'csv': self.csv,
            'label': self.label,
            'attempt': len(self.attempt),
            'success': self.success,
            'failed': self.failed,
            'success_rate': self.success_rate(),
            'timestamp': self.timestamp,
            'hostname': self.hostname
        }
