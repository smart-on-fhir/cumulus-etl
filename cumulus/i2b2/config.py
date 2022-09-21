"""ETL job config with summary"""

import os
from socket import gethostname

from cumulus import common, store


class JobConfig:
    """Configuration for an ETL job"""

    def __init__(self, dir_input, dir_output, dir_cache, config_store):
        self.dir_input = dir_input
        self.dir_output = dir_output
        self.dir_cache = dir_cache
        self.store = config_store
        self.timestamp = common.timestamp()
        self.hostname = gethostname()

    def path_codebook(self) -> str:
        return store.path_file(self.dir_cache, 'codebook.json')

    def path_config(self) -> str:
        return store.path_file(self.dir_cache_config(), 'job_config.json')

    def dir_cache_config(self):
        return store.path_root(self.dir_cache, f'JobConfig_{self.timestamp}')

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
            'dir_cache': self.dir_cache,
            'path': self.path_config(),
            'codebook': self.path_codebook(),
            'list_csv_patient': self.list_csv_patient(),
            'list_csv_visit': self.list_csv_visit(),
            'list_csv_lab': self.list_csv_lab(),
            'list_csv_notes': self.list_csv_notes(),
            'list_csv_diagnosis': self.list_csv_diagnosis(),
            'store_class': type(self.store).__name__,
        }


class JobSummary:
    """Summary of an ETL job's results"""

    def __init__(self, label=None):
        self.label = label
        self.csv = []
        self.attempt = 0
        self.success = 0
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

        prct = float(self.success) / float(self.attempt)

        if 0 == self.attempt % show_every:
            print(f'success = {self.success} rate % {prct}')

        return prct

    def as_json(self):
        return {
            'csv': self.csv,
            'label': self.label,
            'attempt': self.attempt,
            'success': self.success,
            'failed': self.failed,
            'success_rate': self.success_rate(),
            'timestamp': self.timestamp,
            'hostname': self.hostname
        }
