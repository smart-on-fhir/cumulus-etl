"""ETL job config with summary"""

import os
from socket import gethostname

from cumulus import common, loaders, store


class JobConfig:
    """Configuration for an ETL job"""

    def __init__(
            self,
            loader: loaders.Loader,
            dir_input: str,
            store_format: store.Format,
            dir_phi: store.Root,
            comment: str = None,
            batch_size: int = 1,  # this default is never really used - overridden by command line args
    ):
        """
        :param loader: describes how input files were loaded (e.g. i2b2 or ndjson)
        :param dir_input: the actual folder to grab input files from, in ndjson format
        :param store_format: where to place output files and how, like ndjson
        :param dir_phi: where to place PHI build artifacts like the codebook
        """
        self._loader = loader  # only kept around for logging purposes, use dir_input to read data
        self.dir_input = dir_input
        self.format = store_format
        self.dir_phi = dir_phi
        self.timestamp = common.timestamp_filename()
        self.hostname = gethostname()
        self.comment = comment or ''
        self.batch_size = batch_size

    def path_codebook(self) -> str:
        return self.dir_phi.joinpath('codebook.json')

    def path_config(self) -> str:
        return os.path.join(self.dir_job_config(), 'job_config.json')

    def dir_job_config(self) -> str:
        path = self.format.root.joinpath(f'JobConfig/{self.timestamp}')
        self.format.root.makedirs(path)
        return path

    def as_json(self):
        return {
            'dir_input': self._loader.root.path,  # the original folder, rather than the temp dir holding deid files
            'dir_output': self.format.root.path,
            'dir_phi': self.dir_phi.path,
            'path': self.path_config(),
            'codebook': self.path_codebook(),
            'input_format': type(self._loader).__name__,
            'output_format': type(self.format).__name__,
            'comment': self.comment,
            'batch_size': self.batch_size,
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
