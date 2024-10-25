"""ETL job config with summary"""

import datetime
import os
from socket import gethostname

from cumulus_etl import common, fhir, formats, store


class JobConfig:
    """
    Configuration for an entire ETL run.

    This only store simple data structures, but can act as a factory for more interesting ones.
    For example, this config holds the output format slug string, but can spit out a Format class for you.
    This architecture is designed to make it easier to pass a JobConfig to multiple processes.
    """

    def __init__(
        self,
        dir_input_orig: str,  # original user-input path
        dir_input_deid: str,  # temporary dir where we are reading the de-identified data from
        dir_output: str,
        dir_phi: str,
        input_format: str,
        output_format: str,
        client: fhir.FhirClient,
        timestamp: datetime.datetime | None = None,
        comment: str | None = None,
        batch_size: int = 1,  # this default is never really used - overridden by command line args
        ctakes_overrides: str | None = None,
        dir_errors: str | None = None,
        tasks: list[str] | None = None,
        export_group_name: str | None = None,
        export_datetime: datetime.datetime | None = None,
        export_url: str | None = None,
        deleted_ids: dict[str, set[str]] | None = None,
    ):
        self._dir_input_orig = dir_input_orig
        self.dir_input = dir_input_deid
        self._dir_output = dir_output
        self.dir_phi = dir_phi
        self._input_format = input_format
        self._output_format = output_format
        self.dir_errors = dir_errors
        self.client = client
        self.timestamp = timestamp
        self.hostname = gethostname()
        self.comment = comment or ""
        self.batch_size = batch_size
        self.ctakes_overrides = ctakes_overrides
        self.tasks = tasks or []
        self.export_group_name = export_group_name
        self.export_datetime = export_datetime
        self.export_url = export_url
        self.deleted_ids = deleted_ids or {}

        # initialize format class
        self._output_root = store.Root(self._dir_output, create=True)
        self._format_class = formats.get_format_class(self._output_format)
        self._format_class.initialize_class(self._output_root)

    def create_formatter(self, dbname: str, **kwargs) -> formats.Format:
        return self._format_class(self._output_root, dbname, **kwargs)

    def path_config(self) -> str:
        return os.path.join(self.dir_job_config(), "job_config.json")

    def dir_job_config(self) -> str:
        timestamp_dir = common.timestamp_filename(self.timestamp)
        path = self._output_root.joinpath(f"JobConfig/{timestamp_dir}")
        self._output_root.makedirs(path)
        return path

    def as_json(self):
        return {
            "dir_input": self._dir_input_orig,  # the original folder, rather than the temp dir holding deid files
            "dir_output": self._dir_output,
            "dir_phi": self.dir_phi,
            "path": self.path_config(),
            "input_format": self._input_format,
            "output_format": self._output_format,
            "comment": self.comment,
            "batch_size": self.batch_size,
            "tasks": ",".join(self.tasks),
            "export_group_name": self.export_group_name,
            "export_timestamp": self.export_datetime and self.export_datetime.isoformat(),
            "export_url": self.export_url,
        }


class JobSummary:
    """Summary of an ETL job's results"""

    def __init__(self, label=None):
        self.label = label
        self.attempt = 0
        self.success = 0
        self.had_errors = False
        self.timestamp = common.timestamp_datetime()
        self.hostname = gethostname()

    def success_rate(self) -> float:
        """
        :return: % success rate (0.0 to 1.0)
        """
        if not self.attempt:
            return 1.0

        return float(self.success) / float(self.attempt)

    def as_json(self):
        return {
            "label": self.label,
            "attempt": self.attempt,
            "success": self.success,
            "success_rate": self.success_rate(),
            "had_errors": self.had_errors,
            "timestamp": self.timestamp,
            "hostname": self.hostname,
        }
