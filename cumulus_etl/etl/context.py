"""
ETL job context, holding some persistent state between runs.
"""

import datetime

from cumulus_etl import common


class JobContext:
    """
    Context for an ETL job.

    This is not really settings or config in the sense of user-derived values.
    A prime example is "last successful run time" which the next run might want to use as part of its work
    (like to only extract the changed data since the last run) but is otherwise ephemeral information.

    Another possible (but not yet implemented) use might be to store some past config values,
    like the last used output format. To either give the user a chance to correct a mistake or to know
    how to change from one format to another.

    This is stored in the phi/build directory and is thus safe to store possible PHI.
    """

    _LAST_SUCCESSFUL_DATETIME = "last_successful_datetime"
    _LAST_SUCCESSFUL_INPUT_DIR = "last_successful_input_dir"
    _LAST_SUCCESSFUL_OUTPUT_DIR = "last_successful_output_dir"

    def __init__(self, path: str):
        """
        :param path: path to context file
        """
        self._path: str = path
        try:
            self._data = common.read_json(path)
        except (FileNotFoundError, PermissionError):
            self._data = {}

    @property
    def last_successful_datetime(self) -> datetime.datetime | None:
        value = self._data.get(self._LAST_SUCCESSFUL_DATETIME)
        if value is not None:
            return datetime.datetime.fromisoformat(value)
        return None

    @last_successful_datetime.setter
    def last_successful_datetime(self, value: datetime.datetime) -> None:
        self._data[self._LAST_SUCCESSFUL_DATETIME] = value.isoformat()

    @property
    def last_successful_input_dir(self) -> str | None:
        return self._data.get(self._LAST_SUCCESSFUL_INPUT_DIR)

    @last_successful_input_dir.setter
    def last_successful_input_dir(self, value: str) -> None:
        self._data[self._LAST_SUCCESSFUL_INPUT_DIR] = value

    @property
    def last_successful_output_dir(self) -> str | None:
        return self._data.get(self._LAST_SUCCESSFUL_OUTPUT_DIR)

    @last_successful_output_dir.setter
    def last_successful_output_dir(self, value: str) -> None:
        self._data[self._LAST_SUCCESSFUL_OUTPUT_DIR] = value

    def save(self) -> None:
        # pretty-print this since it isn't large
        common.write_json(self._path, self.as_json(), indent=4)

    def as_json(self) -> dict:
        return dict(self._data)
