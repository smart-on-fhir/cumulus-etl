from cumulus_etl import common


class OrderedNdjsonWriter:
    """
    Convenience context manager to write multiple objects to a ndjson file in order.

    Specifically, it will keep the output in the intended order, even if lines are provided
    out of order.

    Note that this is not atomic - partial writes will make it to the target file.
    And queued writes may not make it to the target file at all, if interrupted.
    """

    def __init__(self, path: str, **kwargs):
        self._writer = common.NdjsonWriter(path, **kwargs)
        self._queued_rows: dict[int, dict] = {}
        self._current_num: int = 0

    def __enter__(self):
        self._writer.__enter__()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self._writer.__exit__(exc_type, exc_value, traceback)

    def _process_queue(self) -> None:
        while self._current_num in self._queued_rows:
            self._writer.write(self._queued_rows.pop(self._current_num))
            self._current_num += 1

    def write(self, index: int, obj: dict) -> None:
        """
        Writes the object to the file at the specified row number.

        May hold the row in memory until previous rows can be written first.
        """
        # We just queue the rows in memory until we write them out. Our expectation is that we
        # won't hold so many in the queue that it will be a memory issue.
        #
        # If this does turn out to be a problem, we can explore other solutions like keeping
        # track of byte indices for missing rows and seeking to that location, then inserting data
        # (but you'd have to be careful to shift-down/re-write the later row data and update
        # other byte indices).
        self._queued_rows[index] = obj
        self._process_queue()
