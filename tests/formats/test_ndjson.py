"""Tests for ndjson output format support"""

import os

import ddt

from cumulus_etl import formats, store
from cumulus_etl.formats.ndjson import NdjsonFormat
from tests import utils


@ddt.ddt
class TestNdjsonFormat(utils.AsyncTestCase):
    """
    Test case for the ndjson format writer.

    i.e. tests for ndjson.py
    """

    def setUp(self):
        super().setUp()
        self.output_tempdir = self.make_tempdir()
        self.root = store.Root(self.output_tempdir)
        NdjsonFormat.initialize_class(self.root)

    @staticmethod
    def df(**kwargs) -> list[dict]:
        """
        Creates a dummy Table with ids & values equal to each kwarg provided.
        """
        return [{"id": k, "value": v} for k, v in kwargs.items()]

    def store(
        self,
        rows: list[dict],
        batch_index: int = 10,
    ) -> bool:
        """
        Writes a single batch of data to the output dir.

        :param rows: the data to insert
        :param batch_index: which batch number this is, defaulting to 10 to avoid triggering any first/last batch logic
        """
        ndjson = NdjsonFormat(self.root, "condition")
        batch = formats.Batch(rows, index=batch_index)
        return ndjson.write_records(batch)

    @ddt.data(
        (None, True),
        ([], True),
        (["condition.1234.ndjson", "condition.22.ndjson"], True),
        (["condition.ndjson"], False),
        (["condition.000.parquet"], False),
        (["patient.000.ndjson"], False),
    )
    @ddt.unpack
    def test_handles_existing_files(self, files: None | list[str], is_ok: bool):
        """Verify that we bail out if any weird files already exist in the output"""
        dbpath = self.root.joinpath("condition")
        if files is not None:
            os.makedirs(dbpath)
            for file in files:
                with open(f"{dbpath}/{file}", "w", encoding="utf8") as f:
                    f.write('{"id": "A"}')

        if is_ok:
            self.store([{"id": "B"}], batch_index=0)
            self.assertEqual(["condition.000.ndjson"], os.listdir(dbpath))
        else:
            with self.assertRaises(SystemExit):
                self.store([{"id": "B"}])
            self.assertEqual(files or [], os.listdir(dbpath))
