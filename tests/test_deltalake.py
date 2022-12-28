"""Tests for Delta Lake support"""

import os
import shutil
import tempfile
import unittest

import pandas
from pyspark.sql.utils import AnalysisException
from cumulus import config, formats, store


class TestDeltaLake(unittest.TestCase):
    """
    Test case for the Delta Lake format writer.

    i.e. tests for deltalake.py
    """

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        output_tempdir = tempfile.TemporaryDirectory()  # pylint: disable=consider-using-with
        cls.output_tempdir = output_tempdir
        cls.output_dir = output_tempdir.name

        # It is expensive to create a DeltaLakeFormat instance because of all the pyspark jar downloading etc.
        # So we only do it once per class suite. (And erase all folder contents per-test)
        cls.deltalake = formats.DeltaLakeFormat(store.Root(output_tempdir.name))

    def setUp(self):
        super().setUp()
        shutil.rmtree(self.output_dir, ignore_errors=True)
        self.job = config.JobSummary()

    @staticmethod
    def df(**kwargs) -> pandas.DataFrame:
        """
        Creates a dummy DataFrame with ids & values equal to each kwarg provided.
        """
        rows = [{'id': k, 'value': v} for k, v in kwargs.items()]
        return pandas.DataFrame(rows)

    def store(self, df: pandas.DataFrame, batch: int = 10) -> None:
        """
        Writes a single batch of data to the data lake.

        :param df: the data to insert
        :param batch: which batch number this is, defaulting to 10 to avoid triggering any first/last batch logic
        """
        self.deltalake.store_patients(self.job, df, batch)

    def assert_lake_equal(self, df: pandas.DataFrame, when: int = None) -> None:
        table_path = os.path.join(self.output_dir, 'patient')

        reader = self.deltalake.spark.read
        if when is not None:
            reader = reader.option('versionAsOf', when)

        table_df = reader.format('delta').load(table_path).sort('id').toPandas()
        self.assertDictEqual(df.to_dict(), table_df.to_dict())

    def test_creates_if_empty(self):
        """Verify that the lake is created when empty"""
        # sanity check that it doesn't exist yet
        with self.assertRaises(AnalysisException):
            self.assert_lake_equal(self.df())

        self.store(self.df(a=1))
        self.assert_lake_equal(self.df(a=1))

    def test_upsert(self):
        """Verify that we can update and insert data"""
        self.store(self.df(a=1, b=2))
        self.store(self.df(b=20, c=3))
        self.assert_lake_equal(self.df(a=1, b=20, c=3))
