"""Tests for Delta Lake support"""

import os
import shutil
import tempfile
import unittest
from typing import List

import pandas
import pytest
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
        rows = [{"id": k, "value": v} for k, v in kwargs.items()]
        return pandas.DataFrame(rows)

    def store(self, df: pandas.DataFrame, batch: int = 10) -> None:
        """
        Writes a single batch of data to the data lake.

        :param df: the data to insert
        :param batch: which batch number this is, defaulting to 10 to avoid triggering any first/last batch logic
        """
        self.deltalake.store_patients(self.job, df, batch)

    @staticmethod
    def spark_to_records(table) -> List[dict]:
        table_df = table.toPandas()
        table_records = table_df.to_dict(orient="records")
        for r in table_records:
            # convert spark Row to dict (it's annoying that toPandas() doesn't do that for us)
            if hasattr(r["value"], "asDict"):
                r["value"] = r["value"].asDict()
        return table_records

    def assert_lake_equal(self, df: pandas.DataFrame, when: int = None) -> None:
        table_path = os.path.join(self.output_dir, "patient")

        reader = self.deltalake.spark.read
        if when is not None:
            reader = reader.option("versionAsOf", when)

        table_records = self.spark_to_records(reader.format("delta").load(table_path).sort("id"))
        self.assertListEqual(df.to_dict(orient="records"), table_records)

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

    def test_added_field(self):
        """
        Verify that new fields can be added.

        By default, Delta Lake does not allow any additions or subtractions.
        """
        self.store(self.df(a={"one": 1}))
        self.store(self.df(b={"one": 1, "two": 2}))
        self.assert_lake_equal(self.df(a={"one": 1, "two": None}, b={"one": 1, "two": 2}))

    def test_missing_field(self):
        """
        Verify that fields can be missing.

        By default, Delta Lake does not allow any additions or subtractions.
        """
        self.store(self.df(a={"one": 1, "two": 2}))
        self.store(self.df(b={"one": 1}))
        self.assert_lake_equal(self.df(a={"one": 1, "two": 2}, b={"one": 1, "two": None}))

    # This currently fails because delta silently drops field data that can't be converted to the correct type.
    # Here is a request to change this behavior into an error: https://github.com/delta-io/delta/issues/1551
    # See https://github.com/smart-on-fhir/cumulus-etl/issues/133 for some discussion of this issue.
    @pytest.mark.xfail
    def test_altered_field(self):
        """Verify that field types cannot be altered."""
        self.store(self.df(a={"one": 1}))
        self.store(self.df(b={"one": "string"}))  # should error out / not update
        self.assert_lake_equal(self.df(a={"one": 1}))

    def test_schema_has_names(self):
        """Verify that the lake's schemas has valid nested names, which may not always happen with spark"""
        self.store(self.df(a=[{"one": 1, "two": 2}]))

        table_path = os.path.join(self.output_dir, "patient")
        reader = self.deltalake.spark.read
        table_df = reader.format("delta").load(table_path)
        self.assertDictEqual(
            {
                "type": "struct",
                "fields": [
                    {"metadata": {}, "name": "id", "nullable": True, "type": "string"},
                    {
                        "metadata": {},
                        "name": "value",
                        "nullable": True,
                        "type": {
                            "containsNull": True,
                            "elementType": {
                                "fields": [
                                    {
                                        "metadata": {},
                                        "name": "one",
                                        "nullable": True,
                                        "type": "long",
                                    },
                                    {
                                        "metadata": {},
                                        "name": "two",
                                        "nullable": True,
                                        "type": "long",
                                    },
                                ],
                                "type": "struct",
                            },
                            "type": "array",
                        },
                    },
                ],
            },
            table_df.schema.jsonValue(),
        )
