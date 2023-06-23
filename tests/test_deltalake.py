"""Tests for Delta Lake support"""

import contextlib
import io
import os
import shutil
import tempfile

import pandas
import pytest
from pyspark.sql.utils import AnalysisException

from cumulus_etl import store
from cumulus_etl.formats.deltalake import DeltaLakeFormat
from tests import utils


class TestDeltaLake(utils.AsyncTestCase):
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
        cls.root = store.Root(output_tempdir.name)

        # It is expensive to initialize DeltaLakeFormat because of all the pyspark jar downloading etc.
        # So we only do it once per class suite. (And erase all folder contents per-test)
        DeltaLakeFormat.initialize_class(cls.root)

    def setUp(self):
        super().setUp()
        shutil.rmtree(self.output_dir, ignore_errors=True)

    @staticmethod
    def df(**kwargs) -> pandas.DataFrame:
        """
        Creates a dummy DataFrame with ids & values equal to each kwarg provided.
        """
        rows = [{"id": k, "value": v} for k, v in kwargs.items()]
        return pandas.DataFrame(rows)

    def store(self, df: pandas.DataFrame, batch: int = 10, group_field: str = None, resource_type: str = None) -> None:
        """
        Writes a single batch of data to the data lake.

        :param df: the data to insert
        :param batch: which batch number this is, defaulting to 10 to avoid triggering any first/last batch logic
        :param group_field: a group field name, used to delete non-matching group rows
        :param resource_type: the name of the resource being stored
        """
        deltalake = DeltaLakeFormat(self.root, "patient", group_field=group_field, resource_type=resource_type)
        deltalake.write_records(df, batch)

    def assert_lake_equal(self, df: pandas.DataFrame) -> None:
        table_path = os.path.join(self.output_dir, "patient")
        table_records = utils.read_delta_lake(table_path)
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
        self.assert_lake_equal(self.df(a={"one": 1}, b={"one": 1, "two": 2}))

    def test_missing_field(self):
        """
        Verify that fields can be missing.

        By default, Delta Lake does not allow any additions or subtractions.
        """
        self.store(self.df(a={"one": 1, "two": 2}))
        self.store(self.df(b={"one": 1}))
        self.assert_lake_equal(self.df(a={"one": 1, "two": 2}, b={"one": 1}))

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
        reader = DeltaLakeFormat.spark.read
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

    def test_merged_schema_for_resource(self):
        """Verify that the lake's schemas is derived from the full resource schema and also merged with real data"""
        # This dataframe will:
        # (A) be missing 99% of the schema
        # (B) include a nested FHIR struct that our default (non-recursive) resource schemas will not include
        # (C) include whole new non-FHIR columns
        # Ideally the final result includes those two new elements and all the normal FHIR schema.
        rows = [
            # We specifically make the new columns int & bool types because pandas can have difficulty with nullable
            # versions of those columns -- this will verify that the schema arrives back out correctly.
            {"id": "int-row", "contact": [{"name": {"text": "Jane Doe"}}], "newIntColumn": 2000},
            {"id": "bool-row", "contact": [{"name": {"text": "John Doe"}}], "newBoolColumn": True},
        ]
        self.store(pandas.DataFrame(rows), resource_type="Patient")

        table_path = os.path.join(self.output_dir, "patient")
        table_df = DeltaLakeFormat.spark.read.format("delta").load(table_path)

        # pyspark's printSchema function is much more readable/concise than other schema inspection methods,
        # but it doesn't expose the internal calls it makes to produce that - so let's just capture stdout.
        with contextlib.redirect_stdout(io.StringIO()) as schema_tree:
            table_df.printSchema()

        self.assertEqual(
            """root
 |-- id: string (nullable = true)
 |-- contact: array (nullable = true)
 |    |-- element: struct (containsNull = true)
 |    |    |-- name: struct (nullable = true)
 |    |    |    |-- text: string (nullable = true)
 |    |    |-- id: string (nullable = true)
 |    |    |-- gender: string (nullable = true)
 |-- newIntColumn: long (nullable = true)
 |-- newBoolColumn: boolean (nullable = true)
 |-- implicitRules: string (nullable = true)
 |-- language: string (nullable = true)
 |-- meta: struct (nullable = true)
 |    |-- id: string (nullable = true)
 |    |-- lastUpdated: string (nullable = true)
 |    |-- profile: array (nullable = true)
 |    |    |-- element: string (containsNull = true)
 |    |-- source: string (nullable = true)
 |    |-- versionId: string (nullable = true)
 |-- contained: array (nullable = true)
 |    |-- element: struct (containsNull = true)
 |    |    |-- id: string (nullable = true)
 |    |    |-- implicitRules: string (nullable = true)
 |    |    |-- language: string (nullable = true)
 |-- extension: array (nullable = true)
 |    |-- element: struct (containsNull = true)
 |    |    |-- id: string (nullable = true)
 |    |    |-- url: string (nullable = true)
 |    |    |-- valueBase64Binary: string (nullable = true)
 |    |    |-- valueBoolean: boolean (nullable = true)
 |    |    |-- valueCanonical: string (nullable = true)
 |    |    |-- valueCode: string (nullable = true)
 |    |    |-- valueDate: string (nullable = true)
 |    |    |-- valueDateTime: string (nullable = true)
 |    |    |-- valueDecimal: double (nullable = true)
 |    |    |-- valueId: string (nullable = true)
 |    |    |-- valueInstant: string (nullable = true)
 |    |    |-- valueInteger: long (nullable = true)
 |    |    |-- valueMarkdown: string (nullable = true)
 |    |    |-- valueOid: string (nullable = true)
 |    |    |-- valuePositiveInt: long (nullable = true)
 |    |    |-- valueString: string (nullable = true)
 |    |    |-- valueTime: string (nullable = true)
 |    |    |-- valueUnsignedInt: long (nullable = true)
 |    |    |-- valueUri: string (nullable = true)
 |    |    |-- valueUrl: string (nullable = true)
 |    |    |-- valueUuid: string (nullable = true)
 |-- modifierExtension: array (nullable = true)
 |    |-- element: struct (containsNull = true)
 |    |    |-- id: string (nullable = true)
 |    |    |-- url: string (nullable = true)
 |    |    |-- valueBase64Binary: string (nullable = true)
 |    |    |-- valueBoolean: boolean (nullable = true)
 |    |    |-- valueCanonical: string (nullable = true)
 |    |    |-- valueCode: string (nullable = true)
 |    |    |-- valueDate: string (nullable = true)
 |    |    |-- valueDateTime: string (nullable = true)
 |    |    |-- valueDecimal: double (nullable = true)
 |    |    |-- valueId: string (nullable = true)
 |    |    |-- valueInstant: string (nullable = true)
 |    |    |-- valueInteger: long (nullable = true)
 |    |    |-- valueMarkdown: string (nullable = true)
 |    |    |-- valueOid: string (nullable = true)
 |    |    |-- valuePositiveInt: long (nullable = true)
 |    |    |-- valueString: string (nullable = true)
 |    |    |-- valueTime: string (nullable = true)
 |    |    |-- valueUnsignedInt: long (nullable = true)
 |    |    |-- valueUri: string (nullable = true)
 |    |    |-- valueUrl: string (nullable = true)
 |    |    |-- valueUuid: string (nullable = true)
 |-- text: struct (nullable = true)
 |    |-- id: string (nullable = true)
 |    |-- div: string (nullable = true)
 |    |-- status: string (nullable = true)
 |-- active: boolean (nullable = true)
 |-- address: array (nullable = true)
 |    |-- element: struct (containsNull = true)
 |    |    |-- id: string (nullable = true)
 |    |    |-- city: string (nullable = true)
 |    |    |-- country: string (nullable = true)
 |    |    |-- district: string (nullable = true)
 |    |    |-- line: array (nullable = true)
 |    |    |    |-- element: string (containsNull = true)
 |    |    |-- postalCode: string (nullable = true)
 |    |    |-- state: string (nullable = true)
 |    |    |-- text: string (nullable = true)
 |    |    |-- type: string (nullable = true)
 |    |    |-- use: string (nullable = true)
 |-- birthDate: string (nullable = true)
 |-- communication: array (nullable = true)
 |    |-- element: struct (containsNull = true)
 |    |    |-- id: string (nullable = true)
 |    |    |-- preferred: boolean (nullable = true)
 |-- deceasedBoolean: boolean (nullable = true)
 |-- deceasedDateTime: string (nullable = true)
 |-- gender: string (nullable = true)
 |-- generalPractitioner: array (nullable = true)
 |    |-- element: struct (containsNull = true)
 |    |    |-- id: string (nullable = true)
 |    |    |-- display: string (nullable = true)
 |    |    |-- reference: string (nullable = true)
 |    |    |-- type: string (nullable = true)
 |-- identifier: array (nullable = true)
 |    |-- element: struct (containsNull = true)
 |    |    |-- id: string (nullable = true)
 |    |    |-- system: string (nullable = true)
 |    |    |-- use: string (nullable = true)
 |    |    |-- value: string (nullable = true)
 |-- link: array (nullable = true)
 |    |-- element: struct (containsNull = true)
 |    |    |-- id: string (nullable = true)
 |    |    |-- type: string (nullable = true)
 |-- managingOrganization: struct (nullable = true)
 |    |-- id: string (nullable = true)
 |    |-- display: string (nullable = true)
 |    |-- reference: string (nullable = true)
 |    |-- type: string (nullable = true)
 |-- maritalStatus: struct (nullable = true)
 |    |-- id: string (nullable = true)
 |    |-- text: string (nullable = true)
 |-- multipleBirthBoolean: boolean (nullable = true)
 |-- multipleBirthInteger: long (nullable = true)
 |-- name: array (nullable = true)
 |    |-- element: struct (containsNull = true)
 |    |    |-- id: string (nullable = true)
 |    |    |-- family: string (nullable = true)
 |    |    |-- given: array (nullable = true)
 |    |    |    |-- element: string (containsNull = true)
 |    |    |-- prefix: array (nullable = true)
 |    |    |    |-- element: string (containsNull = true)
 |    |    |-- suffix: array (nullable = true)
 |    |    |    |-- element: string (containsNull = true)
 |    |    |-- text: string (nullable = true)
 |    |    |-- use: string (nullable = true)
 |-- photo: array (nullable = true)
 |    |-- element: struct (containsNull = true)
 |    |    |-- id: string (nullable = true)
 |    |    |-- contentType: string (nullable = true)
 |    |    |-- creation: string (nullable = true)
 |    |    |-- data: string (nullable = true)
 |    |    |-- hash: string (nullable = true)
 |    |    |-- language: string (nullable = true)
 |    |    |-- size: long (nullable = true)
 |    |    |-- title: string (nullable = true)
 |    |    |-- url: string (nullable = true)
 |-- telecom: array (nullable = true)
 |    |-- element: struct (containsNull = true)
 |    |    |-- id: string (nullable = true)
 |    |    |-- rank: long (nullable = true)
 |    |    |-- system: string (nullable = true)
 |    |    |-- use: string (nullable = true)
 |    |    |-- value: string (nullable = true)

""",
            schema_tree.getvalue(),
        )

    def test_group_field(self):
        """Verify that we can safely delete some data from the lake using groups"""
        self.store(
            self.df(aa={"group": "X", "val": 5}, ab={"group": "X", "val": 10}, b={"group": "Y", "val": 1}),
            group_field="value.group",
        )
        self.store(
            # Add a quote as part of the Z group identifier, just to confirm we escape these strings
            self.df(ab={"group": "X", "val": 11}, ac={"group": "X", "val": 16}, c={"group": 'Z"', "val": 2}),
            group_field="value.group",
        )
        self.assert_lake_equal(
            self.df(
                ab={"group": "X", "val": 11},
                ac={"group": "X", "val": 16},
                b={"group": "Y", "val": 1},
                c={"group": 'Z"', "val": 2},
            )
        )
