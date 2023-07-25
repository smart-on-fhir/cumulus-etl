"""Tests for Delta Lake support"""

import contextlib
import io
import os
import shutil
import tempfile

import ddt
import pyarrow
import pyspark.sql
from pyspark.sql.utils import AnalysisException

from cumulus_etl import formats, store
from cumulus_etl.formats.deltalake import DeltaLakeFormat
from tests import utils


@ddt.ddt
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
    def df(**kwargs) -> list[dict]:
        """
        Creates a dummy Table with ids & values equal to each kwarg provided.
        """
        return [{"id": k, "value": v} for k, v in kwargs.items()]

    def get_spark_schema(self, df: pyspark.sql.DataFrame) -> str:
        # pyspark's printSchema function is much more readable/concise than other schema inspection methods,
        # but it doesn't expose the internal calls it makes to produce that - so let's just capture stdout.
        with contextlib.redirect_stdout(io.StringIO()) as schema_tree:
            df.printSchema()
        return schema_tree.getvalue().strip()

    def store(
        self, rows: list[dict], batch_index: int = 10, schema: pyarrow.Schema = None, group_field: str = None
    ) -> bool:
        """
        Writes a single batch of data to the data lake.

        :param rows: the data to insert
        :param batch_index: which batch number this is, defaulting to 10 to avoid triggering any first/last batch logic
        :param schema: the batch schema, in pyarrow format
        :param group_field: a group field name, used to delete non-matching group rows
        """
        deltalake = DeltaLakeFormat(self.root, "patient", group_field=group_field)
        return deltalake.write_records(formats.Batch(rows, index=batch_index, schema=schema))

    def assert_lake_equal(self, rows: list[dict]) -> None:
        table_path = os.path.join(self.output_dir, "patient")
        rows_by_id = sorted(rows, key=lambda x: x["id"])
        self.assertListEqual(rows_by_id, utils.read_delta_lake(table_path))

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

    def test_last_updated_support(self):
        """Verify that we don't knowingly overwrite current data with old data"""
        past = "2000-01-01T01:00:00.000-00:00"
        past_with_offset = "2000-01-01T04:00:00.000+03:00"  # lexically later than now
        now = "2000-01-01T02:00:00.000Z"
        now_without_zed = "2000-01-01T02:00:00.000-00:00"  # lexically earlier than now
        future = "2000-01-01T03:00:00.000-00:00"
        future_with_offset = "2000-01-01T00:00:00.000-03:00"  # lexically earlier than now
        self.store(  # original table (all "value" fields are 1)
            [
                {"id": "past", "meta": {"lastUpdated": past}, "value": 1},
                {"id": "past-with-offset", "meta": {"lastUpdated": past_with_offset}, "value": 1},
                {"id": "now", "meta": {"lastUpdated": now}, "value": 1},
                {"id": "now-without-zed", "meta": {"lastUpdated": now_without_zed}, "value": 1},
                {"id": "future", "meta": {"lastUpdated": future}, "value": 1},
                {"id": "future-with-offset", "meta": {"lastUpdated": future_with_offset}, "value": 1},
                # this next one is off-spec (lastUpdated must provide at least seconds), but still
                {"id": "future-partial", "meta": {"lastUpdated": "3000-01-01"}, "value": 1},
                {"id": "missing-date-table", "meta": {}, "value": 1},
                {"id": "missing-date-update", "meta": {"lastUpdated": future}, "value": 1},
                {"id": "missing-date-both", "meta": {}, "value": 1},
                {"id": "missing-meta-table", "value": 1},
                {"id": "missing-meta-update", "meta": {"lastUpdated": future}, "value": 1},
                {"id": "missing-meta-both", "value": 1},
                {"id": "unmatched-table", "value": 1},
            ]
        )
        self.store(  # update (all "value" fields are 2 and all "lastUpdated" fields are "now")
            [
                {"id": "past", "meta": {"lastUpdated": now}, "value": 2},
                {"id": "past-with-offset", "meta": {"lastUpdated": now}, "value": 2},
                {"id": "now", "meta": {"lastUpdated": now}, "value": 2},
                {"id": "now-without-zed", "meta": {"lastUpdated": now}, "value": 2},
                {"id": "future", "meta": {"lastUpdated": now}, "value": 2},
                {"id": "future-with-offset", "meta": {"lastUpdated": now}, "value": 2},
                {"id": "future-partial", "meta": {"lastUpdated": now}, "value": 2},
                {"id": "missing-date-table", "meta": {"lastUpdated": now}, "value": 2},
                {"id": "missing-date-update", "meta": {}, "value": 2},
                {"id": "missing-date-both", "meta": {}, "value": 2},
                {"id": "missing-meta-table", "meta": {"lastUpdated": now}, "value": 2},
                {"id": "missing-meta-update", "value": 2},
                {"id": "missing-meta-both", "value": 2},
                {"id": "unmatched-update", "value": 2},
            ]
        )
        self.assert_lake_equal(
            [
                {"id": "past", "meta": {"lastUpdated": now}, "value": 2},
                {"id": "past-with-offset", "meta": {"lastUpdated": now}, "value": 2},
                {"id": "now", "meta": {"lastUpdated": now}, "value": 1},
                {"id": "now-without-zed", "meta": {"lastUpdated": now_without_zed}, "value": 1},
                {"id": "future", "meta": {"lastUpdated": future}, "value": 1},
                {"id": "future-with-offset", "meta": {"lastUpdated": future_with_offset}, "value": 1},
                {"id": "future-partial", "meta": {"lastUpdated": "3000-01-01"}, "value": 1},
                {"id": "missing-date-table", "meta": {"lastUpdated": now}, "value": 2},
                {"id": "missing-date-update", "meta": {}, "value": 2},
                {"id": "missing-date-both", "meta": {}, "value": 2},
                {"id": "missing-meta-table", "meta": {"lastUpdated": now}, "value": 2},
                {"id": "missing-meta-update", "meta": {}, "value": 2},
                {"id": "missing-meta-both", "meta": {}, "value": 2},
                {"id": "unmatched-table", "value": 1},
                {"id": "unmatched-update", "value": 2},
            ]
        )

    def test_missing_field(self):
        """
        Verify that fields can be missing.

        By default, Delta Lake does not allow any additions or subtractions.
        """
        self.store(self.df(a={"one": 1, "two": 2}))
        self.store(self.df(b={"one": 1}))
        self.assert_lake_equal(self.df(a={"one": 1, "two": 2}, b={"one": 1}))

    def test_altered_field(self):
        """Verify that field types cannot be altered, as long as we have a schema."""
        schema = pyarrow.schema(
            [
                pyarrow.field("id", pyarrow.string()),
                pyarrow.field("value", pyarrow.int32()),
            ]
        )
        self.assertTrue(self.store(self.df(a=1), schema=schema))
        self.assertFalse(self.store(self.df(b="string"), schema=schema))
        self.assert_lake_equal(self.df(a=1))

        # And just confirm the mildly buggy behavior that Delta Lake will silently ignore
        # altered types when we don't force a schema. This is one reason we like to force a schema!
        # We don't desire or care about this behavior, but just testing it here as a sort of documentation,
        # in case they ever fix that, and then we get to know about it.
        # Upstream issue: https://github.com/delta-io/delta/issues/1551
        self.assertTrue(self.store(self.df(b="string")))
        self.assert_lake_equal([{"id": "a", "value": 1}, {"id": "b"}])

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
        """Verify that the lake's schemas is updated over time as new fields appear"""
        rows = [
            {"id": "bare-row"},
            {"id": "int-row", "contact": [{"name": {"text": "Jane Doe"}}], "newIntColumn": 2000},
            {"id": "bool-row", "contact": [{"name": {"given": ["John"]}}], "newBoolColumn": True},
        ]
        for row in rows:
            self.store([row])

        table_path = os.path.join(self.output_dir, "patient")
        table_df = DeltaLakeFormat.spark.read.format("delta").load(table_path)

        schema_tree = self.get_spark_schema(table_df)

        self.assertEqual(
            """root
 |-- id: string (nullable = true)
 |-- contact: array (nullable = true)
 |    |-- element: struct (containsNull = true)
 |    |    |-- name: struct (nullable = true)
 |    |    |    |-- text: string (nullable = true)
 |    |    |    |-- given: array (nullable = true)
 |    |    |    |    |-- element: string (containsNull = true)
 |-- newIntColumn: long (nullable = true)
 |-- newBoolColumn: boolean (nullable = true)""",
            schema_tree,
        )

    @ddt.data(
        # In general, the first type used wins
        (pyarrow.int64(), 2000, pyarrow.int32(), 2000, "long", 2000),
        (pyarrow.int32(), 2000, pyarrow.int64(), 2000, "integer", 2000),
        (pyarrow.int64(), 3000000000, pyarrow.int32(), 2000, "long", 2000),
        # Interestingly, delta lake will silently down-convert for us.
        # This is not an expected scenario, but we should beware this gotcha.
        (pyarrow.int32(), 2000, pyarrow.int64(), 3000000000, "integer", -1294967296),
    )
    @ddt.unpack
    def test_column_type_merges(self, type1, val1, type2, val2, expected_type, expected_value):
        """Verify that if we write a slightly different, but compatible field to the delta lake, it works"""
        schema1 = pyarrow.schema(
            [
                pyarrow.field("id", pyarrow.string()),
                pyarrow.field("int", type1),
            ]
        )
        self.store([{"id": "1", "int": val1}], schema=schema1)

        schema2 = pyarrow.schema(
            [
                pyarrow.field("id", pyarrow.string()),
                pyarrow.field("int", type2),
            ]
        )
        self.store([{"id": "1", "int": val2}], schema=schema2)

        table_path = os.path.join(self.output_dir, "patient")
        table_df = DeltaLakeFormat.spark.read.format("delta").load(table_path)
        schema_tree = self.get_spark_schema(table_df)
        self.assertEqual(
            f"""root
 |-- id: string (nullable = true)
 |-- int: {expected_type} (nullable = true)""",
            schema_tree,
        )

        values = utils.read_delta_lake(table_path)
        self.assertEqual(expected_value, values[0]["int"])

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
