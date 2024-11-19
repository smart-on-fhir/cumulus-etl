"""Tests for Delta Lake support"""

import contextlib
import io
import os
import shutil
import tempfile
from unittest import mock

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
        output_tempdir = tempfile.TemporaryDirectory()
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
        self,
        rows: list[dict],
        schema: pyarrow.Schema = None,
        groups: set[str] | None = None,
        **kwargs,
    ) -> bool:
        """
        Writes a single batch of data to the data lake.

        :param rows: the data to insert
        :param batch_index: which batch number this is, defaulting to 10 to avoid triggering any first/last batch logic
        :param schema: the batch schema, in pyarrow format
        :param groups: all group values for this batch (ignored if group_field is not set)
        """
        deltalake = DeltaLakeFormat(self.root, "patient", **kwargs)
        batch = formats.Batch(rows, groups=groups, schema=schema)
        return deltalake.write_records(batch)

    def assert_lake_equal(self, rows: list[dict]) -> None:
        table_path = os.path.join(self.output_dir, "patient")
        rows_by_id = sorted(rows, key=lambda x: x.get("id", sorted(x.items())))
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
                {
                    "id": "future-with-offset",
                    "meta": {"lastUpdated": future_with_offset},
                    "value": 1,
                },
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
                {"id": "now", "meta": {"lastUpdated": now}, "value": 2},
                {"id": "now-without-zed", "meta": {"lastUpdated": now}, "value": 2},
                {"id": "future", "meta": {"lastUpdated": future}, "value": 1},
                {
                    "id": "future-with-offset",
                    "meta": {"lastUpdated": future_with_offset},
                    "value": 1,
                },
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

        # Confirm that Delta Lake will error out when presented with an altered type, with or without a schema.
        self.assertFalse(self.store(self.df(b="string"), schema=schema))
        self.assertFalse(self.store(self.df(b="string")))

        self.assert_lake_equal(self.df(a=1))

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
        (pyarrow.int64(), 2000, pyarrow.int32(), 2001, True, "long", 2001),
        (pyarrow.int32(), 2000, pyarrow.int64(), 2001, True, "integer", 2001),
        (pyarrow.int64(), 3000000000, pyarrow.int32(), 2000, True, "long", 2000),
        # Delta lake will refuse to store too large a value for the type
        (pyarrow.int32(), 2000, pyarrow.int64(), 3000000000, False, "integer", 2000),
    )
    @ddt.unpack
    def test_column_type_merges(
        self, type1, val1, type2, val2, expected_success, expected_type, expected_value
    ):
        """Verify that if we write a slightly different, but compatible field to the delta lake, it works"""
        schema1 = pyarrow.schema(
            [
                pyarrow.field("id", pyarrow.string()),
                pyarrow.field("int", type1),
            ]
        )
        self.assertTrue(self.store([{"id": "1", "int": val1}], schema=schema1))

        schema2 = pyarrow.schema(
            [
                pyarrow.field("id", pyarrow.string()),
                pyarrow.field("int", type2),
            ]
        )
        self.assertEqual(expected_success, self.store([{"id": "1", "int": val2}], schema=schema2))

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
            self.df(
                aa={"group": "A", "val": 5},  # will be deleted as stale group member
                ab={"group": "A", "val": 10},  # will be updated
                # will be ignored because 2nd batch won't have group B in it
                b={
                    "group": "B",
                    "val": 1,
                },
                # will be deleted as group with zero members in new batch
                c={
                    "group": "C",
                    "val": 2,
                },
            ),
            group_field="value.group",
            groups={"A", "B", "C"},
        )
        # Sanity check that group settings don't change anything with a fresh delta lake
        self.assert_lake_equal(
            self.df(
                aa={"group": "A", "val": 5},
                ab={"group": "A", "val": 10},
                b={"group": "B", "val": 1},
                c={"group": "C", "val": 2},
            )
        )

        # Now update the delta lake with a new batch
        self.store(
            self.df(
                ab={"group": "A", "val": 11},  # same id, new value
                ac={"group": "A", "val": 16},  # new group member
                # Add a quote as part of the D group identifier, just to confirm we escape these strings
                d={"group": 'D"', "val": 3},  # whole new group
            ),
            group_field="value.group",
            groups={
                "A",
                "C",  # C is present but with no rows (existing rows will be deleted)
                'D"',
            },
        )
        self.assert_lake_equal(
            self.df(
                ab={"group": "A", "val": 11},
                ac={"group": "A", "val": 16},
                b={"group": "B", "val": 1},
                d={"group": 'D"', "val": 3},
            )
        )

    def test_custom_uniqueness(self):
        """Verify that `uniqueness_fields` is properly handled."""
        ids = {"F1", "F2"}
        self.store(
            [
                {"F1": 1, "F2": 2, "msg": "original value"},
                {"F1": 1, "F2": 9, "msg": "same F1"},
                {"F1": 9, "F2": 2, "msg": "same F2"},
            ],
            uniqueness_fields=ids,
        )
        self.store([{"F1": 1, "F2": 2, "msg": "new"}], uniqueness_fields=ids)
        self.assert_lake_equal(
            [
                {"F1": 1, "F2": 2, "msg": "new"},
                {"F1": 1, "F2": 9, "msg": "same F1"},
                {"F1": 9, "F2": 2, "msg": "same F2"},
            ]
        )

    def test_update_existing(self):
        """Verify that `update_existing` is properly handled."""
        self.store(self.df(a=1, b=2))
        self.store(self.df(a=999, c=3), update_existing=False)
        self.assert_lake_equal(self.df(a=1, b=2, c=3))

    def test_s3_options(self):
        """Verify that we read in S3 options and set spark config appropriately"""
        # Save global/class-wide spark object, to be restored. Then clear it out.
        old_spark = DeltaLakeFormat.spark

        def restore_spark():
            DeltaLakeFormat.spark = old_spark

        self.addCleanup(restore_spark)
        DeltaLakeFormat.spark = None

        # Now re-initialize the class, mocking out all the slow spark stuff, and using S3.
        fs_options = {
            "s3_kms_key": "test-key",
            "s3_region": "us-west-1",
        }
        with (
            mock.patch("cumulus_etl.store._user_fs_options", new=fs_options),
            mock.patch("delta.configure_spark_with_delta_pip"),
            mock.patch("pyspark.sql"),
        ):
            DeltaLakeFormat.initialize_class(store.Root("s3://test/"))

        self.assertEqual(
            sorted(DeltaLakeFormat.spark.conf.set.call_args_list, key=lambda x: x[0][0]),
            [
                mock.call(
                    "fs.s3a.aws.credentials.provider",
                    "com.amazonaws.auth.DefaultAWSCredentialsProviderChain",
                ),
                mock.call("fs.s3a.endpoint.region", "us-west-1"),
                mock.call("fs.s3a.server-side-encryption-algorithm", "SSE-KMS"),
                mock.call("fs.s3a.server-side-encryption.key", "test-key"),
                mock.call("fs.s3a.sse.enabled", "true"),
            ],
        )

    def test_finalize_happy_path(self):
        """Verify that we clean up the delta lake when finalizing."""
        # Limit our fake table to just these attributes, to notice any new usage in future
        mock_table = mock.MagicMock(spec=["generate", "optimize", "vacuum"])
        self.patch("delta.DeltaTable.forPath", return_value=mock_table)

        DeltaLakeFormat(self.root, "patient").finalize()
        self.assertEqual(mock_table.optimize.call_args_list, [mock.call()])
        self.assertEqual(
            mock_table.optimize.return_value.executeCompaction.call_args_list, [mock.call()]
        )
        self.assertEqual(mock_table.generate.call_args_list, [mock.call("symlink_format_manifest")])
        self.assertEqual(mock_table.vacuum.call_args_list, [mock.call()])

    def test_finalize_cannot_load_table(self):
        """Verify that we gracefully handle failing to read an existing table when finalizing."""
        # No table
        deltalake = DeltaLakeFormat(self.root, "patient")
        with self.assertNoLogs():
            deltalake.finalize()
        self.assertFalse(os.path.exists(self.output_dir))

        # Error loading the table
        with self.assertLogs(level="ERROR") as logs:
            with mock.patch("delta.DeltaTable.forPath", side_effect=ValueError):
                deltalake.finalize()
        self.assertEqual(len(logs.output), 1)
        self.assertTrue(
            logs.output[0].startswith("ERROR:root:Could not load Delta Lake table patient\n")
        )

    def test_finalize_error(self):
        """Verify that we gracefully handle an error while finalizing."""
        self.store(self.df(a=1))  # create a simple table to load
        with self.assertLogs(level="ERROR") as logs:
            with mock.patch("delta.DeltaTable.optimize", side_effect=ValueError):
                DeltaLakeFormat(self.root, "patient").finalize()
        self.assertEqual(len(logs.output), 1)
        self.assertTrue(
            logs.output[0].startswith("ERROR:root:Could not finalize Delta Lake table patient\n")
        )

    def test_delete_records_happy_path(self):
        """Verify that `delete_records` works in a basic way."""
        self.store(self.df(a=1, b=2, c=3, d=4))

        deltalake = DeltaLakeFormat(self.root, "patient")
        deltalake.delete_records({"a", "c"})
        deltalake.delete_records({"d"})
        deltalake.delete_records(set())

        self.assert_lake_equal(self.df(b=2))

    def test_delete_records_cannot_load_table(self):
        """Verify we gracefully handle a missing table"""
        deltalake = DeltaLakeFormat(self.root, "patient")
        with self.assertNoLogs():
            deltalake.delete_records({"a"})
        self.assertFalse(os.path.exists(self.output_dir))

    def test_delete_records_error(self):
        """Verify that `delete_records` handles errors gracefully."""
        mock_table = mock.MagicMock(spec=["delete"])
        mock_table.delete.side_effect = ValueError
        self.patch("delta.DeltaTable.forPath", return_value=mock_table)

        with self.assertLogs(level="ERROR") as logs:
            DeltaLakeFormat(self.root, "patient").delete_records("a")

        self.assertEqual(len(logs.output), 1)
        self.assertTrue(
            logs.output[0].startswith(
                "ERROR:root:Could not delete IDs from Delta Lake table patient\n"
            )
        )
