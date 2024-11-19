"""
An implementation of Format that writes to a Delta Lake.

See https://delta.io/
"""

import contextlib
import logging
import os
import tempfile

import delta
import pyarrow
import pyarrow.parquet
import pyspark
from pyspark.sql.utils import AnalysisException

from cumulus_etl import store
from cumulus_etl.formats.base import Format
from cumulus_etl.formats.batch import Batch

# This class would be a lot simpler if we could use fsspec & pyarrow directly, since that's what the rest of our code
# uses and expects (in terms of filesystem writing).
#
# There is a 1st party Delta Lake implementation (`deltalake`) based off native Rust code and which talks to
# fsspec & pyarrow by default. But it is missing some critical features as of this writing (mostly merges):
# - Merge support in deltalake bindings: https://github.com/delta-io/delta-rs/issues/850


@contextlib.contextmanager
def _suppress_output():
    """
    Totally hides stdout and stderr unless there is an error, and then stderr is printed.

    This is a more powerful version of contextlib.redirect_stdout that also works for subprocesses / threads.
    """
    stdout = os.dup(1)
    stderr = os.dup(2)
    silent = os.open(os.devnull, os.O_WRONLY)
    os.dup2(silent, 1)
    os.dup2(silent, 2)

    try:
        yield
    finally:
        os.dup2(stdout, 1)
        os.dup2(stderr, 2)


class DeltaLakeFormat(Format):
    """
    Stores data in a delta lake.
    """

    spark = None

    @classmethod
    def initialize_class(cls, root: store.Root) -> None:
        if cls.spark is not None:
            return

        # This _suppress_output call is because pyspark is SO NOISY during session creation. Like 40 lines of trivial
        # output. Progress reports of downloading the jars. Comments about default logging level and the hostname.
        # I could not find a way to set the log level before the session is created. So here we just suppress
        # stdout/stderr entirely.
        with _suppress_output():
            # Prep the builder with various config options
            builder = (
                pyspark.sql.SparkSession.builder.appName("cumulus-etl")
                .config("spark.databricks.delta.schema.autoMerge.enabled", "true")
                .config("spark.driver.memory", "4g")
                .config(
                    "spark.sql.catalog.spark_catalog",
                    "org.apache.spark.sql.delta.catalog.DeltaCatalog",
                )
                .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            )

            # Now add delta's packages and actually build the session
            cls.spark = delta.configure_spark_with_delta_pip(
                builder,
                extra_packages=[
                    # See https://docs.delta.io/latest/delta-storage.html for advice
                    # on which version of hadoop-aws to use.
                    "org.apache.hadoop:hadoop-aws:3.3.4",
                ],
            ).getOrCreate()
        cls.spark.sparkContext.setLogLevel("ERROR")
        cls._configure_fs(root, cls.spark)

    def _write_one_batch(self, batch: Batch) -> None:
        """Writes the whole dataframe to a delta lake"""
        with self.batch_to_spark(batch) as updates:
            delta_table = self.update_delta_table(updates, groups=batch.groups)

        delta_table.generate("symlink_format_manifest")

    def update_delta_table(
        self, updates: pyspark.sql.DataFrame, groups: set[str]
    ) -> delta.DeltaTable:
        table = (
            delta.DeltaTable.createIfNotExists(self.spark)
            .addColumns(updates.schema)
            .clusterBy(*self.uniqueness_fields)
            .location(self._table_path(self.dbname))
            .execute()
        )

        # Determine merge condition
        conditions = [f"table.{field} = updates.{field}" for field in self.uniqueness_fields]
        condition = " AND ".join(conditions)

        # Merge in new data
        merge = (
            table.alias("table")
            .merge(source=updates.alias("updates"), condition=condition)
            .whenNotMatchedInsertAll()
        )
        if self.update_existing:
            update_condition = self._get_update_condition(updates.schema)
            merge = merge.whenMatchedUpdateAll(condition=update_condition)

        if self.group_field and groups:
            # Delete any entries for groups touched by this update that are no longer present in the group
            # (we are guaranteed to have all members of each group in the `updates` dataframe).
            condition_column = table.toDF()[self.group_field].isin(groups)
            merge = merge.whenNotMatchedBySourceDelete(condition_column)

        merge.execute()

        return table

    def delete_records(self, ids: set[str]) -> None:
        """Deletes the given IDs."""
        if not ids:
            return

        table = self._load_table()
        if not table:
            return

        try:
            id_list = "', '".join(ids)
            table.delete(f"id in ('{id_list}')")
        except Exception:
            logging.exception("Could not delete IDs from Delta Lake table %s", self.dbname)

    def finalize(self) -> None:
        """Performs any necessary cleanup after all batches have been written"""
        table = self._load_table()
        if not table:
            return

        try:
            table.optimize().executeCompaction()  # pool small files for better query performance
            table.generate("symlink_format_manifest")
            table.vacuum()  # Clean up unused data files older than retention policy (default 7 days)
        except Exception:
            logging.exception("Could not finalize Delta Lake table %s", self.dbname)

    def _table_path(self, dbname: str) -> str:
        # hadoop uses the s3a: scheme instead of s3:
        return self.root.joinpath(dbname).replace("s3://", "s3a://")

    def _load_table(self) -> delta.DeltaTable | None:
        full_path = self._table_path(self.dbname)

        try:
            return delta.DeltaTable.forPath(self.spark, full_path)
        except AnalysisException:
            # The table likely doesn't exist.
            # Which can be normal if we didn't write anything yet, that's fine - just bail.
            return None
        except Exception:
            logging.exception("Could not load Delta Lake table %s", self.dbname)
            return None

    @staticmethod
    def _get_update_condition(schema: pyspark.sql.types.StructType) -> str | None:
        """
        Determine what (if any) whenMatchedUpdateAll condition to use for the given update schema.

        Usually, this means checking the meta.lastUpdated value and skipping updates if the new row is older than the
        existing data. But we only want to check that if the field exists (and it might not in some cases - like if
        this is a custom table ala covid_symptom__nlp_results).
        """
        # See if this update dataframe has a meta.lastUpdated field.
        # If not (which might be true for custom tables like covid_symptom__nlp_results or unit tests),
        # unconditionally update matching rows by returning None for the condition.
        meta_type = "meta" in schema.fieldNames() and schema["meta"].dataType
        has_last_updated_field = (
            meta_type
            and isinstance(meta_type, pyspark.sql.types.StructType)
            and "lastUpdated" in meta_type.fieldNames()
        )
        if not has_last_updated_field:
            return None

        # OK, the field exists (which is typical for any FHIR resource tables, as we provide a wide
        # FHIR schema), so we want to conditionally update rows based on the timestamp.
        #
        # We skip the update row if both the table and the update have a lastUpdated value and the
        # update's value is in the past. But err on the side of caution if anything is missing,
        # by taking the update.
        #
        # This uses less-than-or-equal instead of less-than when comparing the date, because
        # sometimes the ETL will upload different content for the same resource as we update the
        # ETL (for example, we allow-list yet another extension - we still want to re-upload the
        # content with the new extension but same lastUpdated value). This does cause some needless
        # churn on the delta lake side, but we'll have to live with that.
        #
        # If we eventually decide that sub-second updates are a real concern, we can additionally
        # compare versionId. But I don't know how you extracted both versions so quickly. :)
        #
        # The cast-as-timestamp does not seem to noticeably slow us down.
        # If it becomes an issue, we could always actually convert this string column to a real
        # date/time column.
        return (
            "table.meta.lastUpdated is null or "
            "updates.meta.lastUpdated is null or "
            "CAST(table.meta.lastUpdated AS TIMESTAMP) <= "
            "CAST(updates.meta.lastUpdated AS TIMESTAMP)"
        )

    @staticmethod
    def _configure_fs(root: store.Root, spark: pyspark.sql.SparkSession):
        """Tell spark/hadoop how to talk to S3 for us"""
        fsspec_options = root.fsspec_options()
        # This credentials.provider option enables usage of the AWS credentials default priority list (i.e. it will
        # cause a check for a ~/.aws/credentials file to happen instead of just looking for env vars).
        # See http://wrschneider.github.io/2019/02/02/spark-credentials-file.html for details
        spark.conf.set(
            "fs.s3a.aws.credentials.provider",
            "com.amazonaws.auth.DefaultAWSCredentialsProviderChain",
        )
        spark.conf.set("fs.s3a.sse.enabled", "true")
        spark.conf.set("fs.s3a.server-side-encryption-algorithm", "SSE-KMS")
        kms_key = fsspec_options.get("s3_additional_kwargs", {}).get("SSEKMSKeyId")
        if kms_key:
            spark.conf.set("fs.s3a.server-side-encryption.key", kms_key)
        region_name = fsspec_options.get("client_kwargs", {}).get("region_name")
        if region_name:
            spark.conf.set("fs.s3a.endpoint.region", region_name)

    @contextlib.contextmanager
    def batch_to_spark(self, batch: Batch) -> pyspark.sql.DataFrame:
        """Transforms a batch to a spark DF"""
        # This is the quick and dirty way - write batch to parquet with pyarrow and read it back.
        # But a more direct way would be to convert the pyarrow schema to a pyspark schema and just
        # call self.spark.createDataFrame(batch.rows, schema=pyspark_schema). A future improvement.
        with tempfile.NamedTemporaryFile() as data_path:
            table = pyarrow.Table.from_pylist(batch.rows, schema=batch.schema)
            pyarrow.parquet.write_table(table, data_path.name)
            del table

            yield self.spark.read.parquet(data_path.name)
