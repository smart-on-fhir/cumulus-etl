"""
An implementation of Format that writes to a Delta Lake.

See https://delta.io/
"""

import contextlib
import logging
import os
import tempfile

import delta
import pandas
import pyspark
from pyspark.sql.utils import AnalysisException

from cumulus import store
from cumulus.formats.base import Format

# This class would be a lot simpler if we could use fsspec & pandas directly, since that's what the rest of our code
# uses and expects (in terms of filesystem writing).
#
# There is a 1st party Delta Lake implementation (`deltalake`) based off native Rust code and which talks to
# fsspec & pandas by default. But it is missing some critical features as of this writing (mostly merges):
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
                .config("spark.driver.memory", "2g")
                .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
                .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            )

            # Now add delta's packages and actually build the session
            cls.spark = delta.configure_spark_with_delta_pip(
                builder,
                extra_packages=[
                    "org.apache.hadoop:hadoop-aws:3.3.4",
                ],
            ).getOrCreate()

        cls.spark.sparkContext.setLogLevel("ERROR")
        cls._configure_fs(root, cls.spark)

    def _write_one_batch(self, dataframe: pandas.DataFrame, batch: int) -> None:
        """Writes the whole dataframe to a delta lake"""
        # First, convert our pandas dataframe to a spark dataframe.
        # You'd think that self.spark.createDataFrame(df) would be the right thing to do, but actually, it can't
        # seem to correctly infer the nested schema (it doesn't give nested fields names, just the types).
        # But parquet does this well, and spark can read parquet well. So we do this dance of pandas -> parquet ->
        # sparks.
        with tempfile.NamedTemporaryFile() as parquet_file:
            dataframe.to_parquet(parquet_file.name, index=False)
            del dataframe  # allow GC to clean this up
            updates = self.spark.read.parquet(parquet_file.name)
            table = self.update_delta_table(updates)

        table.generate("symlink_format_manifest")

    def update_delta_table(self, updates: pyspark.sql.DataFrame) -> delta.DeltaTable:
        full_path = self._table_path(self.dbname)

        try:
            # Load table -- this will trigger an AnalysisException if the table doesn't exist yet
            table = delta.DeltaTable.forPath(self.spark, full_path)

            if self.group_field:
                # Delete any existing groups about to be overwritten. This leaves a small gap where if we
                # crash before inserting new rows, we will have deleted data without a replacement.
                # But a re-run of the ETL will correct that mistake. So it's not the worst gap to have.
                #
                # Ideally we'd be able to do both in the same MERGE statement for maximum atomicity.
                # But delta lake won't let us, as it tries to detect possibly undefined behavior,
                # and a line like the following will trip on it, worried about updating multiple rows at once:
                #  whenMatchedUpdateAll("table.id = updates.id").whenMatchedDelete().whenNotMatchedInsertAll()
                #
                # TODO: Once delta-spark 2.3 releases, we can do everything in one MERGE:
                #  - step 1: gather all group_field values in the source dataframe
                #  - step 2: add whenNotMatchedBySourceDelete(f"{group_field} in {all_source_values}")
                table.alias("table").merge(
                    updates.alias("updates"), f"table.{self.group_field} = updates.{self.group_field}"
                ).whenMatchedDelete().execute()

            # Merge in new data
            table.alias("table").merge(
                source=updates.alias("updates"), condition="table.id = updates.id"
            ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

        except AnalysisException:
            # table does not exist yet, let's make an initial version
            updates.write.save(path=full_path, format="delta")
            table = delta.DeltaTable.forPath(self.spark, full_path)

        return table

    def finalize(self) -> None:
        """Performs any necessary cleanup after all batches have been written"""
        full_path = self._table_path(self.dbname)

        try:
            table = delta.DeltaTable.forPath(self.spark, full_path)
        except AnalysisException:
            return  # if the table doesn't exist because we didn't write anything, that's fine - just bail

        try:
            table.optimize().executeCompaction()  # pool small files for better query performance
            table.generate("symlink_format_manifest")
            table.vacuum()  # Clean up unused data files older than retention policy (default 7 days)
        except AnalysisException:
            logging.exception("Could not finalize Delta Lake table %s", self.dbname)

    def _table_path(self, dbname: str) -> str:
        return self.root.joinpath(dbname).replace("s3://", "s3a://")  # hadoop uses the s3a: scheme instead of s3:

    @staticmethod
    def _configure_fs(root: store.Root, spark: pyspark.sql.SparkSession):
        """Tell spark/hadoop how to talk to S3 for us"""
        fsspec_options = root.fsspec_options()
        spark.conf.set("fs.s3a.sse.enabled", "true")
        spark.conf.set("fs.s3a.server-side-encryption-algorithm", "SSE-KMS")
        kms_key = fsspec_options.get("s3_additional_kwargs", {}).get("SSEKMSKeyId")
        if kms_key:
            spark.conf.set("fs.s3a.server-side-encryption.key", kms_key)
        region_name = fsspec_options.get("client_kwargs", {}).get("region_name")
        if region_name:
            spark.conf.set("fs.s3a.endpoint.region", region_name)
