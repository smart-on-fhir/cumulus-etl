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

from cumulus_etl import fhir, store
from cumulus_etl.formats.base import Format

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
        with self.pandas_to_spark_with_schema(dataframe) as updates:
            table = self.update_delta_table(updates)

        table.generate("symlink_format_manifest")

    def update_delta_table(self, updates: pyspark.sql.DataFrame) -> delta.DeltaTable:
        full_path = self._table_path(self.dbname)

        try:
            # Load table -- this will trigger an AnalysisException if the table doesn't exist yet
            table = delta.DeltaTable.forPath(self.spark, full_path)

            # Merge in new data
            merge = (
                table.alias("table")
                .merge(source=updates.alias("updates"), condition="table.id = updates.id")
                .whenMatchedUpdateAll()
                .whenNotMatchedInsertAll()
            )

            if self.group_field:
                # Delete any entries for groups touched by this update that are no longer present in the group
                # (we are guaranteed to have all members of each group in the `updates` dataframe).
                distinct_values = [row[0] for row in updates.select(self.group_field).distinct().collect()]
                condition_column = table.toDF()[self.group_field].isin(distinct_values)
                merge = merge.whenNotMatchedBySourceDelete(condition_column)

            merge.execute()

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
        except Exception:  # pylint: disable=broad-except
            logging.exception("Could not finalize Delta Lake table %s", self.dbname)
            return

        try:
            table.optimize().executeCompaction()  # pool small files for better query performance
            table.generate("symlink_format_manifest")
            table.vacuum()  # Clean up unused data files older than retention policy (default 7 days)
        except Exception:  # pylint: disable=broad-except
            logging.exception("Could not finalize Delta Lake table %s", self.dbname)

    def _table_path(self, dbname: str) -> str:
        return self.root.joinpath(dbname).replace("s3://", "s3a://")  # hadoop uses the s3a: scheme instead of s3:

    @staticmethod
    def _configure_fs(root: store.Root, spark: pyspark.sql.SparkSession):
        """Tell spark/hadoop how to talk to S3 for us"""
        fsspec_options = root.fsspec_options()
        # This credentials.provider option enables usage of the AWS credentials default priority list (i.e. it will
        # cause a check for a ~/.aws/credentials file to happen instead of just looking for env vars).
        # See http://wrschneider.github.io/2019/02/02/spark-credentials-file.html for details
        spark.conf.set("fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain")
        spark.conf.set("fs.s3a.sse.enabled", "true")
        spark.conf.set("fs.s3a.server-side-encryption-algorithm", "SSE-KMS")
        kms_key = fsspec_options.get("s3_additional_kwargs", {}).get("SSEKMSKeyId")
        if kms_key:
            spark.conf.set("fs.s3a.server-side-encryption.key", kms_key)
        region_name = fsspec_options.get("client_kwargs", {}).get("region_name")
        if region_name:
            spark.conf.set("fs.s3a.endpoint.region", region_name)

    @contextlib.contextmanager
    def pandas_to_spark_with_schema(self, dataframe: pandas.DataFrame) -> pyspark.sql.DataFrame:
        """Transforms a pandas DF to a spark DF with a full FHIR schema included"""
        # This method solves two problems:
        # 1. Pandas schemas are very loosey-goosey (and don't really take nested data into account), so simply
        #    calling self.spark.createDataFrame(df) does not give names for nested struct fields.
        # 2. We want to provide column info for all valid FHIR fields (at least shallowly) so that the
        #    downstream SQL can reference all toplevel columns even if the source data doesn't have those fields.
        #
        # Issue #1 is solved by writing to parquet and reading it back in (a little wonky, but it gives full schemas).
        # Issue #2 is solved by merging a computed FHIR schema with the actual data schema.
        #
        # Some devils-in-the-details:
        # - Pyspark does not let us merge schemas in python code, we can only seem to do it while reading in multiple
        #   dataframes. So we write out the schema as an empty parquet file and read it back in with the data.
        #   We could write some manual schema-merging code, but I'm leery that we'd get it right or that it's worth
        #   doing ourselves rather than just writing this weird file and letting pyspark do it for us.
        # - Our FHIR schema is incomplete and shallow (it skips all nested structs) to avoid infinite recursion issues,
        #   and we simply merge this incomplete schema in with the actual data schema, which will have full nested
        #   inferred schemas for exactly the fields it uses. We always write to the delta lake with autoMerge of
        #   schemas enabled, so incrementally adding fields to existing lakes will be fine.
        # - Delta Lake does not like columns that have null types nor struct types with no children. So we make sure
        #   that every column has *some* definition, and that structs have content.

        with tempfile.TemporaryDirectory() as parquet_dir:
            data_path = os.path.join(parquet_dir, "data.parquet")
            schema_path = os.path.join(parquet_dir, "schema.parquet")

            # Write the pandas dataframe to parquet to force full nested schemas.
            # We also convert dtypes, to get modern nullable pandas types (rather than using its default behavior of
            # converting a nullable integer column into a float column).
            dataframe.convert_dtypes().to_parquet(data_path, index=False)
            del dataframe  # allow GC to clean this up
            paths = [data_path]

            # Write the empty schema dataframe, so we can merge it with the above real dataframe
            if self.resource_type:
                schema = fhir.create_spark_schema_for_resource(self.resource_type)
                self.spark.createDataFrame([], schema=schema).write.parquet(schema_path)
                paths.append(schema_path)

            # Provide the happy merged result, coalesced to one partition (because our schema trick above creates 2)
            yield self.spark.read.parquet(*paths, mergeSchema=True).coalesce(1)
