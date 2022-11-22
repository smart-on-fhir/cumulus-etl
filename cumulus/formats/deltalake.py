"""
An implementation of Format that writes to a Delta Lake.

See https://delta.io/
"""

import logging

import delta
import pandas
import pyspark
from pyspark.sql.utils import AnalysisException

from cumulus import store

from .athena import AthenaFormat


# This class would be a lot simpler if we could use fsspec & pandas directly, since that's what the rest of our code
# uses and expects (in terms of filesystem writing).
# Spark uses hadoop and both are written in Java, so we need to write some glue code and also convert S3 args.
# There is a different Delta Lake implementation (deltalake on pypi) based off native Rust code and which talks to
# Pandas & fsspec by default. But it is missing some critical features as of this writing (like merges):
# - Merge support in deltalake bindings: https://github.com/delta-io/delta-rs/issues/850

class IcebergFormat(AthenaFormat):
    """
    Stores data in an iceberg data lake.
    """
    def __init__(self, root: store.Root):
        super().__init__(root)
        # Package link: https://mvnrepository.com/artifact/org.apache.iceberg/iceberg-spark-extensions-3.3
        self.spark = pyspark.sql.SparkSession.builder \
            .appName('cumulus-etl') \
            .config('spark.driver.memory', '2g') \
            .config('spark.jars.packages', 'org.apache.iceberg:iceberg-spark-3.3_2.12:1.0.0,'
                                           'org.apache.iceberg:iceberg-spark-extensions-3.3_2.12:1.0.0') \
            .config('spark.sql.catalog.cumulus', 'org.apache.iceberg.spark.SparkCatalog') \
            .config('spark.sql.catalog.cumulus.type', 'hadoop') \
            .config('spark.sql.catalog.cumulus.warehouse', root.path) \
            .config('spark.sql.catalog.spark_catalog', 'org.apache.iceberg.spark.SparkSessionCatalog') \
            .config('spark.sql.defaultCatalog', 'cumulus') \
            .config('spark.sql.extensions', 'org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions') \
            .getOrCreate()
        #self._configure_fs()

    def write_records(self, job, df: pandas.DataFrame, dbname: str, batch: int) -> None:
        """Writes the whole dataframe to a delta lake"""
        job.attempt += len(df)
        full_path = self.root.joinpath(dbname).replace('s3://', 's3a://')  # hadoop uses the s3a: scheme instead of s3:

        try:
            updates = self.spark.createDataFrame(df)
            updates.createOrReplaceTempView('updates')

            try:
                #_table = self.spark.table(dbname).alias('table')
                self.spark.sql(
                    f'MERGE INTO {dbname} table '
                    'USING updates '
                    'ON table.id = updates.id '
                    'WHEN MATCHED THEN UPDATE SET * '
                    'WHEN NOT MATCHED THEN INSERT * '
                )
            except AnalysisException as exc:
                print(exc)
                # table does not exist yet, let's make an initial version
                updates.writeTo(dbname).create()

            job.success += len(df)
            job.success_rate(1)
        except Exception:  # pylint: disable=broad-except
            logging.exception('Could not process data records')


class DeltaLakeFormat(AthenaFormat):
    """
    Stores data in a delta lake.
    """
    def __init__(self, root: store.Root):
        super().__init__(root)
        builder = pyspark.sql.SparkSession.builder \
            .appName('cumulus-etl') \
            .config('spark.driver.memory', '2g') \
            .config('spark.sql.catalog.spark_catalog', 'org.apache.spark.sql.delta.catalog.DeltaCatalog') \
            .config('spark.sql.extensions', 'io.delta.sql.DeltaSparkSessionExtension')
        self.spark = delta.configure_spark_with_delta_pip(builder).getOrCreate()
        self._configure_fs()

    def write_records(self, job, df: pandas.DataFrame, dbname: str, batch: int) -> None:
        """Writes the whole dataframe to a delta lake"""
        job.attempt += len(df)
        full_path = self.root.joinpath(dbname).replace('s3://', 's3a://')  # hadoop uses the s3a scheme (same as s3)

        try:
            updates = self.spark.createDataFrame(df)

            try:
                table = delta.DeltaTable.forPath(self.spark, full_path)
                # if batch == 0:
                #     table.vacuum()
                # TODO: why does this keep deleting and recreating a single row...?
                table.alias('table') \
                    .merge(source=updates.alias('updates'), condition='table.id = updates.id') \
                    .whenMatchedUpdateAll() \
                    .whenNotMatchedInsertAll() \
                    .execute()
            except AnalysisException:
                # table does not exist yet, let's make an initial version
                updates.write.format('delta').save(full_path)
                table = delta.DeltaTable.forPath(self.spark, full_path)

            # Now generate a manifest file for Athena's benefit.
            # Otherwise, Athena would scan all parquet files, even ones holding removed data.
            # https://docs.delta.io/latest/delta-utility.html#generate-a-manifest-file
            table.generate('symlink_format_manifest')

            job.success += len(df)
            job.success_rate(1)
        except Exception:  # pylint: disable=broad-except
            logging.exception('Could not process data records')

    def _configure_fs(self):
        """Tell spark/hadoop how to talk to S3 for us"""
        fsspec_options = self.root.fsspec_options()
        self.spark.conf.set('fs.s3a.sse.enabled', 'true')
        self.spark.conf.set('fs.s3a.server-side-encryption-algorithm', 'SSE-KMS')
        kms_key = fsspec_options.get('s3_additional_kwargs', {}).get('SSEKMSKeyId')
        if kms_key:
            self.spark.conf.set('fs.s3a.sse.kms.keyId', kms_key)
        region_name = fsspec_options.get('client_kwargs', {}).get('region_name')
        if region_name:
            self.spark.conf.set('fs.s3a.endpoint.region', region_name)
