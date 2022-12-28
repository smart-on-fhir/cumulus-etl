"""
An implementation of Format that writes to a Delta Lake.

See https://delta.io/
"""

import contextlib
import logging
import os

import delta
import pandas
import pyspark
from pyspark.sql.utils import AnalysisException

from cumulus import store

from .athena import AthenaFormat

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


class DeltaLakeFormat(AthenaFormat):
    """
    Stores data in a delta lake.
    """
    def __init__(self, root: store.Root):
        super().__init__(root)

        # This _suppress_output call is because pyspark is SO NOISY during session creation. Like 40 lines of trivial
        # output. Progress reports of downloading the jars. Comments about default logging level and the hostname.
        # I could not find a way to set the log level before the session is created. So here we just suppress
        # stdout/stderr entirely.
        with _suppress_output():
            # Prep the builder with various config options
            builder = pyspark.sql.SparkSession.builder \
                .appName('cumulus-etl') \
                .config('spark.driver.memory', '2g') \
                .config('spark.sql.catalog.spark_catalog', 'org.apache.spark.sql.delta.catalog.DeltaCatalog') \
                .config('spark.sql.extensions', 'io.delta.sql.DeltaSparkSessionExtension')

            # Now add delta's packages and actually build the session
            self.spark = delta.configure_spark_with_delta_pip(builder, extra_packages=[
                'org.apache.hadoop:hadoop-aws:3.3.4',
            ]).getOrCreate()

        self.spark.sparkContext.setLogLevel('ERROR')
        self._configure_fs()

    def write_records(self, job, df: pandas.DataFrame, dbname: str, batch: int) -> None:
        """Writes the whole dataframe to a delta lake"""
        job.attempt += len(df)
        full_path = self.root.joinpath(dbname).replace('s3://', 's3a://')  # hadoop uses the s3a: scheme instead of s3:

        try:
            updates = self.spark.createDataFrame(df)

            try:
                table = delta.DeltaTable.forPath(self.spark, full_path)
                if batch == 0:
                    table.vacuum()  # Clean up unused data files older than retention policy (default 7 days)
                table.alias('table') \
                    .merge(source=updates.alias('updates'), condition='table.id = updates.id') \
                    .whenMatchedUpdateAll() \
                    .whenNotMatchedInsertAll() \
                    .execute()
            except AnalysisException:
                # table does not exist yet, let's make an initial version
                updates.write.save(path=full_path, format='delta')

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
