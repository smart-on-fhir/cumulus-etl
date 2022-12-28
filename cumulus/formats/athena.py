"""An implementation of Format designed to write to AWS Athena"""

import abc
import logging
import os

import pandas

from cumulus import store


class AthenaFormat(store.Format):
    """
    Stores output files in a tree of files designed for easy consumption by AWS Athena

    Still useful in other contexts too (i.e. it's totally reasonable to write to disk instead of S3).
    But the particulars of why we organize files are designed for Athena.
    (i.e. one folder per data type, broken into large files)
    """

    @abc.abstractmethod
    def write_records(self, job, df: pandas.DataFrame, dbname: str, batch: int) -> None:
        """Writes the whole dataframe to the output database folder"""

    def store_patients(self, job, patients: pandas.DataFrame, batch: int) -> None:
        self.write_records(job, patients, 'patient', batch)

    def store_encounters(self, job, encounters: pandas.DataFrame, batch: int) -> None:
        self.write_records(job, encounters, 'encounter', batch)

    def store_labs(self, job, labs: pandas.DataFrame, batch: int) -> None:
        self.write_records(job, labs, 'observation', batch)

    def store_conditions(self, job, conditions: pandas.DataFrame, batch: int) -> None:
        self.write_records(job, conditions, 'condition', batch)

    def store_docrefs(self, job, docrefs: pandas.DataFrame, batch: int) -> None:
        self.write_records(job, docrefs, 'documentreference', batch)

    def store_covid_symptom__nlp_results(self, job, observations: pandas.DataFrame, batch: int) -> None:
        self.write_records(job, observations, 'covid_symptom__nlp_results', batch)


class AthenaBatchedFileFormat(AthenaFormat):
    """
    Stores output files as batched individual files.

    i.e. a few ndjson files that hold all the rows
    """

    @property
    @abc.abstractmethod
    def suffix(self) -> str:
        """
        The suffix to use for any files written out

        Honestly, not super necessary, since S3 filetypes are independent of suffix. But useful if writing locally.
        """

    @abc.abstractmethod
    def write_format(self, df: pandas.DataFrame, path: str) -> None:
        """
        Write the data in `df` to the target path file
        """

    ##########################################################################################
    #
    # Implementation details below
    #
    ##########################################################################################

    def write_records(self, job, df: pandas.DataFrame, dbname: str, batch: int) -> None:
        """Writes the whole dataframe to a single file"""
        job.attempt += len(df)

        if batch == 0:
            # First batch, let's clear out any existing files before writing any new ones.
            # FIXME: There is a real issue here where Athena will see invalid results until we've written all
            #        our files out. What we really want is some sort of blue/green deploy of data. There's no
            #        satisfying fix while we are writing to the same folder. (Unless we do incremental/delta
            #        writes and keep all old data around still.)
            parent_dir = self.root.joinpath(dbname)
            try:
                self.root.rm(parent_dir, recursive=True)
            except FileNotFoundError:
                pass

        try:
            full_path = self.root.joinpath(f'{dbname}/{dbname}.{batch:03}.{self.suffix}')
            self.root.makedirs(os.path.dirname(full_path))
            self.write_format(df, full_path)

            job.success += len(df)
            job.success_rate(1)
        except Exception:  # pylint: disable=broad-except
            logging.exception('Could not process data records')
