"""Read files into data structures"""

import logging
from collections.abc import Iterator

import pandas

from cumulus_etl.loaders.i2b2.schema import ObservationFact, PatientDimension, VisitDimension


def extract_csv(path_csv: str, batch_size: int) -> Iterator[dict]:
    """
    :param path_csv: /path/to/i2b2_formatted_file.csv
    :param batch_size: how many entries to load into memory at once
    :return: an iterator over each row from the file
    """
    count = 0
    try:
        with pandas.read_csv(path_csv, dtype=str, na_filter=False, chunksize=batch_size) as reader:
            print(f"Reading csv {path_csv}...")
            for chunk in reader:
                for _, row in chunk.iterrows():
                    yield dict(row)
                count += len(chunk)
                print(f"  Read {count:,} entries...")
            print(f"Done reading {path_csv}.")
    except FileNotFoundError:
        print(f"No {path_csv}, skipping.")


def extract_csv_observation_facts(path_csv: str, batch_size: int) -> Iterator[ObservationFact]:
    """
    :param path_csv: /path/to/file.csv
    :param batch_size: how many entries to load into memory at once
    :return: i2b2 ObservationFact table
    """
    logging.info("Transforming text into List[ObservationFact]")
    for row in extract_csv(path_csv, batch_size):
        yield ObservationFact(row)


def extract_csv_patients(path_csv: str, batch_size: int) -> Iterator[PatientDimension]:
    """
    :param path_csv: /path/to/file.csv
    :param batch_size: how many entries to load into memory at once
    :return: List i2b2 patient dimension table
    """
    logging.info("Transforming text into List[PatientDimension]")
    for row in extract_csv(path_csv, batch_size):
        yield PatientDimension(row)


def extract_csv_visits(path_csv: str, batch_size: int) -> Iterator[VisitDimension]:
    """
    :param path_csv: /path/to/file.csv
    :param batch_size: how many entries to load into memory at once
    :return: List i2b2 visit dimension table
    """
    logging.info("Transforming text into List[VisitDimension]")
    for row in extract_csv(path_csv, batch_size):
        yield VisitDimension(row)
