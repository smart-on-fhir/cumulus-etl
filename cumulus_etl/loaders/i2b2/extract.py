"""Read files into data structures"""

import logging
from collections.abc import Iterator

from cumulus_etl import common
from cumulus_etl.loaders.i2b2.schema import ObservationFact, PatientDimension, VisitDimension


def extract_csv(path_csv: str) -> Iterator[dict]:
    """
    :param path_csv: /path/to/i2b2_formatted_file.csv
    :return: an iterator over each row from the file
    """
    try:
        with common.read_csv(path_csv) as reader:
            print(f"Reading csv {path_csv}...")
            yield from reader
            print(f"Done reading {path_csv}.")
    except FileNotFoundError:
        print(f"No {path_csv}, skipping.")


def extract_csv_observation_facts(path_csv: str) -> Iterator[ObservationFact]:
    """
    :param path_csv: /path/to/file.csv
    :return: i2b2 ObservationFact table
    """
    logging.info("Transforming text into List[ObservationFact]")
    for row in extract_csv(path_csv):
        yield ObservationFact(row)


def extract_csv_patients(path_csv: str) -> Iterator[PatientDimension]:
    """
    :param path_csv: /path/to/file.csv
    :return: List i2b2 patient dimension table
    """
    logging.info("Transforming text into List[PatientDimension]")
    for row in extract_csv(path_csv):
        yield PatientDimension(row)


def extract_csv_visits(path_csv: str) -> Iterator[VisitDimension]:
    """
    :param path_csv: /path/to/file.csv
    :return: List i2b2 visit dimension table
    """
    logging.info("Transforming text into List[VisitDimension]")
    for row in extract_csv(path_csv):
        yield VisitDimension(row)
