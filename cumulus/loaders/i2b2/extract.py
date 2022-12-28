"""Read files into data structures"""

import logging
from typing import Iterator

import pandas

from cumulus.loaders.i2b2.schema import ObservationFact, PatientDimension, VisitDimension


def extract_csv(path_csv: str, batch_size: int) -> Iterator[dict]:
    """
    :param path_csv: /path/to/i2b2_formatted_file.csv
    :param batch_size: how many entries to load into memory at once
    :return: an iterator over each row from the file
    """
    print(f'Reading csv {path_csv}...')
    count = 0
    with pandas.read_csv(path_csv, dtype=str, na_filter=False, chunksize=batch_size) as reader:
        for chunk in reader:
            print(f'  Read {count:,} entries...')
            for _, row in chunk.iterrows():
                yield dict(row)
            count += batch_size
    print(f'Done reading {path_csv} .')


def extract_csv_observation_facts(path_csv: str, batch_size: int) -> Iterator[ObservationFact]:
    """
    :param path_csv: /path/to/file.csv
    :param batch_size: how many entries to load into memory at once
    :return: i2b2 ObservationFact table
    """
    logging.info('Transforming text into List[ObservationFact]')
    for row in extract_csv(path_csv, batch_size):
        yield ObservationFact(row)


def extract_csv_patients(path_csv: str, batch_size: int) -> Iterator[PatientDimension]:
    """
    :param path_csv: /path/to/file.csv
    :param batch_size: how many entries to load into memory at once
    :return: List i2b2 patient dimension table
    """
    logging.info('Transforming text into List[PatientDimension]')
    for row in extract_csv(path_csv, batch_size):
        yield PatientDimension(row)


def extract_csv_visits(path_csv: str, batch_size: int) -> Iterator[VisitDimension]:
    """
    :param path_csv: /path/to/file.csv
    :param batch_size: how many entries to load into memory at once
    :return: List i2b2 visit dimension table
    """
    logging.info('Transforming text into List[VisitDimension]')
    for row in extract_csv(path_csv, batch_size):
        yield VisitDimension(row)
