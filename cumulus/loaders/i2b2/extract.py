"""Read files into data structures"""

from typing import List
import logging
import pandas
from cumulus import common
from cumulus.loaders.i2b2.schema import ObservationFact, PatientDimension, VisitDimension


def extract_csv(path_csv: str, sample=1.0) -> pandas.DataFrame:
    """
    :param path_csv: /path/to/i2b2_formatted_file.csv
    :param sample: %percentage of file to read
    :return: pandas Dataframe
    """
    return common.extract_csv(path_csv, sample)


def extract_csv_observation_facts(path_csv: str,
                                  sample=1.0) -> List[ObservationFact]:
    """
    :param path_csv: /path/to/file.csv
    :param sample: %percentage of file to read
    :return: i2b2 ObservationFact table
    """
    df = extract_csv(path_csv, sample)

    logging.info('Transforming text into List[ObservationFact]')
    facts = []
    for _, row in df.iterrows():
        facts.append(ObservationFact(row))

    logging.info('Ready List[ObservationFact]')
    return facts


def extract_csv_patients(path_csv: str, sample=1.0) -> List[PatientDimension]:
    """
    :param path_csv: /path/to/file.csv
    :param sample: %percentage of file to read
    :return: List i2b2 patient dimension table
    """
    df = extract_csv(path_csv, sample)

    logging.info('Transforming text into List[PatientDimension]')
    patients = []
    for _, row in df.iterrows():
        patients.append(PatientDimension(row))

    logging.info('Ready List[PatientDimension]')
    return patients


def extract_csv_visits(path_csv: str, sample=1.0) -> List[VisitDimension]:
    """
    :param path_csv: /path/to/file.csv
    :param sample: %percentage of file to read
    :return: List i2b2 visit dimension table
    """
    df = extract_csv(path_csv, sample)

    logging.info('Transforming text into List[VisitDimension]')
    visits = []
    for _, row in df.iterrows():
        visits.append(VisitDimension(row))

    logging.info('Ready List[VisitDimension]')
    return visits