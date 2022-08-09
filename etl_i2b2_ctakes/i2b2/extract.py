from typing import List
import logging
import pandas
from i2b2.i2b2_schema import ObservationFact, PatientDimension, VisitDimension

def extract_csv(path_csv:str, sample=1.0) -> pandas.DataFrame:
    """
    :param path_csv: /path/to/file.csv
    :param sample: %percentage of file to read
    :return: pandas Dataframe
    """
    logging.info(f'Reading csv {path_csv} ...')
    df = pandas.read_csv(path_csv, dtype=str).sample(frac=sample)
    logging.info(f'Done reading {path_csv} .')
    return df

def extract_csv_observation_fact(path_csv:str, sample=1.0) -> List[ObservationFact]:
    """
    :param path_csv: /path/to/file.csv
    :param sample: %percentage of file to read
    :return: i2b2 ObservationFact table
    """
    df = extract_csv(path_csv, sample)

    logging.info('Transforming text into List[ObservationFact]')
    facts = list()
    for index, row in df.iterrows():
        facts.append(ObservationFact(row))

    logging.info('Ready List[ObservationFact]')
    return facts

def extract_csv_patient(path_csv:str, sample=1.0) -> List[PatientDimension]:
    """
    :param path_csv: /path/to/file.csv
    :param sample: %percentage of file to read
    :return: List i2b2 patient dimension table
    """
    df = extract_csv(path_csv, sample)

    logging.info('Transforming text into List[PatientDimension]')
    patients = list()
    for index, row in df.iterrows():
        patients.append(PatientDimension(row))

    logging.info('Ready List[PatientDimension]')
    return patients

def extract_csv_visits(path_csv:str, sample=1.0) -> List[VisitDimension]:
    """
    :param path_csv: /path/to/file.csv
    :param sample: %percentage of file to read
    :return: List i2b2 visit dimension table
    """
    df = extract_csv(path_csv, sample)

    logging.info('Transforming text into List[VisitDimension]')
    visits = list()
    for index, row in df.iterrows():
        visits.append(VisitDimension(row))

    logging.info('Ready List[VisitDimension]')
    return visits