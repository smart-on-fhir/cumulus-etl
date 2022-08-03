from typing import List
import logging
import pandas
import i2b2

def extract_csv(notes_csv:str, sample=1.0) -> pandas.DataFrame:
    """
    :param notes_csv:
    :param sample:
    :return:
    """
    logging.info(f'Reading csv {notes_csv} ...')
    df = pandas.read_csv(notes_csv, dtype=str).sample(frac=sample)
    logging.info(f'Done reading {notes_csv} .')
    return df

def extract_sql(sql_conn) -> List[i2b2.ObservationFact]:
    """
    :param sql_conn: https://pypi.org/project/records/
    :return:
    """
    logging.fatal('No implementation. Awaiting IRB/Avillach green light.')
    return None



