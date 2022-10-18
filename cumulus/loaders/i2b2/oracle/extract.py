"""Extract data types from oracle"""

from typing import List
import logging

from cumulus.loaders.i2b2.schema import ObservationFact, PatientDimension, VisitDimension
from cumulus.loaders.i2b2.schema import ConceptDimension, ProviderDimension
from cumulus.loaders.i2b2.oracle import connect, query


def execute(sql_statement: str):
    """
    :param sql_statement: SQL
    :return: iterable
    """
    cursor = connect.connect().cursor()
    return cursor.execute(sql_statement)


def list_observation_fact() -> List[ObservationFact]:
    logging.info('Extracting List[ObservationFact]')
    facts = []

    for row in execute(query.sql_observation_fact()):
        facts.append(ObservationFact(row))

    logging.info('Ready List[ObservationFact]')
    return facts


def list_patient() -> List[PatientDimension]:
    logging.info('Extracting List[PatientDimension]')
    patients = []
    for row in execute(query.sql_patient()):
        patients.append(PatientDimension(row))

    logging.info('Ready List[PatientDimension]')
    return patients


def list_visit() -> List[VisitDimension]:
    logging.info('Extracting List[VisitDimension]')
    visits = []
    for row in execute(query.sql_visit()):
        visits.append(VisitDimension(row))

    logging.info('Ready List[VisitDimension]')
    return visits


def list_concept() -> List[ConceptDimension]:
    logging.info('Extracting List[ConceptDimension]')
    concepts = []
    for row in execute(query.sql_visit()):
        concepts.append(ConceptDimension(row))

    logging.info('Ready List[ConceptDimension]')
    return concepts


def list_provider() -> List[ProviderDimension]:
    logging.info('Extracting List[ProviderDimension]')
    providers = []
    for row in execute(query.sql_provider()):
        providers.append(ProviderDimension(row))

    logging.info('Ready List[ProviderDimension]')
    return providers
