"""Extract data types from oracle"""

import time
from typing import Iterable, List

from cumulus import common
from cumulus.loaders.i2b2.schema import ObservationFact, PatientDimension, VisitDimension
from cumulus.loaders.i2b2.schema import ConceptDimension, ProviderDimension
from cumulus.loaders.i2b2.oracle import connect, query


def execute(dsn: str, desc: str, sql_statement: str) -> Iterable[dict]:
    """
    :param dsn: data source name (URL like tcp://example.com/foo)
    :param desc: description of download (like 'observ
    :param sql_statement: SQL
    :return: iterable
    """
    cursor = connect.connect(dsn).cursor()
    cursor.execute(sql_statement)
    columns = [col[0] for col in cursor.description]
    cursor.rowfactory = lambda *args: dict(zip(columns, args))

    # OK, cursor is now going to page through results and yield rows.
    # Let's add some nice printing on top of that, because this can take a long while.
    common.print_header(f"Starting SQL download for {desc}...")

    # Loop over each row, printing reports as we go
    count = 0
    prev_time = time.time()
    for row in cursor:
        yield row
        count += 1
        now = time.time()
        elapsed = now - prev_time
        if elapsed >= 30:  # print twice a minute
            print(f"  {desc}: downloaded {count:,} so far...")
            prev_time = now

    print(f"Done with {desc}! Downloaded {count:,} total.")


def list_observation_fact(dsn: str, categories: List[str]) -> List[ObservationFact]:
    """
    Grabs a single category of observation facts.

    There are many, many kinds of observation facts, and usually they map to different FHIR resources.
    Here are the known-valid categories:
        Allergies
        Clinic
        Demographics
        Diagnosis
        Insurance
        Lab View
        Medications
        Notes
        PDIA
        Procedures
        Protocols
        Service
        Specimens
        Vitals
    """
    facts = []
    desc = ",".join(categories)
    for row in execute(dsn, f"ObservationFact[{desc}]", query.sql_observation_fact(categories)):
        facts.append(ObservationFact(row))
    return facts


def list_patient(dsn: str) -> List[PatientDimension]:
    patients = []
    for row in execute(dsn, "PatientDimension", query.sql_patient()):
        patients.append(PatientDimension(row))
    return patients


def list_visit(dsn: str) -> List[VisitDimension]:
    visits = []
    for row in execute(dsn, "VisitDimension", query.sql_visit()):
        visits.append(VisitDimension(row))
    return visits


def list_concept(dsn: str) -> List[ConceptDimension]:
    concepts = []
    for row in execute(dsn, "ConceptDimension", query.sql_concept()):
        concepts.append(ConceptDimension(row))
    return concepts


def list_provider(dsn: str) -> List[ProviderDimension]:
    providers = []
    for row in execute(dsn, "ProviderDimension", query.sql_provider()):
        providers.append(ProviderDimension(row))
    return providers
