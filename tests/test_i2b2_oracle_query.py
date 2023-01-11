"""Tests for oracle queries"""

import unittest

from cumulus import common
from cumulus.loaders.i2b2 import schema
from cumulus.loaders.i2b2.oracle import query


def pretty(text):
    print("-----------------------------------------------")
    print(text)


class TestOracleQueries(unittest.TestCase):
    """
    Test case for sql queries

    I know a lot of these tests feel odd:
    (a) a lot of them print sql to the console. This is just for ease of generating some SQL to use manually.
    (b) the few asserts are testing whole copy and pasted strings. We want to be alerted if *anything* changes
        in these sensitive queries, so we're being a bit overboard with the unit checks.

    Integration testing this is not super easy, so this is what we've got.
    """

    def setUp(self) -> None:
        super().setUp()
        self.maxDiff = None  # pylint: disable=invalid-name

    def test_list_patient(self):
        common.print_header("# patient")
        pretty(query.sql_patient() + query.limit(20))
        pretty(query.count_by_date_group(schema.Table.patient))  # Null dates?
        self.assertEqual(
            "select PATIENT_NUM, "
            "to_char(cast(BIRTH_DATE as date), 'YYYY-MM-DD') as BIRTH_DATE, "
            "to_char(cast(DEATH_DATE as date), 'YYYY-MM-DD') as DEATH_DATE, "
            "SEX_CD, RACE_CD, ZIP_CD "
            "\n from patient_dimension",
            query.sql_patient(),
        )

    def test_sql_provider(self):
        common.print_header("# provider")
        pretty(query.sql_provider() + query.limit(20))
        pretty(query.count_by_date_group(schema.Table.provider))  # Null dates?
        self.assertEqual(
            "select PROVIDER_ID, PROVIDER_PATH, NAME_CHAR, "
            "to_char(cast(IMPORT_DATE as date), 'YYYY-MM-DD') as IMPORT_DATE "
            "\n from provider_dimension",
            query.sql_provider(),
        )

    def test_sql_visit(self):
        common.print_header("# visit")
        pretty(query.sql_visit() + query.limit(20))
        pretty(query.count_by_date_group(schema.Table.visit))
        self.assertEqual(
            "select ENCOUNTER_NUM, PATIENT_NUM, LOCATION_CD, INOUT_CD, "
            "to_char(cast(START_DATE as date), 'YYYY-MM-DD') as START_DATE, "
            "to_char(cast(END_DATE as date), 'YYYY-MM-DD') as END_DATE, "
            "to_char(cast(IMPORT_DATE as date), 'YYYY-MM-DD') as IMPORT_DATE, "
            "LENGTH_OF_STAY "
            "\n from visit_dimension",
            query.sql_visit(),
        )

    def test_sql_observation_fact(self):
        common.print_header("# observation_fact")
        pretty(query.sql_observation_fact("Diagnosis") + query.limit(20))
        pretty(query.count_by_date_group(schema.Table.observation_fact))
        pretty(query.count_by_date_group(schema.Table.observation_fact, "UPDATE_DATE"))
        self.assertEqual(
            "select O.PATIENT_NUM, O.PROVIDER_ID, O.ENCOUNTER_NUM, "
            "to_char(cast(O.START_DATE as date), 'YYYY-MM-DD') as START_DATE, "
            "to_char(cast(O.END_DATE as date), 'YYYY-MM-DD') as END_DATE, "
            "O.LOCATION_CD, O.CONCEPT_CD, O.INSTANCE_NUM, "
            "to_char(cast(O.IMPORT_DATE as date), 'YYYY-MM-DD') as IMPORT_DATE, "
            "O.TVAL_CHAR, O.VALTYPE_CD, O.VALUEFLAG_CD, O.OBSERVATION_BLOB "
            "\n from observation_fact O join concept_dimension C on C.CONCEPT_CD = O.CONCEPT_CD "
            "where instr(C.CONCEPT_PATH, 'Notes') = 7",
            query.sql_observation_fact("Notes"),
        )

    def test_sql_concept(self):
        common.print_header("# concept_dimension")
        pretty(query.sql_concept() + query.limit(20))
        pretty(query.count_by_date_group(schema.Table.concept))
        self.assertEqual(
            "select CONCEPT_CD, NAME_CHAR, SOURCESYSTEM_CD, CONCEPT_BLOB, "
            "to_char(cast(IMPORT_DATE as date), 'YYYY-MM-DD') as IMPORT_DATE "
            "\n from concept_dimension",
            query.sql_concept(),
        )


if __name__ == "__main__":
    unittest.main()
