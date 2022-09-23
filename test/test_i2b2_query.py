"""Tests for oracle queries"""

import unittest
from cumulus.i2b2.oracle.query import *  # pylint: disable=wildcard-import,unused-wildcard-import


def header(text):
    print('##############################################')
    print(text)


def pretty(text):
    print('-----------------------------------------------')
    print(text)


class TestI2b2Sql(unittest.TestCase):
    """Test case for sql queries"""

    def test_count_import_date(self):
        header('# count_import_date')
        pretty(count_by_date_group())

    def test_sql_patient(self):
        header('# patient')
        pretty(sql_patient() + limit(20))
        pretty(count_by_date_group(Table.patient))  # Null dates?

    def test_sql_provider(self):
        header('# provider')
        pretty(sql_provider() + limit(20))
        pretty(count_by_date_group(Table.provider))  # Null dates?

    def test_sql_visit(self):
        header('# visit')
        pretty(sql_visit() + limit(20))
        pretty(count_by_date_group(Table.visit))

    def test_sql_observation_fact(self):
        header('# observation_fact')
        pretty(sql_observation_fact() + limit(20))
        pretty(count_by_date_group(Table.observation_fact))
        pretty(count_by_date_group(Table.observation_fact, 'UPDATE_DATE'))

    def test_sql_concept(self):
        header('# concept_dimension')
        pretty(sql_concept() + limit(20))
        pretty(count_by_date_group(Table.concept))


if __name__ == '__main__':
    unittest.main()
