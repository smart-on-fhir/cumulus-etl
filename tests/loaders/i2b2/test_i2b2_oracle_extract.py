"""Tests for oracle extraction"""

import os
from unittest import mock

from cumulus_etl import common, store
from cumulus_etl.loaders.i2b2 import loader
from cumulus_etl.loaders.i2b2.oracle import extract, query
from tests import i2b2_mock_data
from tests.utils import AsyncTestCase


class TestOracleExtraction(AsyncTestCase):
    """Test case for sql queries"""

    def setUp(self) -> None:
        super().setUp()
        self.maxDiff = None

        # Mock all the sql connection/cursor/execution stuff
        connect_patcher = mock.patch("cumulus_etl.loaders.i2b2.oracle.extract.connect")
        self.addCleanup(connect_patcher.stop)
        self.mock_connect = connect_patcher.start()
        connection = mock.MagicMock()
        self.mock_connect.connect.return_value = connection
        self.mock_cursor = mock.MagicMock()
        connection.cursor.return_value = self.mock_cursor
        self.mock_execute = self.mock_cursor.execute

    def test_list_observation_fact(self):
        self.mock_cursor.__iter__.return_value = [
            {
                "OBSERVATION_BLOB": "notes",
            }
        ]
        results = extract.list_observation_fact("localhost", "Diagnosis")
        self.assertEqual(1, len(results))
        self.assertEqual("notes", results[0].observation_blob)
        self.assertEqual(
            [mock.call(query.sql_observation_fact("Diagnosis"))], self.mock_execute.call_args_list
        )

    def test_list_patient(self):
        self.mock_cursor.__iter__.return_value = [
            {
                "PATIENT_NUM": 1234,
            }
        ]
        results = extract.list_patient("localhost")
        self.assertEqual(1, len(results))
        self.assertEqual(1234, results[0].patient_num)
        self.assertEqual([mock.call(query.sql_patient())], self.mock_execute.call_args_list)

    def test_list_visit(self):
        self.mock_cursor.__iter__.return_value = [
            {
                "ENCOUNTER_NUM": 67890,
            }
        ]
        results = extract.list_visit("localhost")
        self.assertEqual(1, len(results))
        self.assertEqual(67890, results[0].encounter_num)
        self.assertEqual([mock.call(query.sql_visit())], self.mock_execute.call_args_list)

    def test_list_concept(self):
        self.mock_cursor.__iter__.return_value = [
            {
                "CONCEPT_BLOB": "snomed:3234",
            }
        ]
        results = extract.list_concept("localhost")
        self.assertEqual(1, len(results))
        self.assertEqual("snomed:3234", results[0].concept_blob)
        self.assertEqual([mock.call(query.sql_concept())], self.mock_execute.call_args_list)

    def test_list_provider(self):
        self.mock_cursor.__iter__.return_value = [
            {
                "PROVIDER_ID": 3456,
            }
        ]
        results = extract.list_provider("localhost")
        self.assertEqual(1, len(results))
        self.assertEqual(3456, results[0].provider_id)
        self.assertEqual([mock.call(query.sql_provider())], self.mock_execute.call_args_list)

    @mock.patch("cumulus_etl.loaders.i2b2.loader.oracle_extract")
    async def test_loader(self, mock_extract):
        """Verify that when our i2b2 loader is given an Oracle URL, it runs SQL against it and drops it to ndjson"""
        mock_extract.list_observation_fact.return_value = [i2b2_mock_data.condition_dim()]
        mock_extract.list_patient.return_value = [i2b2_mock_data.patient_dim()]
        mock_extract.list_visit.return_value = [i2b2_mock_data.encounter_dim()]

        root = store.Root("tcp://localhost/foo")
        oracle_loader = loader.I2b2Loader(root)
        results = await oracle_loader.load_resources({"Condition", "Encounter", "Patient"})

        # Check results
        self.assertEqual(
            {
                "Condition.ndjson",
                "Encounter.ndjson",
                "Patient.ndjson",
            },
            set(os.listdir(results.path)),
        )

        self.assertEqual(
            i2b2_mock_data.condition(),
            common.read_json(os.path.join(results.path, "Condition.ndjson")),
        )
        self.assertEqual(
            i2b2_mock_data.encounter(),
            common.read_json(os.path.join(results.path, "Encounter.ndjson")),
        )
        self.assertEqual(
            i2b2_mock_data.patient(), common.read_json(os.path.join(results.path, "Patient.ndjson"))
        )
