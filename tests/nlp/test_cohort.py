"""Tests for cohort filtering in NLP tasks."""

import base64
from unittest import mock

import ddt
import pydantic

from cumulus_etl import common, errors
from cumulus_etl.etl.studies.irae.irae_tasks import DSAMention, DSAPresent
from tests.etl import BaseEtlSimple
from tests.nlp.utils import OpenAITestCase


@ddt.ddt
class TestCohorts(OpenAITestCase, BaseEtlSimple):
    MODEL_ID = "openai/gpt-oss-120b"

    def setUp(self):
        super().setUp()
        self.make_docs(
            [("doc1.1", "pat1"), ("doc1.2", "pat1"), ("doc2.1", "pat2")], "DocumentReference"
        )
        self.make_docs(
            [("dx3.1", "pat3"), ("dx3.1", "pat3"), ("dx4.1", "pat4")], "DiagnosticReport"
        )

    def default_content(self) -> pydantic.BaseModel:
        return DSAMention(
            dsa_history=False,
            dsa_mentioned=False,
            dsa_present=DSAPresent("None of the above"),
        )

    def make_docs(self, docs: list[tuple[str, str]], res_type: str) -> None:
        with common.NdjsonWriter(f"{self.tmpdir}/{res_type}.ndjson") as writer:
            for doc in docs:
                if res_type == "DocumentReference":
                    note = {
                        "resourceType": res_type,
                        "id": doc[0],
                        "subject": {"reference": f"Patient/{doc[1]}"},
                        "context": {"encounter": [{"reference": f"Encounter/enc-{doc[0]}"}]},
                        "content": [
                            {
                                "attachment": {
                                    "contentType": "text/plain",
                                    "data": base64.standard_b64encode(doc[0].encode()).decode(),
                                }
                            }
                        ],
                    }
                else:
                    note = {
                        "resourceType": res_type,
                        "id": doc[0],
                        "subject": {"reference": f"Patient/{doc[1]}"},
                        "encounter": {"reference": f"Encounter/enc-{doc[0]}"},
                        "presentedForm": [
                            {
                                "contentType": "text/plain",
                                "data": base64.standard_b64encode(doc[0].encode()).decode(),
                            },
                        ],
                    }
                writer.write(note)

    def make_cohort_csv(self, rows: list[str]) -> str:
        common.write_text(f"{self.tmpdir}/cohort.csv", "\n".join(rows))
        return f"{self.tmpdir}/cohort.csv"

    def mock_athena(self, rows: list[str]):
        # First query: returning a count (minus headers)
        count = mock.MagicMock()
        count.fetchone.return_value = [str(len(rows) - 1)]

        # Second query: actual results in a csv
        results = mock.MagicMock()
        results.output_location = self.make_cohort_csv(rows)

        # cursor()
        cursor = mock.MagicMock()
        answers = iter([count, results])

        def fake_execute(query):
            if not query.endswith('FROM "cohort__test"'):
                raise ValueError("bad table")
            return next(answers)

        cursor.execute.side_effect = fake_execute

        # connection
        conn = mock.MagicMock()
        conn.cursor.return_value = cursor

        # connect()
        connect = self.patch("pyathena.connect")

        def fake_connect(**kwargs):
            if kwargs.get("schema_name") != "db":
                raise ValueError("bad database")
            return conn

        connect.side_effect = fake_connect

    async def run_etl(self, *args):
        await super().run_etl(
            *args, input_path=self.tmpdir, tasks=["irae__nlp_gpt_oss_120b"], nlp=True
        )

    async def test_uses_note_ids(self):
        """Confirm we ignore patient IDs if we have a subset already chosen for note IDs"""
        path = self.make_cohort_csv(
            ["patient_id,docref_id,diagnosticreport_id", "pat1,doc1.1,dx3.1"]
        )

        self.mock_response()
        self.mock_response()
        await self.run_etl(f"--cohort-csv={path}")

        self.assertEqual(self.mock_create.call_count, 2)
        model_args = self.mock_create.call_args_list[0][1]
        self.assertIn("dx3.1", model_args["messages"][1]["content"])
        model_args = self.mock_create.call_args_list[1][1]
        self.assertIn("doc1.1", model_args["messages"][1]["content"])

    async def test_uses_patient_ids(self):
        """Confirm we read all patient docs, if only patients were specified"""
        path = self.make_cohort_csv(["subject_ref", "Patient/pat1"])

        self.mock_response()
        self.mock_response()
        await self.run_etl(f"--cohort-csv={path}")

        self.assertEqual(self.mock_create.call_count, 2)
        model_args = self.mock_create.call_args_list[0][1]
        self.assertIn("doc1.1", model_args["messages"][1]["content"])
        model_args = self.mock_create.call_args_list[1][1]
        self.assertIn("doc1.2", model_args["messages"][1]["content"])

    async def test_anon_doc_ids(self):
        """Confirm we read all patient docs, if only patients were specified"""
        path = self.make_cohort_csv(
            [
                "note_ref",
                "DocumentReference/"
                "8b735e9d807f915d37b6d5f3eecf275f9fc54d402a49def7bc66136edb69c7b9",
            ]
        )

        self.mock_response()
        await self.run_etl(f"--cohort-anon-csv={path}")

        self.assertEqual(self.mock_create.call_count, 1)
        model_args = self.mock_create.call_args_list[0][1]
        self.assertIn("doc2.1", model_args["messages"][1]["content"])

    async def test_anon_patient_ids(self):
        """Confirm we read all patient docs, if only patients were specified"""
        path = self.make_cohort_csv(
            ["patient_id", "142d1d12327eb248ed8c69346b07a1f030d58f6c5cbdcb15c95a39f56a02f785"]
        )

        self.mock_response()
        await self.run_etl(f"--cohort-anon-csv={path}")

        self.assertEqual(self.mock_create.call_count, 1)
        model_args = self.mock_create.call_args_list[0][1]
        self.assertIn("doc2.1", model_args["messages"][1]["content"])

    @ddt.data(True, False)
    async def test_athena_happy_path(self, inline_db):
        self.mock_athena(
            ["patient_id", "142d1d12327eb248ed8c69346b07a1f030d58f6c5cbdcb15c95a39f56a02f785"]
        )

        self.mock_response()
        if inline_db:
            args = ["--cohort-athena-table=db.cohort__test"]
        else:
            args = ["--cohort-athena-table=cohort__test", "--athena-database=db"]
        await self.run_etl(*args)

        self.assertEqual(self.mock_create.call_count, 1)
        model_args = self.mock_create.call_args_list[0][1]
        self.assertIn("doc2.1", model_args["messages"][1]["content"])

    async def test_athena_sanity_check_bad_values(self):
        """This is testing our test framework more than the code, but it's good to confirm"""
        self.mock_athena(["patient_id", "abcd"])

        with self.assertRaisesRegex(ValueError, "bad database"):
            await self.run_etl("--cohort-athena-table=bogus.cohort__test")

        with self.assertRaisesRegex(ValueError, "bad table"):
            await self.run_etl("--cohort-athena-table=db.bogus")

    async def test_too_many_cohort_args(self):
        with self.assert_fatal_exit(errors.MULTIPLE_COHORT_ARGS):
            await self.run_etl("--cohort-athena-table=db.cohort__test", "--cohort-csv=hello.txt")

    async def test_csv_not_found(self):
        with self.assertRaises(FileNotFoundError):
            await self.run_etl(f"--cohort-csv={self.tmpdir}/nope.csv")

    async def test_missing_athena_db(self):
        with self.assert_fatal_exit(errors.ATHENA_DATABASE_MISSING):
            await self.run_etl("--cohort-athena-table=nope")

    async def test_invalid_athena_table_name(self):
        with self.assert_fatal_exit(errors.ATHENA_TABLE_NAME_INVALID):
            await self.run_etl("--cohort-athena-table=db.hell;o")

    async def test_no_ref_id_columns(self):
        self.mock_athena(["condition_id", "abc"])
        with self.assert_fatal_exit(errors.COHORT_NOT_FOUND):
            await self.run_etl("--cohort-athena-table=db.cohort__test")

    async def test_athena_table_too_large(self):
        self.mock_athena(["a"] * 20_002)  # one extra for header, one extra to cross threshold
        with self.assert_fatal_exit(errors.ATHENA_TABLE_TOO_BIG):
            await self.run_etl("--cohort-athena-table=db.cohort__test")

        # Do it again with the allow arg
        self.mock_athena(["a"] * 20_002)
        with self.assert_fatal_exit(errors.COHORT_NOT_FOUND):  # different error now
            await self.run_etl("--cohort-athena-table=db.cohort__test", "--allow-large-cohort")

    async def test_no_patient_defined(self):
        with common.NdjsonWriter(f"{self.tmpdir}/docs.ndjson") as writer:
            writer.write(
                {
                    "resourceType": "DocumentReference",
                    "id": "doc1",
                    "context": {"encounter": [{"reference": "Encounter/enc1"}]},
                    "content": [
                        {
                            "attachment": {
                                "contentType": "text/plain",
                                "data": base64.standard_b64encode(b"doc1").decode(),
                            }
                        }
                    ],
                }
            )

        path = self.make_cohort_csv(["patient_id", "a"])
        await self.run_etl(f"--cohort-csv={path}")
        self.assertEqual(self.mock_create.call_count, 0)
