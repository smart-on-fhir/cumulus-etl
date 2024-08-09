"""Tests for GPT covid symptom tasks"""

import filecmp
import hashlib
import json
import os
from unittest import mock

import ddt
import openai
from openai.types import chat

from cumulus_etl.etl.studies import covid_symptom
from tests import i2b2_mock_data
from tests.etl import TaskTestCase


@ddt.ddt
@mock.patch.dict(
    os.environ, {"AZURE_OPENAI_API_KEY": "test-key", "AZURE_OPENAI_ENDPOINT": "test-endpoint"}
)
class TestCovidSymptomGptResultsTask(TaskTestCase):
    """Test case for CovidSymptomNlpResultsGpt*Task"""

    def setUp(self):
        super().setUp()
        self.mock_client_factory = self.patch("openai.AsyncAzureOpenAI")
        self.mock_client = mock.AsyncMock()
        self.mock_client_factory.return_value = self.mock_client
        self.mock_create = self.mock_client.chat.completions.create
        self.responses = []

    def prep_docs(self, docref: dict | None = None):
        """Create two docs for input"""
        docref = docref or i2b2_mock_data.documentreference("foo")
        self.make_json("DocumentReference", "1", **docref)
        self.make_json("DocumentReference", "2", **i2b2_mock_data.documentreference("bar"))

    def mock_response(
        self, found: bool = False, finish_reason: str = "stop", content: str | None = None
    ) -> None:
        symptoms = {
            "Congestion or runny nose": found,
            "Cough": found,
            "Diarrhea": found,
            "Dyspnea": found,
            "Fatigue": found,
            "Fever or chills": found,
            "Headache": found,
            "Loss of taste or smell": found,
            "Muscle or body aches": found,
            "Nausea or vomiting": found,
            "Sore throat": found,
        }
        if content is None:
            content = json.dumps(symptoms)

        self.responses.append(
            chat.ChatCompletion(
                id="test-id",
                choices=[
                    {
                        "finish_reason": finish_reason,
                        "index": 0,
                        "message": {"content": content, "role": "assistant"},
                    }
                ],
                created=1723143708,
                model="test-model",
                object="chat.completion",
                system_fingerprint="test-fp",
            ),
        )
        self.mock_create.side_effect = self.responses

    async def assert_failed_doc(self, msg: str):
        task = covid_symptom.CovidSymptomNlpResultsGpt35Task(self.job_config, self.scrubber)
        with self.assertLogs(level="WARN") as cm:
            await task.run()

        # Confirm we printed a warning
        self.assertEqual(len(cm.output), 1)
        self.assertRegex(cm.output[0], msg)

        # Confirm we flagged and recorded the error
        self.assertTrue(task.summaries[0].had_errors)
        self.assertTrue(
            filecmp.cmp(
                f"{self.input_dir}/1.ndjson", f"{self.errors_dir}/{task.name}/nlp-errors.ndjson"
            )
        )

        # Confirm that we did write the second docref out - that we continued instead of exiting.
        self.assertEqual(self.format.write_records.call_count, 1)
        batch = self.format.write_records.call_args[0][0]
        self.assertEqual(len(batch.rows), 1)
        self.assertEqual(batch.rows[0]["id"], self.codebook.db.resource_hash("2"))

    @ddt.data(
        # env vars to set, success
        (["AZURE_OPENAI_API_KEY", "AZURE_OPENAI_ENDPOINT"], True),
        (["AZURE_OPENAI_API_KEY"], False),
        (["AZURE_OPENAI_ENDPOINT"], False),
    )
    @ddt.unpack
    async def test_requires_env(self, names, success):
        task = covid_symptom.CovidSymptomNlpResultsGpt35Task(self.job_config, self.scrubber)
        env = {name: "content" for name in names}
        with mock.patch.dict(os.environ, env, clear=True):
            self.assertEqual(await task.prepare_task(), success)
        self.assertEqual(task.summaries[0].had_errors, not success)

    async def test_gpt4_changes(self):
        """
        Verify anything that is GPT4 specific.

        The rest of the tests work with gpt 3.5.
        """
        self.make_json("DocumentReference", "doc", **i2b2_mock_data.documentreference())
        self.mock_response()

        task = covid_symptom.CovidSymptomNlpResultsGpt4Task(self.job_config, self.scrubber)
        await task.run()

        # Only the model (and sometimes the task version) are unique to the gpt4 task
        self.assertEqual(self.mock_create.call_args_list[0][1]["model"], "gpt-4")
        batch = self.format.write_records.call_args[0][0]
        self.assertEqual(batch.rows[0]["task_version"], 1)

    async def test_happy_path(self):
        self.prep_docs()
        self.mock_response(found=True)
        self.mock_response(found=False)

        task = covid_symptom.CovidSymptomNlpResultsGpt35Task(self.job_config, self.scrubber)
        await task.run()

        # Confirm we send the correct params to the server
        prompt = covid_symptom.CovidSymptomNlpResultsGpt35Task.get_prompt("foo")
        self.assertEqual(
            hashlib.md5(prompt.encode("utf8")).hexdigest(),
            "8c5025f98a6cfc94dfedbf10a82396ae",  # catch unexpected prompt changes
        )
        self.assertEqual(self.mock_create.call_count, 2)
        self.assertEqual(
            {
                "messages": [
                    {"role": "system", "content": "You are a helpful assistant."},
                    {"role": "user", "content": prompt},
                ],
                "model": "gpt-35-turbo-0125",
                "seed": 12345,
                "response_format": {"type": "json_object"},
            },
            self.mock_create.call_args_list[0][1],
        )

        # Confirm we formatted the output correctly
        self.assertEqual(self.format.write_records.call_count, 1)
        batch = self.format.write_records.call_args[0][0]
        self.assertEqual(
            [
                {
                    "id": self.codebook.db.resource_hash("1"),
                    "docref_id": self.codebook.db.resource_hash("1"),
                    "encounter_id": self.codebook.db.resource_hash("67890"),
                    "subject_id": self.codebook.db.resource_hash("12345"),
                    "generated_on": "2021-09-14T21:23:45+00:00",
                    "task_version": 1,
                    "system_fingerprint": "test-fp",
                    "symptoms": {
                        "congestion_or_runny_nose": True,
                        "cough": True,
                        "diarrhea": True,
                        "dyspnea": True,
                        "fatigue": True,
                        "fever_or_chills": True,
                        "headache": True,
                        "loss_of_taste_or_smell": True,
                        "muscle_or_body_aches": True,
                        "nausea_or_vomiting": True,
                        "sore_throat": True,
                    },
                },
                {
                    "id": self.codebook.db.resource_hash("2"),
                    "docref_id": self.codebook.db.resource_hash("2"),
                    "encounter_id": self.codebook.db.resource_hash("67890"),
                    "subject_id": self.codebook.db.resource_hash("12345"),
                    "generated_on": "2021-09-14T21:23:45+00:00",
                    "task_version": 1,
                    "system_fingerprint": "test-fp",
                    "symptoms": {
                        "congestion_or_runny_nose": False,
                        "cough": False,
                        "diarrhea": False,
                        "dyspnea": False,
                        "fatigue": False,
                        "fever_or_chills": False,
                        "headache": False,
                        "loss_of_taste_or_smell": False,
                        "muscle_or_body_aches": False,
                        "nausea_or_vomiting": False,
                        "sore_throat": False,
                    },
                },
            ],
            batch.rows,
        )
        self.assertEqual(batch.groups, set())

    async def test_caching(self):
        # Provide the fist docref using the same content as the second.
        # So the second will be cached.
        self.prep_docs(i2b2_mock_data.documentreference("bar"))
        self.mock_response(found=True)

        task = covid_symptom.CovidSymptomNlpResultsGpt35Task(self.job_config, self.scrubber)
        await task.run()

        # Confirm we only asked the server once
        self.assertEqual(self.mock_create.call_count, 1)

        # Confirm we round-tripped the data correctly
        self.assertEqual(self.format.write_records.call_count, 1)
        batch = self.format.write_records.call_args[0][0]
        self.assertEqual(len(batch.rows), 2)
        self.assertEqual(batch.rows[0]["symptoms"], batch.rows[1]["symptoms"])

    async def test_no_encounter_error(self):
        docref = i2b2_mock_data.documentreference("foo")
        del docref["context"]
        self.prep_docs(docref)
        self.mock_response()
        await self.assert_failed_doc("No encounters for docref")

    async def test_network_error(self):
        self.prep_docs()
        self.responses.append(openai.APIError("oops", mock.MagicMock(), body=None))
        self.mock_response()
        await self.assert_failed_doc("Could not connect to GPT for DocRef .*: oops")

    async def test_incomplete_response_error(self):
        self.prep_docs()
        self.mock_response(finish_reason="length")
        self.mock_response()
        await self.assert_failed_doc("GPT response didn't complete for DocRef .*: length")

    async def test_bad_json_error(self):
        self.prep_docs()
        self.mock_response(content='{"hello"')
        self.mock_response()
        await self.assert_failed_doc("Could not parse GPT results for DocRef .*: Expecting ':'")
