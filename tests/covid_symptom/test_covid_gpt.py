"""Tests for GPT covid symptom tasks"""

import hashlib

import ddt

from cumulus_etl.etl.studies import covid_symptom
from cumulus_etl.etl.studies.covid_symptom.covid_tasks import CovidSymptoms
from tests import i2b2_mock_data
from tests.nlp.utils import NlpModelTestCase


@ddt.ddt
class TestCovidSymptomGptResultsTask(NlpModelTestCase):
    """Test case for CovidSymptomNlpResultsGpt*Task"""

    def default_content(self):
        return CovidSymptoms.model_validate(
            {
                "Congestion or runny nose": True,
                "Cough": True,
                "Diarrhea": True,
                "Dyspnea": True,
                "Fatigue": True,
                "Fever or chills": True,
                "Headache": True,
                "Loss of taste or smell": True,
                "Muscle or body aches": True,
                "Nausea or vomiting": True,
                "Sore throat": True,
            }
        )

    async def test_gpt4_changes(self):
        """
        Verify anything that is GPT4 specific.

        The rest of the tests work with gpt 3.5.
        """
        self.make_json("DocumentReference", "doc", **i2b2_mock_data.documentreference())
        self.mock_azure("gpt-4")
        self.mock_response()

        task = covid_symptom.CovidSymptomNlpResultsGpt4Task(self.job_config, self.scrubber)
        await task.run()

        # Only the model (and sometimes the task version) are unique to the gpt4 task
        self.assertEqual(self.mock_create.call_args_list[0][1]["model"], "gpt-4")
        batch = self.format.write_records.call_args[0][0]
        self.assertEqual(batch.rows[0]["task_version"], 3)

    async def test_happy_path(self):
        self.make_json("DocumentReference", "1", **i2b2_mock_data.documentreference("foo"))
        self.mock_azure("gpt-35-turbo-0125")
        self.mock_response(content=self.default_content().model_dump_json(by_alias=True))

        task = covid_symptom.CovidSymptomNlpResultsGpt35Task(self.job_config, self.scrubber)
        await task.run()

        # Confirm we send the correct params to the server
        prompt = task.get_user_prompt("foo")
        self.assertEqual(
            hashlib.md5(prompt.encode("utf8")).hexdigest(),
            "8c5025f98a6cfc94dfedbf10a82396ae",  # catch unexpected prompt changes
        )
        self.assertEqual(self.mock_create.call_count, 1)
        self.assertEqual(
            {
                "messages": [
                    {"role": "system", "content": "You are a helpful assistant."},
                    {"role": "user", "content": prompt},
                ],
                "model": "gpt-35-turbo-0125",
                "seed": 12345,
                "temperature": 0,
                "timeout": 120,
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
                    "task_version": 3,
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
            ],
            batch.rows,
        )
        self.assertEqual(batch.groups, set())
