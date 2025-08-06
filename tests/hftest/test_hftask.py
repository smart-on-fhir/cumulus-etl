"""Tests for etl/studies/hftest/"""

import contextlib
import os
from unittest import mock

import httpx
import openai

from cumulus_etl import common, errors, nlp
from cumulus_etl.etl.studies import hftest
from tests import i2b2_mock_data
from tests.etl import BaseEtlSimple, TaskTestCase

INSTRUCTION = (
    "You will be given a clinical note, and you should reply with a short summary of that note."
)


@contextlib.contextmanager
def mock_prompt(texts: str | list[str]):
    with mock.patch("openai.AsyncClient.responses") as mock_responses:
        mock_answer = mock.MagicMock()
        mock_answer.output_text = "Patient has a fever."
        mock_responses.create = mock.AsyncMock(return_value=mock_answer)
        yield mock_responses.create

    if isinstance(texts, str):
        texts = [texts]

    assert mock_responses.create.call_count == len(texts)
    for index, text in enumerate(texts):
        call = mock_responses.create.call_args_list[index]
        assert call[1]["input"] == text
        assert call[1]["instructions"] == INSTRUCTION
        assert call[1]["model"] == "meta-llama/Llama-2-13b-chat-hf"
        assert call[1]["temperature"] == 0


@contextlib.contextmanager
def mock_models(model_id: str = "meta-llama/Llama-2-13b-chat-hf", error: bool = False):
    with mock.patch("openai.AsyncClient.models") as model_mock:

        async def async_list():
            mock_answer = mock.MagicMock()
            mock_answer.id = model_id
            if error:
                raise openai.APIConnectionError(request=httpx.Request("GET", "blarg"))
            yield mock_answer

        model_mock.list = async_list
        yield


class TestHuggingFaceTestTask(TaskTestCase):
    """Test case for HuggingFaceTestTask"""

    async def test_happy_path(self):
        """Verify we summarize a basic note properly"""
        docref0 = i2b2_mock_data.documentreference()
        self.make_json("DocumentReference", "0", **docref0)

        with mock_prompt(i2b2_mock_data.DOCREF_TEXT):
            await hftest.HuggingFaceTestTask(self.job_config, self.scrubber).run()

        self.assertEqual(1, self.format.write_records.call_count)
        batch = self.format.write_records.call_args[0][0]
        self.assertEqual(1, len(batch.rows))
        expected_id = self.codebook.db.resource_hash("0")
        self.assertEqual(
            {
                "id": expected_id,
                "docref_id": expected_id,
                "summary": "Patient has a fever.",
                "generated_on": "2021-09-14T21:23:45+00:00",
                "task_version": hftest.HuggingFaceTestTask.task_version,
            },
            batch.rows[0],
        )

    async def test_env_url_override(self):
        """Verify we can override the default URL."""
        self.patch_dict(os.environ, {"CUMULUS_LLAMA2_URL": ""})
        self.assertEqual(nlp.Llama2Model().url, "http://localhost:8086/v1")

        self.patch_dict(os.environ, {"CUMULUS_LLAMA2_URL": "https://blarg/"})
        self.assertEqual(nlp.Llama2Model().url, "https://blarg/")

    async def test_caching(self):
        """Verify we cache results"""
        docref0 = i2b2_mock_data.documentreference()
        self.make_json("DocumentReference", "0", **docref0)

        self.assertFalse(os.path.exists(f"{self.phi_dir}/ctakes-cache"))
        with mock_prompt([i2b2_mock_data.DOCREF_TEXT] * 2) as mock_create:
            await hftest.HuggingFaceTestTask(self.job_config, self.scrubber).run()

            self.assertEqual(mock_create.call_count, 1)
            cache_dir = f"{self.phi_dir}/nlp-cache/hftest__summary_v0/06ee/"
            cache_file = f"{cache_dir}/sha256-06ee538c626fbf4bdcec2199b7225c8034f26e2b46a7b5cb7ab385c8e8c00efa.cache"
            self.assertEqual("Patient has a fever.", common.read_text(cache_file))

            await hftest.HuggingFaceTestTask(self.job_config, self.scrubber).run()
            self.assertEqual(mock_create.call_count, 1)

            # Confirm that if we remove the cache file, we call the endpoint again
            os.remove(cache_file)
            await hftest.HuggingFaceTestTask(self.job_config, self.scrubber).run()
            self.assertEqual(mock_create.call_count, 2)

    async def test_init_check_unreachable(self):
        """Verify we bail if the server isn't reachable"""
        with mock_models(error=True):
            with self.assertRaises(SystemExit) as cm:
                await hftest.HuggingFaceTestTask.init_check()
        self.assertEqual(errors.SERVICE_MISSING, cm.exception.code)

    async def test_init_check_config(self):
        """Verify we check the server properties"""
        # Happy path
        with mock_models():
            await hftest.HuggingFaceTestTask.init_check()

        # Bad model ID
        with mock_models("bogus/Llama-2-13b-chat-hf"):
            with self.assertRaises(SystemExit) as cm:
                await hftest.HuggingFaceTestTask.init_check()
            self.assertEqual(errors.SERVICE_MISSING, cm.exception.code)


class TestHuggingFaceETL(BaseEtlSimple):
    """Tests the end-to-end ETL of the hftest tasks."""

    DATA_ROOT = "hftest"

    async def test_basic_etl(self):
        with mock_models():
            with mock_prompt(["Test note 1", "Test note 2"]):
                await self.run_etl(tasks=["hftest__summary"])
        self.assert_output_equal()
