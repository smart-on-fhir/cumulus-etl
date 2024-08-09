"""Tests for etl/studies/hftest/"""

import os

import respx

from cumulus_etl import common, errors
from cumulus_etl.etl.studies import hftest
from tests import i2b2_mock_data
from tests.etl import BaseEtlSimple, TaskTestCase


def mock_prompt(
    respx_mock: respx.MockRouter, text: str, url: str = "http://localhost:8086/"
) -> respx.Route:
    full_prompt = f"""<s>[INST] <<SYS>>
You will be given a clinical note, and you should reply with a short summary of that note.
<</SYS>>

{text} [/INST]"""
    return respx_mock.post(
        url,
        json={
            "inputs": full_prompt,
            "options": {
                "wait_for_model": True,
            },
            "parameters": {
                "max_new_tokens": 1000,
            },
        },
    ).respond(json=[{"generated_text": full_prompt + " Patient has a fever."}])


def mock_info(
    respx_mock: respx.MockRouter,
    url: str = "http://localhost:8086/info",
    override: dict | None = None,
) -> respx.Route:
    response = {
        "model_id": "meta-llama/Llama-2-13b-chat-hf",
        "model_sha": "0ba94ac9b9e1d5a0037780667e8b219adde1908c",
        "sha": "09eca6422788b1710c54ee0d05dd6746f16bb681",
    }
    response.update(override or {})
    return respx_mock.get(url).respond(json=response)


class TestHuggingFaceTestTask(TaskTestCase):
    """Test case for HuggingFaceTestTask"""

    @respx.mock(assert_all_called=True)
    async def test_happy_path(self, respx_mock):
        """Verify we summarize a basic note properly"""
        docref0 = i2b2_mock_data.documentreference()
        self.make_json("DocumentReference", "0", **docref0)
        mock_prompt(respx_mock, i2b2_mock_data.DOCREF_TEXT)

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

    @respx.mock(assert_all_called=True)
    async def test_env_url_override(self, respx_mock):
        """Verify we can override the hugging face default URL."""
        docref0 = i2b2_mock_data.documentreference()
        self.make_json("DocumentReference", "0", **docref0)

        self.patch_dict(os.environ, {"CUMULUS_HUGGING_FACE_URL": "https://blarg/"})
        mock_prompt(respx_mock, i2b2_mock_data.DOCREF_TEXT, url="https://blarg/")

        await hftest.HuggingFaceTestTask(self.job_config, self.scrubber).run()
        self.assertEqual(1, self.format.write_records.call_count)

    @respx.mock(assert_all_called=True)
    async def test_caching(self, respx_mock):
        """Verify we cache results"""
        docref0 = i2b2_mock_data.documentreference()
        self.make_json("DocumentReference", "0", **docref0)
        route = mock_prompt(respx_mock, i2b2_mock_data.DOCREF_TEXT)

        self.assertFalse(os.path.exists(f"{self.phi_dir}/ctakes-cache"))
        await hftest.HuggingFaceTestTask(self.job_config, self.scrubber).run()

        self.assertEqual(1, route.call_count)
        cache_dir = f"{self.phi_dir}/nlp-cache/hftest__summary_v0/06ee/"
        cache_file = f"{cache_dir}/sha256-06ee538c626fbf4bdcec2199b7225c8034f26e2b46a7b5cb7ab385c8e8c00efa.cache"
        self.assertEqual("Patient has a fever.", common.read_text(cache_file))

        await hftest.HuggingFaceTestTask(self.job_config, self.scrubber).run()
        self.assertEqual(1, route.call_count)

        # Confirm that if we remove the cache file, we call the endpoint again
        os.remove(cache_file)
        await hftest.HuggingFaceTestTask(self.job_config, self.scrubber).run()
        self.assertEqual(2, route.call_count)

    @respx.mock(assert_all_called=True)
    async def test_init_check_unreachable(self, respx_mock):
        """Verify we bail if the server isn't reachable"""
        respx_mock.get("http://localhost:8086/info").respond(status_code=500)
        with self.assertRaises(SystemExit) as cm:
            await hftest.HuggingFaceTestTask.init_check()
        self.assertEqual(errors.SERVICE_MISSING, cm.exception.code)

    @respx.mock(assert_all_called=True)
    async def test_init_check_config(self, respx_mock):
        """Verify we check the server properties"""
        # Happy path
        mock_info(respx_mock)
        await hftest.HuggingFaceTestTask.init_check()

        # Bad model ID
        mock_info(respx_mock, override={"model_id": "bogus/Llama-2-13b-chat-hf"})
        with self.assertRaises(SystemExit) as cm:
            await hftest.HuggingFaceTestTask.init_check()
        self.assertEqual(errors.SERVICE_MISSING, cm.exception.code)

        # Bad model SHA
        mock_info(respx_mock, override={"model_sha": "bogus"})
        with self.assertRaises(SystemExit) as cm:
            await hftest.HuggingFaceTestTask.init_check()
        self.assertEqual(errors.SERVICE_MISSING, cm.exception.code)

        # Bad SHA
        mock_info(respx_mock, override={"sha": "bogus"})
        with self.assertRaises(SystemExit) as cm:
            await hftest.HuggingFaceTestTask.init_check()
        self.assertEqual(errors.SERVICE_MISSING, cm.exception.code)


class TestHuggingFaceETL(BaseEtlSimple):
    """Tests the end-to-end ETL of the hftest tasks."""

    DATA_ROOT = "hftest"

    @respx.mock(assert_all_called=True)
    async def test_basic_etl(self, respx_mock):
        mock_prompt(respx_mock, text="Test note 1")
        mock_prompt(respx_mock, text="Test note 2")
        await self.run_etl(tasks=["hftest__summary"])
        self.assert_output_equal()
