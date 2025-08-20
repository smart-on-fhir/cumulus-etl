"""
Tests for nlp/openai.py and etl/tasks/nlp_tasks.py.

Uses some studies as sample tasks, to exercise the full pipeline, but does not do full testing
of those studies - that code is elsewhere in study-specific test files.
"""

import filecmp
import os
from unittest import mock

import ddt
import openai
import pyarrow
import pydantic

from cumulus_etl import common, errors, nlp
from cumulus_etl.etl.studies import covid_symptom, irae
from cumulus_etl.etl.studies.irae.irae_tasks import DSAMention, DSAPresent
from tests import i2b2_mock_data
from tests.nlp.utils import OpenAITestCase


class TestWithSpansNLPTasks(OpenAITestCase):
    """Tests local NLP with spans and similar shared/generic code"""

    MODEL_ID = "openai/gpt-oss-120b"

    def default_content(self) -> pydantic.BaseModel:
        return DSAMention(
            dsa_history=False,
            dsa_mentioned=False,
            dsa_present=DSAPresent("None of the above"),
        )

    def prep_docs(self, docref: dict | None = None):
        """Create two docs for input"""
        docref = docref or i2b2_mock_data.documentreference("foo")
        self.make_json("DocumentReference", "1", **docref)
        self.make_json("DocumentReference", "2", **i2b2_mock_data.documentreference("bar"))

    async def assert_failed_doc(self, msg: str):
        task = irae.IraeGptOss120bTask(self.job_config, self.scrubber)
        with self.assertLogs(level="WARN") as cm:
            await task.run()

        # Confirm we printed a warning
        self.assertEqual(len(cm.output), 1, cm.output)
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
        self.assertEqual(
            batch.rows[0]["note_ref"], f"DocumentReference/{self.codebook.db.resource_hash('2')}"
        )

    async def test_gpt_oss_120_env_url_override(self):
        """Verify we can override the default URL."""
        self.patch_dict(os.environ, {"CUMULUS_GPT_OSS_120B_URL": ""})
        self.assertEqual(nlp.GptOss120bModel().url, "http://localhost:8086/v1")

        self.patch_dict(os.environ, {"CUMULUS_GPT_OSS_120B_URL": "https://blarg/"})
        self.assertEqual(nlp.GptOss120bModel().url, "https://blarg/")

    async def test_llama4_scout_env_url_override(self):
        """Verify we can override the default URL."""
        self.patch_dict(os.environ, {"CUMULUS_LLAMA4_SCOUT_URL": ""})
        self.assertEqual(nlp.Llama4ScoutModel().url, "http://localhost:8087/v1")

        self.patch_dict(os.environ, {"CUMULUS_LLAMA4_SCOUT_URL": "https://blarg/"})
        self.assertEqual(nlp.Llama4ScoutModel().url, "https://blarg/")

    async def test_caching(self):
        """Verify we cache results"""
        docref0 = i2b2_mock_data.documentreference()
        self.make_json("DocumentReference", "0", **docref0)
        self.assertFalse(os.path.exists(f"{self.phi_dir}/nlp-cache"))

        self.mock_response()
        await irae.IraeGptOss120bTask(self.job_config, self.scrubber).run()

        self.assertEqual(self.mock_create.call_count, 1)
        cache_dir = f"{self.phi_dir}/nlp-cache/irae__nlp_gpt_oss_120b_v1/06ee"
        cache_file = f"{cache_dir}/sha256-06ee538c626fbf4bdcec2199b7225c8034f26e2b46a7b5cb7ab385c8e8c00efa.cache"
        self.assertEqual(
            common.read_json(cache_file),
            {
                "id": "test-id",
                "choices": [
                    {
                        "finish_reason": "stop",
                        "index": 0,
                        "message": {
                            "parsed": {
                                "dsa_history": False,
                                "dsa_mentioned": False,
                                "dsa_present": "None of the above",
                            },
                            "role": "assistant",
                        },
                    },
                ],
                "created": 1723143708,
                "model": "test-model",
                "object": "chat.completion",
                "system_fingerprint": "test-fp",
            },
        )

        await irae.IraeGptOss120bTask(self.job_config, self.scrubber).run()
        self.assertEqual(self.mock_create.call_count, 1)

        # Confirm that if we remove the cache file, we call the endpoint again
        self.mock_response()
        os.remove(cache_file)
        await irae.IraeGptOss120bTask(self.job_config, self.scrubber).run()
        self.assertEqual(self.mock_create.call_count, 2)

    async def test_init_check_unreachable(self):
        """Verify we bail if the server isn't reachable"""
        self.mock_client.models.list = self.mock_model_list(error=True)

        with self.assertRaises(SystemExit) as cm:
            await irae.IraeGptOss120bTask.init_check()
        self.assertEqual(errors.SERVICE_MISSING, cm.exception.code)

    async def test_init_check_config(self):
        """Verify we check the server properties"""
        # Happy path
        await irae.IraeGptOss120bTask.init_check()

        # Bad model ID
        self.mock_client.models.list = self.mock_model_list("bogus-model")
        with self.assertRaises(SystemExit) as cm:
            await irae.IraeGptOss120bTask.init_check()
        self.assertEqual(errors.SERVICE_MISSING, cm.exception.code)

    async def test_output_fields(self):
        self.make_json("DocumentReference", "1", **i2b2_mock_data.documentreference("foo"))
        self.mock_response(
            content=DSAMention(
                spans=["oo"],
                dsa_history=False,
                dsa_mentioned=True,
                dsa_present=DSAPresent("None of the above"),
            )
        )
        await irae.IraeGptOss120bTask(self.job_config, self.scrubber).run()

        self.assertEqual(self.format.write_records.call_count, 1)
        batch = self.format.write_records.call_args[0][0]
        self.assertEqual(len(batch.rows), 1)
        self.assertEqual(
            batch.rows[0],
            {
                "note_ref": "DocumentReference/"
                "2c436339b1a3f4ef39ed6764e1f2988113feef8d9ed1b24e49ceb058684b9d45",
                "encounter_ref": "Encounter/"
                "67e6d98f54585262808e2de313b103ba230272c2843d9b553e41ff9c36d6ada2",
                "subject_ref": "Patient/"
                "6beb306dc5b91513f353ecdb6aaedee8a9864b3a2f20d91f0d5b27510152acf2",
                "generated_on": "2021-09-14T21:23:45+00:00",
                "system_fingerprint": "test-fp",
                "task_version": 1,
                "result": {
                    "spans": [(1, 3)],
                    "dsa_history": False,
                    "dsa_mentioned": True,
                    "dsa_present": "None of the above",
                },
            },
        )

    async def test_trailing_whitespace_removed(self):
        self.make_json(
            "DocumentReference",
            "1",
            **i2b2_mock_data.documentreference("Test   \n  lines  "),
        )
        self.mock_response()
        await irae.IraeGptOss120bTask(self.job_config, self.scrubber).run()

        self.assertEqual(self.mock_create.call_count, 1)
        kwargs = self.mock_create.call_args.kwargs
        for message in kwargs["messages"]:
            if message["role"] == "user":
                self.assertIn("\nTest\n  lines", message["content"])
                break
        else:
            assert False, "No user message found"

    async def test_span_conversion(self):
        self.make_json(
            "DocumentReference",
            "1",
            **i2b2_mock_data.documentreference("Test   NOTE. !Hello!"),
        )

        self.mock_response(
            content=DSAMention(
                spans=["Test", "TEST  \n Note.", " !Hello!\n", "nope"],
                dsa_present=DSAPresent("None of the above"),
                dsa_history=True,
                dsa_mentioned=True,
            )
        )
        await irae.IraeGptOss120bTask(self.job_config, self.scrubber).run()

        self.assertEqual(self.format.write_records.call_count, 1)
        batch = self.format.write_records.call_args[0][0]
        self.assertEqual(len(batch.rows), 1)
        self.assertEqual(batch.rows[0]["result"]["spans"], [(0, 4), (0, 11), (14, 19)])

    async def test_span_conversion_in_schema(self):
        schema = irae.IraeGptOss120bTask.get_schema(None, [])
        result_index = schema.get_field_index("result")
        result_type = schema.field(result_index).type
        spans_index = result_type.get_field_index("spans")
        span_type = result_type.field(spans_index).type
        self.assertEqual(span_type, pyarrow.list_(pyarrow.list_(pyarrow.int32(), 2)))

    async def test_no_encounter_error(self):
        docref = i2b2_mock_data.documentreference("foo")
        del docref["context"]
        self.prep_docs(docref)
        self.mock_response()
        await self.assert_failed_doc("No encounters for ")

    async def test_network_error(self):
        self.prep_docs()
        self.responses.append(openai.APIError("oops", mock.MagicMock(), body=None))
        self.mock_response()
        await self.assert_failed_doc("NLP failed for .*: oops")

    async def test_incomplete_response_error(self):
        self.prep_docs()
        self.mock_response(finish_reason="length")
        self.mock_response()
        await self.assert_failed_doc("NLP server response didn't complete for .*: length")

    async def test_bad_json_error(self):
        self.prep_docs()
        self.responses.append(pydantic.ValidationError.from_exception_data("Fake error", []))
        self.mock_response()
        await self.assert_failed_doc(
            "NLP failed for DocumentReference/1: 0 validation errors for Fake error"
        )


@ddt.ddt
class TestAzureNLPTasks(OpenAITestCase):
    """Tests the Azure specific code"""

    MODEL_ID = "gpt-35-turbo-0125"

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
        self.patch_dict(os.environ, env, clear=True)
        if success:
            await task.init_check()
        else:
            with self.assertRaises(SystemExit):
                await task.init_check()
