"""
Tests for nlp/models.py and etl/tasks/nlp_tasks.py.

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
from cumulus_etl.etl.studies.irae.irae_tasks import KidneyTransplantAnnotation
from tests import i2b2_mock_data
from tests.nlp.utils import NlpModelTestCase


@ddt.ddt
class TestWithSpansNLPTasks(NlpModelTestCase):
    """Tests local NLP with spans and similar shared/generic code"""

    MODEL_ID = "openai/gpt-oss-120b"

    def default_kidney(self, **kwargs) -> KidneyTransplantAnnotation:
        model_dict = {
            "donor_transplant_date_mention": {"has_mention": False, "spans": []},
            "donor_type_mention": {"has_mention": False, "spans": []},
            "donor_relationship_mention": {"has_mention": False, "spans": []},
            "donor_hla_match_quality_mention": {"has_mention": False, "spans": []},
            "donor_hla_mismatch_count_mention": {"has_mention": False, "spans": []},
            "rx_therapeutic_status_mention": {"has_mention": False, "spans": []},
            "rx_compliance_mention": {"has_mention": False, "spans": []},
            "dsa_mention": {"has_mention": False, "spans": []},
            "infection_mention": {"has_mention": False, "spans": []},
            "viral_infection_mention": {"has_mention": False, "spans": []},
            "bacterial_infection_mention": {"has_mention": False, "spans": []},
            "fungal_infection_mention": {"has_mention": False, "spans": []},
            "graft_rejection_mention": {"has_mention": False, "spans": []},
            "graft_failure_mention": {"has_mention": False, "spans": []},
            "ptld_mention": {"has_mention": False, "spans": []},
            "cancer_mention": {"has_mention": False, "spans": []},
            "deceased_mention": {"has_mention": False, "spans": []},
        }
        model_dict.update(kwargs)
        return KidneyTransplantAnnotation.model_validate(model_dict)

    def default_content(self) -> pydantic.BaseModel:
        return self.default_kidney()

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
        nlp.GptOss120bModel()
        self.assertEqual(
            self.mock_client_factory.call_args[1]["base_url"], "http://localhost:8086/v1"
        )

        self.patch_dict(os.environ, {"CUMULUS_GPT_OSS_120B_URL": "https://blarg/"})
        nlp.GptOss120bModel()
        self.assertEqual(self.mock_client_factory.call_args[1]["base_url"], "https://blarg/")

    async def test_llama4_scout_env_url_override(self):
        """Verify we can override the default URL."""
        self.patch_dict(os.environ, {"CUMULUS_LLAMA4_SCOUT_URL": ""})
        nlp.Llama4ScoutModel()
        self.assertEqual(
            self.mock_client_factory.call_args[1]["base_url"], "http://localhost:8087/v1"
        )

        self.patch_dict(os.environ, {"CUMULUS_LLAMA4_SCOUT_URL": "https://blarg/"})
        nlp.Llama4ScoutModel()
        self.assertEqual(self.mock_client_factory.call_args[1]["base_url"], "https://blarg/")

    async def test_caching(self):
        """Verify we cache results"""
        docref0 = i2b2_mock_data.documentreference()
        self.make_json("DocumentReference", "0", **docref0)
        self.assertFalse(os.path.exists(f"{self.phi_dir}/nlp-cache"))

        self.mock_response()
        await irae.IraeGptOss120bTask(self.job_config, self.scrubber).run()

        self.assertEqual(self.mock_create.call_count, 1)
        cache_dir = f"{self.phi_dir}/nlp-cache/irae__nlp_gpt_oss_120b_v3/06ee"
        cache_file = f"{cache_dir}/sha256-06ee538c626fbf4bdcec2199b7225c8034f26e2b46a7b5cb7ab385c8e8c00efa.cache"
        self.assertEqual(
            common.read_json(cache_file),
            {
                "answer": self.default_content().model_dump(mode="json", exclude_unset=True),
                "fingerprint": "test-fp",
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

        # Random error bubbles up
        self.mock_client.models.list = mock.MagicMock(side_effect=SystemExit)
        with self.assertRaises(SystemExit):
            await irae.IraeGptOss120bTask.init_check()

        # Bad model ID
        self.mock_client.models.list = self.mock_model_list("bogus-model")
        with self.assert_fatal_exit(errors.SERVICE_MISSING):
            await irae.IraeGptOss120bTask.init_check()

    async def test_output_fields(self):
        self.make_json("DocumentReference", "1", **i2b2_mock_data.documentreference("foo"))
        self.mock_response()
        await irae.IraeGptOss120bTask(self.job_config, self.scrubber).run()

        self.assertEqual(self.format.write_records.call_count, 1)
        batch = self.format.write_records.call_args[0][0]
        self.assertEqual(len(batch.rows), 1)
        del batch.rows[0]["result"]  # don't bother testing the NLP serialization, that's elsewhere
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
                "task_version": 3,
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
            content=self.default_kidney(
                dsa_mention={
                    "spans": ["Test", "TEST  \n Note.", " !Hello!\n", "nope"],
                    "has_mention": True,
                    "dsa_history": True,
                    "dsa": "None of the above",
                }
            )
        )
        await irae.IraeGptOss120bTask(self.job_config, self.scrubber).run()

        self.assertEqual(self.format.write_records.call_count, 1)
        batch = self.format.write_records.call_args[0][0]
        self.assertEqual(len(batch.rows), 1)
        self.assertEqual(
            batch.rows[0]["result"]["dsa_mention"]["spans"], [(0, 4), (0, 11), (14, 19)]
        )

    async def test_span_conversion_in_schema(self):
        schema = irae.IraeGptOss120bTask.get_schema(None, [])
        result_index = schema.get_field_index("result")
        result_type = schema.field(result_index).type
        dsa_index = result_type.get_field_index("dsa_mention")  # spot check one of the structs
        dsa_type = result_type.field(dsa_index).type
        spans_index = dsa_type.get_field_index("spans")
        span_type = dsa_type.field(spans_index).type
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
        await self.assert_failed_doc("NLP failed for .*: did not complete, .*: length")

    async def test_bad_json_error(self):
        self.prep_docs()
        self.responses.append(pydantic.ValidationError.from_exception_data("Fake error", []))
        self.mock_response()
        await self.assert_failed_doc(
            "NLP failed for DocumentReference/1: 0 validation errors for Fake error"
        )

    @ddt.data(
        "Hello\n```json\n%CONTENT%\n```\nGoodbye",
        "```\n%CONTENT%\n```",
        "%CONTENT%",
    )
    async def test_bedrock_text_parsing(self, text):
        self.make_json("DocumentReference", "1", **i2b2_mock_data.documentreference("foo"))
        self.mock_bedrock()
        content = self.default_kidney(dsa_mention={"spans": ["foo"], "has_mention": True})
        content_text = content.model_dump_json()
        self.mock_response(content=text.replace("%CONTENT%", content_text))

        await irae.IraeGptOss120bTask(self.job_config, self.scrubber).run()

        self.assertEqual(self.format.write_records.call_count, 1)
        batch = self.format.write_records.call_args[0][0]
        self.assertEqual(len(batch.rows), 1)
        self.assertEqual(
            batch.rows[0]["result"]["dsa_mention"],
            {
                "spans": [(0, 3)],
                "has_mention": True,
                "dsa_history": False,
                "dsa": "None of the above",
            },
        )

    async def test_bedrock_tool_use_parsing(self):
        self.prep_docs()
        self.mock_bedrock()
        self.mock_response()
        # Also test that we handle Claude's injected parameter parent
        self.mock_response(content={"parameter": self.default_kidney().model_dump()})

        await irae.IraeGptOss120bTask(self.job_config, self.scrubber).run()

        self.assertEqual(self.format.write_records.call_count, 1)
        batch = self.format.write_records.call_args[0][0]
        self.assertEqual(len(batch.rows), 2)

    async def test_bedrock_tool_bad_stop_reason(self):
        self.prep_docs()
        self.mock_bedrock()
        self.mock_response(finish_reason="blarg")
        self.mock_response()

        await self.assert_failed_doc(
            "NLP failed for DocumentReference/1: did not complete, with stop reason: blarg"
        )

    async def test_bedrock_tool_no_content(self):
        self.prep_docs()
        self.mock_bedrock()
        self.add_response(
            {
                "stopReason": "tool_use",
                "output": {"message": {"content": [{"bogus": "content"}]}},
            }
        )
        self.mock_response()

        await self.assert_failed_doc(
            "NLP failed for DocumentReference/1: no response content found"
        )

    @ddt.data(
        ("local", nlp.ClaudeSonnet45Model),
        ("azure", nlp.ClaudeSonnet45Model),
        ("bedrock", nlp.Gpt5Model),
    )
    @ddt.unpack
    async def test_wrong_tool_for_model(self, provider, model):
        with self.assert_fatal_exit(errors.ARGS_INVALID):
            self.mock_provider(provider)
            model()


@ddt.ddt
class TestAzureNLPTasks(NlpModelTestCase):
    """Tests the Azure specific code"""

    @ddt.data(
        # env vars to set, success
        (["AZURE_OPENAI_API_KEY", "AZURE_OPENAI_ENDPOINT"], True),
        (["AZURE_OPENAI_API_KEY"], False),
        (["AZURE_OPENAI_ENDPOINT"], False),
    )
    @ddt.unpack
    async def test_requires_env(self, names, success):
        self.mock_azure("gpt-35-turbo-0125")
        task = covid_symptom.CovidSymptomNlpResultsGpt35Task(self.job_config, self.scrubber)
        env = {name: "content" for name in names}
        self.patch_dict(os.environ, env, clear=True)
        if success:
            await task.init_check()
        else:
            with self.assertRaises(SystemExit):
                await task.init_check()
