"""
Tests for nlp/models.py and etl/tasks/nlp_tasks.py.

Uses some studies as sample tasks, to exercise the full pipeline, but does not do full testing
of those studies - that code is elsewhere in study-specific test files.
"""

import filecmp
import hashlib
import json
import os
from types import SimpleNamespace
from unittest import mock

import ddt
import httpx
import openai
import pyarrow
import pydantic

from cumulus_etl import common, errors, nlp
from cumulus_etl.etl.studies import covid_symptom, irae
from cumulus_etl.etl.studies.example.example_tasks import AgeMention
from cumulus_etl.etl.studies.irae.irae_tasks import (
    ImmunosuppressiveMedicationsAnnotation,
    KidneyTransplantLongitudinalAnnotation,
)
from cumulus_etl.nlp.models import OpenAIProvider
from tests import i2b2_mock_data
from tests.etl import BaseEtlSimple
from tests.nlp.utils import NlpModelTestCase


@ddt.ddt
class TestWithSpansNLPTasks(NlpModelTestCase):
    """Tests local NLP with spans and similar shared/generic code"""

    MODEL_ID = "openai/gpt-oss-120b"

    @staticmethod
    def default_kidney(**kwargs) -> KidneyTransplantLongitudinalAnnotation:
        model_dict = {
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
        return KidneyTransplantLongitudinalAnnotation.model_validate(model_dict)

    def default_content(self) -> pydantic.BaseModel:
        return self.default_kidney()

    def prep_docs(self, docref: dict | None = None):
        """Create two docs for input"""
        docref = docref or i2b2_mock_data.documentreference("foo")
        self.make_json("DocumentReference", "1", **docref)
        self.make_json("DocumentReference", "2", **i2b2_mock_data.documentreference("bar"))

    async def assert_failed_doc(self, msg: str):
        task = irae.IraeLongitudinalGptOss120bTask(self.job_config, self.scrubber)
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
        await irae.IraeLongitudinalGptOss120bTask(self.job_config, self.scrubber).run()

        self.assertEqual(self.mock_create.call_count, 1)
        # NOTE: `vX` here needs to be updated as BaseLongitudinalIraeTask task_version changes
        cache_dir = f"{self.phi_dir}/nlp-cache/irae__nlp_gpt_oss_120b_v6/06ee"
        cache_file = f"{cache_dir}/sha256-06ee538c626fbf4bdcec2199b7225c8034f26e2b46a7b5cb7ab385c8e8c00efa.cache"
        self.assertEqual(
            common.read_json(cache_file),
            {
                "answer": self.default_content().model_dump(mode="json", exclude_unset=True),
                "fingerprint": "test-fp",
            },
        )

        await irae.IraeLongitudinalGptOss120bTask(self.job_config, self.scrubber).run()
        self.assertEqual(self.mock_create.call_count, 1)

        # Confirm that if we remove the cache file, we call the endpoint again
        self.mock_response()
        os.remove(cache_file)
        await irae.IraeLongitudinalGptOss120bTask(self.job_config, self.scrubber).run()
        self.assertEqual(self.mock_create.call_count, 2)

    async def test_init_check_unreachable(self):
        """Verify we bail if the server isn't reachable"""
        self.mock_client.models.list = self.mock_model_list(error=True)

        with self.assertRaises(SystemExit) as cm:
            await irae.IraeLongitudinalGptOss120bTask.init_check()
        self.assertEqual(errors.SERVICE_MISSING, cm.exception.code)

    async def test_init_check_config(self):
        """Verify we check the server properties"""
        # Happy path
        await irae.IraeLongitudinalGptOss120bTask.init_check()

        # Random error bubbles up
        self.mock_client.models.list = mock.MagicMock(side_effect=SystemExit)
        with self.assertRaises(SystemExit):
            await irae.IraeLongitudinalGptOss120bTask.init_check()

        # Bad model ID
        self.mock_client.models.list = self.mock_model_list("bogus-model")
        with self.assert_fatal_exit(errors.SERVICE_MISSING):
            await irae.IraeLongitudinalGptOss120bTask.init_check()

    async def test_output_fields(self):
        self.make_json("DocumentReference", "1", **i2b2_mock_data.documentreference("foo"))
        self.mock_response()
        await irae.IraeLongitudinalGptOss120bTask(self.job_config, self.scrubber).run()

        self.assertEqual(self.format.write_records.call_count, 1)
        batch = self.format.write_records.call_args[0][0]
        self.assertEqual(len(batch.rows), 1)
        del batch.rows[0]["result"]  # don't bother testing the NLP serialization, that's elsewhere
        # NOTE: task_version in the assertion below needs updating as the Longitudinal task changes
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
                "task_version": 6,
            },
        )

    async def test_trailing_whitespace_removed(self):
        self.make_json(
            "DocumentReference",
            "1",
            **i2b2_mock_data.documentreference("Test   \n  lines  "),
        )
        self.mock_response()
        await irae.IraeLongitudinalGptOss120bTask(self.job_config, self.scrubber).run()

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
        await irae.IraeLongitudinalGptOss120bTask(self.job_config, self.scrubber).run()

        self.assertEqual(self.format.write_records.call_count, 1)
        batch = self.format.write_records.call_args[0][0]
        self.assertEqual(len(batch.rows), 1)
        self.assertEqual(
            batch.rows[0]["result"]["dsa_mention"]["spans"], [(0, 4), (0, 11), (14, 19)]
        )

    async def test_practitioner_specialty(self):
        note = i2b2_mock_data.documentreference()
        note["author"] = [{"reference": "PractitionerRole/1"}]
        self.make_json("DocumentReference", "1", **note)
        self.make_json(
            "PractitionerRole",
            "1",
            specialty=[{"coding": [{"display": "Nurse"}]}],
        )

        self.mock_azure("gpt-4o")
        self.mock_response()
        await irae.IraeLongitudinalGpt4oTask(self.job_config, self.scrubber).run()

        self.assertEqual(self.mock_create.call_count, 1)
        self.assertEqual(
            "Evaluate the following clinical document for kidney "
            "transplant variables and outcomes.\n"
            "Here is the clinical document for you to analyze:\n\n"
            "Chief complaint: fever and chills. Denies cough.",
            self.mock_create.call_args_list[0][1]["messages"][1]["content"],
        )

    async def test_span_conversion_after_failed_span_match(self):
        # Regression test for span matching continuing after a failed match for list-based
        # annotation class
        self.make_json(
            "DocumentReference",
            "1",
            **i2b2_mock_data.documentreference("Test   NOTE. !Hello!"),
        )
        # Response to mock is a list-based annotation class with one span that won't match
        # followed by others that will match
        self.mock_response(
            content=ImmunosuppressiveMedicationsAnnotation.model_validate(
                {
                    "immunosuppressive_medication_mentions": [
                        {
                            "has_mention": True,
                            "spans": ["Test"],
                            "status": "None of the above",
                            "category": "None of the above",
                            "route": "None of the above",
                            "phase": "None of the above",
                            "expected_supply_days": None,
                            "number_of_repeats_allowed": None,
                            "frequency": "None of the above",
                            "start_date": None,
                            "end_date": None,
                            "quantity_unit": "None of the above",
                            "quantity_value": None,
                            "drug_class": "None of the above",
                            "ingredient": "None of the above",
                        },
                        {
                            "has_mention": True,
                            "spans": ["WILL NOT MATCH"],
                            "status": "None of the above",
                            "category": "None of the above",
                            "route": "None of the above",
                            "phase": "None of the above",
                            "expected_supply_days": None,
                            "number_of_repeats_allowed": None,
                            "frequency": "None of the above",
                            "start_date": None,
                            "end_date": None,
                            "quantity_unit": "None of the above",
                            "quantity_value": None,
                            "drug_class": "None of the above",
                            "ingredient": "None of the above",
                        },
                        {
                            "has_mention": True,
                            "spans": ["TEST  \n Note."],
                            "status": "None of the above",
                            "category": "None of the above",
                            "route": "None of the above",
                            "phase": "None of the above",
                            "expected_supply_days": None,
                            "number_of_repeats_allowed": None,
                            "frequency": "None of the above",
                            "start_date": None,
                            "end_date": None,
                            "quantity_unit": "None of the above",
                            "quantity_value": None,
                            "drug_class": "None of the above",
                            "ingredient": "None of the above",
                        },
                    ]
                }
            )
        )
        await irae.IraeImmunosuppressiveMedicationsGptOss120bTask(
            self.job_config, self.scrubber
        ).run()

        self.assertEqual(self.format.write_records.call_count, 1)
        batch = self.format.write_records.call_args[0][0]
        self.assertEqual(len(batch.rows), 1)
        print(batch.rows)
        # First match as expected
        self.assertEqual(
            batch.rows[0]["result"]["immunosuppressive_medication_mentions"][0]["spans"], [(0, 4)]
        )
        # No Match
        self.assertEqual(
            batch.rows[0]["result"]["immunosuppressive_medication_mentions"][1]["spans"], []
        )
        # Still matches after failed one
        self.assertEqual(
            batch.rows[0]["result"]["immunosuppressive_medication_mentions"][2]["spans"], [(0, 11)]
        )

    async def test_span_conversion_in_schema(self):
        schema = irae.IraeLongitudinalGptOss120bTask.get_schema(None, [])
        result_index = schema.get_field_index("result")
        result_type = schema.field(result_index).type
        dsa_index = result_type.get_field_index("dsa_mention")  # spot check one of the structs
        dsa_type = result_type.field(dsa_index).type
        spans_index = dsa_type.get_field_index("spans")
        span_type = dsa_type.field(spans_index).type
        self.assertEqual(span_type, pyarrow.list_(pyarrow.list_(pyarrow.int32(), 2)))

        # Also test a slightly different task, with a list in the mix
        schema = irae.IraeImmunosuppressiveMedicationsGptOss120bTask.get_schema(None, [])
        result_index = schema.get_field_index("result")
        result_type = schema.field(result_index).type
        med_index = result_type.get_field_index("immunosuppressive_medication_mentions")
        med_type = result_type.field(med_index).type.value_type
        spans_index = med_type.get_field_index("spans")
        span_type = med_type.field(spans_index).type
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

        await irae.IraeLongitudinalClaudeSonnet45Task(self.job_config, self.scrubber).run()

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

        await irae.IraeLongitudinalGptOss120bTask(self.job_config, self.scrubber).run()

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

    @ddt.data("azure", "bedrock")
    async def test_usage_recorded(self, provider):
        self.prep_docs()
        if provider == "azure":
            self.mock_azure("gpt-oss-120b")
        else:
            self.mock_bedrock()
        self.mock_response(usage=(1, 2, 4, 8))
        self.mock_response(usage=(16, 32, 64, 128))

        task = irae.IraeLongitudinalGptOss120bTask(self.job_config, self.scrubber)
        await task.run()

        self.assertEqual(
            task.model.stats,
            nlp.TokenStats(
                new_input_tokens=17,
                cache_read_input_tokens=34,
                # Only bedrock uses cache writing (other providers do it transparently)
                cache_written_input_tokens=68 if provider == "bedrock" else 0,
                output_tokens=136,
            ),
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
class TestAzureNLPTasks(NlpModelTestCase, BaseEtlSimple):
    """Tests the Azure specific code"""

    @staticmethod
    def batch_line(contents: str) -> str:
        checksum = hashlib.sha256(contents.encode("utf8"), usedforsecurity=False).hexdigest()
        return json.dumps(
            {
                "custom_id": checksum,
                "response": {
                    "body": {
                        "id": f"blarg-{checksum}",
                        "choices": [
                            {
                                "index": 0,
                                "finish_reason": "stop",
                                "message": {
                                    "role": "assistant",
                                    "content": json.dumps({"has_mention": True, "age": 10}),
                                },
                            }
                        ],
                        "created": 1000000,
                        "model": "gpt-4o",
                        "object": "chat.completion",
                    },
                },
            },
        )

    def mock_content(self, contents: list | None = None) -> None:
        if contents is None:
            contents = [
                self.batch_line("Notes for fever"),
                self.batch_line("Notes! for fever"),
            ]
        self.mock_client.files.content.return_value = openai.HttpxBinaryResponseContent(
            httpx.Response(status_code=200, text="\n".join(contents)),
        )

    def default_content(self) -> pydantic.BaseModel:
        return AgeMention(has_mention=True, age=10)

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

    async def test_custom_deployment(self):
        self.mock_azure("gpt-4o")

        self.mock_response()
        self.mock_response()

        await self.run_etl(
            "--azure-deployment=foo", "--provider=azure", tasks=["example_nlp__nlp_gpt4o"]
        )

        self.assertEqual(self.mock_create.call_args_list[0][1]["model"], "foo")

    async def test_batching_happy_path(self):
        self.mock_azure("gpt-4o")

        async def upload_file(**kwargs):
            self.assertEqual(kwargs["purpose"], "batch")
            file_text = common.read_text(str(kwargs["file"]))
            lines = [json.loads(line) for line in file_text.split("\n") if line]
            self.assertEqual(len(lines), 4)  # 2 docrefs, 2 dxreports
            self.assertEqual(
                lines[0]["custom_id"],
                "5db841c4c46d8a25fbb1891fd1eb352170278fa2b931c1c5edebe09a06582fb5",
            )
            self.assertEqual(lines[0]["method"], "POST")
            self.assertEqual(lines[0]["url"], "/v1/chat/completions")
            self.assertEqual(lines[0]["body"]["model"], "gpt-4o")
            self.assertTrue(lines[0]["body"]["messages"][0]["content"].startswith("You are a"))
            return SimpleNamespace(id="input")

        self.mock_client.files.create = upload_file
        self.mock_client.batches.create.return_value = SimpleNamespace(id="batch")
        self.mock_client.batches.retrieve.return_value = SimpleNamespace(
            id="batch", status="completed", error_file_id=None, output_file_id="output"
        )
        self.mock_content()

        await self.run_etl("--batch", "--provider=azure", tasks=["example_nlp__nlp_gpt4o"])

        self.assertEqual(
            self.mock_client.batches.create.call_args_list[0][1],
            {
                "completion_window": "24h",
                "endpoint": "/v1/chat/completions",
                "input_file_id": "input",
            },
        )
        self.assertEqual(
            self.mock_client.batches.retrieve.call_args_list[0][1],
            {
                "batch_id": "batch",
            },
        )

    async def test_model_does_not_support_batching(self):
        self.mock_azure("gpt-oss-120b")
        with self.assert_fatal_exit(errors.NLP_BATCHING_UNSUPPORTED):
            await self.run_etl(
                "--batch", "--provider=azure", tasks=["example_nlp__nlp_gpt_oss_120b"]
            )

    async def test_resume_batching(self):
        self.mock_azure("gpt-4o")

        path_dir = f"{self.phi_path}/nlp-cache/example_nlp__nlp_gpt4o_v1"
        os.makedirs(path_dir)
        with open(f"{path_dir}/metadata.json", "w", encoding="utf8") as f:
            json.dump({"batches-azure": ["b1", "b2"]}, f)

        # Just mock the retrieval bits, make the creation bits blow up
        self.mock_client.files.create.side_effect = RuntimeError
        self.mock_client.batches.retrieve.return_value = SimpleNamespace(
            id="batch", status="completed", error_file_id=None, output_file_id="output"
        )
        self.mock_content()

        await self.run_etl("--batch", "--provider=azure", tasks=["example_nlp__nlp_gpt4o"])

    async def test_errors(self):
        self.mock_azure("gpt-4o")

        self.mock_client.files.create.return_value = SimpleNamespace(id="input")
        self.mock_client.batches.create.return_value = SimpleNamespace(id="batch")
        self.mock_client.batches.retrieve.side_effect = [
            SimpleNamespace(id="batch", status="validating"),
            SimpleNamespace(id="batch", status="in_progress"),
            SimpleNamespace(id="batch", status="finalizing"),
            SimpleNamespace(
                # Will still process error/output files when failed, just prints a message
                id="batch",
                status="failed",
                error_file_id="error",
                output_file_id="output",
            ),
        ]
        self.mock_client.files.content.side_effect = [
            openai.HttpxBinaryResponseContent(  # error file
                httpx.Response(
                    status_code=200,
                    text="\n".join(
                        [
                            # Test all the various ways we can stuff errors in there
                            json.dumps({"error": {"message": {"error": {"message": "error1"}}}}),
                            "{'blarg'",  # invalid json
                        ],
                    ),
                ),
            ),
            openai.HttpxBinaryResponseContent(  # output file
                httpx.Response(
                    status_code=200,
                    text="\n".join(
                        [
                            # Test all the various ways we can stuff errors in there
                            json.dumps({"error": {"message": "error2"}}),
                            json.dumps({"response": {"status_code": 400}}),
                            json.dumps({"response": {"body": {"model": "gpt-4o"}}}),  # no custom_id
                            json.dumps({"custom_id": "xx", "response": {"id": "yy"}}),  # no body
                        ],
                    ),
                ),
            ),
        ]

        # The above gave no real responses, so we'll fall back to non-batching:
        self.mock_response()
        self.mock_response()

        with self.assertLogs(level="WARNING") as logs:
            await self.run_etl("--batch", "--provider=azure", tasks=["example_nlp__nlp_gpt4o"])

        self.assertEqual(
            logs.output,
            [
                "WARNING:root:Batch did not complete, got status: 'failed'",
                "WARNING:root:Error from NLP: error1",
                "WARNING:root:Could not process error message: '{'blarg''",
                "WARNING:root:Error from NLP: error2",
                "WARNING:root:Unexpected status code from NLP: 400",
                "WARNING:root:Unexpected response from NLP: missing data",  # no custom id
                "WARNING:root:Unexpected response from NLP: missing data",  # no body
            ],
        )

    @mock.patch.object(OpenAIProvider, "AZURE_MAX_BATCH_COUNT", 2)
    @mock.patch.object(OpenAIProvider, "AZURE_MAX_BATCH_BYTES", 6000)
    async def test_splitting_batch(self):
        self.mock_azure("gpt-4o")

        docref = i2b2_mock_data.documentreference("foo")
        self.make_json("DocumentReference", "1", **docref)
        self.make_json("DocumentReference", "2", **docref)
        # Now a break because of max count of 2 rows
        docref = i2b2_mock_data.documentreference("very long string that breaks size limit x 2")
        self.make_json("DocumentReference", "3", **docref)
        # Now a break because of max byte limit
        self.make_json("DocumentReference", "4", **docref)

        self.mock_client.files.create.side_effect = [
            SimpleNamespace(id="input1"),
            SimpleNamespace(id="input2"),
            SimpleNamespace(id="input3"),
        ]
        self.mock_client.batches.create.side_effect = [
            SimpleNamespace(id="batch1"),
            SimpleNamespace(id="batch2"),
            SimpleNamespace(id="batch3"),
        ]
        self.mock_client.batches.retrieve.side_effect = [
            SimpleNamespace(
                id="batch1", status="completed", error_file_id=None, output_file_id="output1"
            ),
            SimpleNamespace(
                id="batch2", status="completed", error_file_id=None, output_file_id="output2"
            ),
            SimpleNamespace(
                id="batch3", status="completed", error_file_id=None, output_file_id="output3"
            ),
        ]
        self.mock_client.files.content.side_effect = [
            openai.HttpxBinaryResponseContent(
                httpx.Response(
                    status_code=200,
                    text="\n".join([self.batch_line("foo"), self.batch_line("foo")]),
                ),
            ),
            openai.HttpxBinaryResponseContent(
                httpx.Response(
                    status_code=200,
                    text="\n".join(
                        [self.batch_line("very long string that breaks size limit x 2")]
                    ),
                ),
            ),
            openai.HttpxBinaryResponseContent(
                httpx.Response(
                    status_code=200,
                    text="\n".join(
                        [self.batch_line("very long string that breaks size limit x 2")]
                    ),
                ),
            ),
        ]

        await self.run_etl(
            "--batch",
            "--provider=azure",
            tasks=["example_nlp__nlp_gpt4o"],
            input_path=self.input_dir,
        )
