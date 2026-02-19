import json

from cumulus_etl import common, errors
from tests.etl import BaseEtlSimple
from tests.nlp.utils import NlpModelTestCase


class TestNlpCli(NlpModelTestCase, BaseEtlSimple):
    async def test_multiple_tasks(self):
        self.mock_azure("gpt-4o")

        # Multiple NLP models will fail
        with self.assert_fatal_exit(errors.TASK_TOO_MANY):
            await self.run_etl(
                tasks=["example_nlp__nlp_gpt_oss_120b", "example_nlp__nlp_llama4_scout"]
            )

        donor_model = self.load_pydantic_model("irae/donor.json")
        irae = donor_model.model_validate(
            {
                "donor_transplant_date_mention": {"has_mention": False, "spans": []},
                "donor_type_mention": {"has_mention": False, "spans": []},
                "donor_relationship_mention": {"has_mention": False, "spans": []},
                "donor_hla_match_quality_mention": {"has_mention": False, "spans": []},
                "donor_hla_mismatch_count_mention": {"has_mention": False, "spans": []},
                "donor_serostatus_mention": {"has_mention": False, "spans": []},
                "donor_serostatus_cmv_mention": {"has_mention": False, "spans": []},
                "donor_serostatus_ebv_mention": {"has_mention": False, "spans": []},
                "recipient_serostatus_mention": {"has_mention": False, "spans": []},
                "recipient_serostatus_cmv_mention": {"has_mention": False, "spans": []},
                "recipient_serostatus_ebv_mention": {"has_mention": False, "spans": []},
            }
        )

        age_model = self.load_pydantic_model("example/age.json")
        self.mock_response(content=age_model(has_mention=True, age=10))
        self.mock_response(content=age_model(has_mention=True, age=10))
        self.mock_response(content=irae)
        self.mock_response(content=irae)

        # But same model type does work
        await self.run_etl(tasks=["example_nlp__nlp_gpt_oss_120b", "irae__nlp_donor_gpt_oss_120b"])

    async def test_non_text_docrefs_are_ignored(self):
        self.mock_azure("gpt-4o")

        age_model = self.load_pydantic_model("example/age.json")
        self.mock_response(content=age_model(has_mention=True, age=10))

        valid = {
            "resourceType": "DocumentReference",
            "id": "valid",
            "subject": {"reference": "Patient/1"},
            "context": {"encounter": [{"reference": "Encounter/1"}]},
            "content": [
                {
                    "attachment": {
                        "contentType": "text/plain",
                        "data": "aGVsbG8=",
                    },
                }
            ],
        }
        ignored = {
            "resourceType": "DocumentReference",
            "id": "ignored",
        }

        tmpdir = self.make_tempdir()
        with common.NdjsonWriter(f"{tmpdir}/notes.ndjson") as writer:
            writer.write(valid)
            writer.write(ignored)

        task = "example_nlp__nlp_gpt_oss_120b"
        with self.assert_fatal_exit(errors.TASK_FAILED):
            await self.run_etl(input_path=tmpdir, tasks=[task], errors_to=f"{tmpdir}/errors")

        with open(f"{tmpdir}/errors/{task}/nlp-errors.ndjson", "rb") as f:
            self.assertEqual(ignored, json.load(f))
        with open(f"{self.output_path}/{task}/{task}.000.ndjson", "rb") as f:
            self.assertEqual(
                json.load(f)["note_ref"],
                "DocumentReference/5feee0d19f20824ac33967633125a76e4f916a7888c60bf777a0eec5a1555114",
            )
