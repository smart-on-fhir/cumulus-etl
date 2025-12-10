from cumulus_etl import errors
from cumulus_etl.etl.studies.example.example_tasks import AgeMention
from cumulus_etl.etl.studies.irae.irae_tasks import KidneyTransplantDonorGroupAnnotation
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

        irae = KidneyTransplantDonorGroupAnnotation.model_validate(
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

        self.mock_response(content=AgeMention(has_mention=True, age=10))
        self.mock_response(content=AgeMention(has_mention=True, age=10))
        self.mock_response(content=irae)
        self.mock_response(content=irae)

        # But same model type does work
        await self.run_etl(tasks=["example_nlp__nlp_gpt_oss_120b", "irae__nlp_donor_gpt_oss_120b"])
