"""Tests for etl/studies/irae/"""

import json

import ddt

from cumulus_etl.etl.studies.irae.irae_tasks import (
    ImmunosuppressiveMedicationsAnnotation,
    KidneyTransplantDonorGroupAnnotation,
    KidneyTransplantLongitudinalAnnotation,
    MultipleTransplantHistoryAnnotation,
)
from cumulus_etl.nlp.models import OpenAIProvider
from tests.etl import BaseEtlSimple
from tests.nlp.utils import NlpModelTestCase


@ddt.ddt
class TestIraeTask(NlpModelTestCase, BaseEtlSimple):
    """Test case for Irae tasks"""

    DATA_ROOT = "irae"

    @staticmethod
    def longitudinal_content(**kwargs):
        content = {
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
        content.update(kwargs)
        return KidneyTransplantLongitudinalAnnotation.model_validate(content)

    @ddt.data(
        ("gpt_oss_120b", "gpt-oss-120b"),
        ("gpt4o", "gpt-4o"),
        ("gpt5", "gpt-5"),
        ("llama4_scout", "Llama-4-Scout-17B-16E-Instruct"),
    )
    @ddt.unpack
    async def test_basic_immunosuppressive_medications_etl(self, model_slug, model_id):
        self.mock_azure(model_id)

        self.mock_response(
            content=ImmunosuppressiveMedicationsAnnotation.model_validate(
                {
                    "immunosuppressive_medication_mentions": [
                        {
                            "has_mention": False,
                            "spans": ["note"],
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
                        }
                    ]
                }
            )
        )

        immunosuppressive_medications_task_name = (
            f"irae__nlp_immunosuppressive_medications_{model_slug}"
        )
        await self.run_etl(
            "--provider=azure",
            tasks=[
                immunosuppressive_medications_task_name,
            ],
        )
        self.assert_files_equal(
            f"{self.root_path}/immunosuppressive-medications-output.ndjson",
            f"{self.output_path}/{immunosuppressive_medications_task_name}/{immunosuppressive_medications_task_name}.000.ndjson",
        )

        self.assertEqual(self.mock_create.call_count, 1)
        self.assertEqual(
            {
                "messages": [
                    {
                        "role": "system",
                        "content": "You are a clinical chart reviewer for a kidney transplant outcomes study.\n"
                        "Your task is to extract patient-specific information from an unstructured clinical "
                        "document and map it into a predefined Pydantic schema.\n"
                        "\n"
                        "Core Rules:\n"
                        "1. Base all assertions ONLY on patient-specific information in the clinical document.\n"
                        "   - Never negate or exclude information just because it is not mentioned.\n"
                        "   - Never conflate family history or population-level risk with patient findings.\n"
                        "   - Do not count past medical history, prior episodes, or family history.\n"
                        "2. Do not invent or infer facts beyond what is documented.\n"
                        "3. Maintain high fidelity to the clinical document language when citing spans.\n"
                        "4. Answer patient outcomes with strongest available documented evidence:\n"
                        "    BIOPSY_PROVEN > CONFIRMED > SUSPECTED > NONE_OF_THE_ABOVE.\n"
                        "5. Always produce structured JSON that conforms to the Pydantic schema provided below.\n"
                        "\n"
                        "Pydantic Schema:\n"
                        + json.dumps(ImmunosuppressiveMedicationsAnnotation.model_json_schema()),
                    },
                    {
                        "role": "user",
                        "content": "Evaluate the following clinical document for kidney "
                        "transplant variables and outcomes.\n"
                        "Here is the clinical document for you to analyze:\n\n"
                        "Test note 2 with Past surgical history: Two failed renal transplants",
                    },
                ],
                "model": model_id,
                "seed": 12345,
                "temperature": 0,
                "timeout": 120,
                "response_format": OpenAIProvider.pydantic_to_response_format(
                    ImmunosuppressiveMedicationsAnnotation
                ),
            },
            self.mock_create.call_args_list[0][1],
        )

    @ddt.data(
        ("gpt_oss_120b", "gpt-oss-120b"),
        ("gpt4o", "gpt-4o"),
        ("gpt5", "gpt-5"),
        ("llama4_scout", "Llama-4-Scout-17B-16E-Instruct"),
    )
    @ddt.unpack
    async def test_basic_multiple_transplant_history_etl(self, model_slug, model_id):
        self.mock_azure(model_id)

        self.mock_response(
            content=MultipleTransplantHistoryAnnotation.model_validate(
                {
                    "multiple_transplant_history_mention": {
                        "multiple_transplant_history": True,
                        "has_mention": True,
                        "spans": ["Past surgical history: Two failed renal transplants"],
                    },
                }
            )
        )

        multiple_transplant_history_task_name = (
            f"irae__nlp_multiple_transplant_history_{model_slug}"
        )
        await self.run_etl(
            "--provider=azure",
            tasks=[
                multiple_transplant_history_task_name,
            ],
        )
        self.assert_files_equal(
            f"{self.root_path}/multiple-transplant-history-output.ndjson",
            f"{self.output_path}/{multiple_transplant_history_task_name}/{multiple_transplant_history_task_name}.000.ndjson",
        )

        self.assertEqual(self.mock_create.call_count, 1)
        self.assertEqual(
            {
                "messages": [
                    {
                        "role": "system",
                        "content": "You are a clinical chart reviewer for a kidney transplant outcomes study.\n"
                        "Your task is to extract patient-specific information from an unstructured clinical "
                        "document and map it into a predefined Pydantic schema.\n"
                        "\n"
                        "Core Rules:\n"
                        "1. Base all assertions ONLY on patient-specific information in the clinical document.\n"
                        "   - Never negate or exclude information just because it is not mentioned.\n"
                        "   - Never conflate family history or population-level risk with patient findings.\n"
                        "   - Do not count past medical history, prior episodes, or family history.\n"
                        "2. Do not invent or infer facts beyond what is documented.\n"
                        "3. Maintain high fidelity to the clinical document language when citing spans.\n"
                        "4. Answer patient outcomes with strongest available documented evidence:\n"
                        "    BIOPSY_PROVEN > CONFIRMED > SUSPECTED > NONE_OF_THE_ABOVE.\n"
                        "5. Always produce structured JSON that conforms to the Pydantic schema provided below.\n"
                        "\n"
                        "Pydantic Schema:\n"
                        + json.dumps(MultipleTransplantHistoryAnnotation.model_json_schema()),
                    },
                    {
                        "role": "user",
                        "content": "Evaluate the following clinical document for kidney "
                        "transplant variables and outcomes.\n"
                        "Here is the clinical document for you to analyze:\n\n"
                        "Test note 2 with Past surgical history: Two failed renal transplants",
                    },
                ],
                "model": model_id,
                "seed": 12345,
                "temperature": 0,
                "timeout": 120,
                "response_format": OpenAIProvider.pydantic_to_response_format(
                    MultipleTransplantHistoryAnnotation
                ),
            },
            self.mock_create.call_args_list[0][1],
        )

    @ddt.data(
        ("gpt_oss_120b", "gpt-oss-120b"),
        ("gpt4o", "gpt-4o"),
        ("gpt5", "gpt-5"),
        ("llama4_scout", "Llama-4-Scout-17B-16E-Instruct"),
    )
    @ddt.unpack
    async def test_basic_donor_etl(self, model_slug, model_id):
        self.mock_azure(model_id)
        self.mock_response(
            content=KidneyTransplantDonorGroupAnnotation.model_validate(
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
        )

        donor_task_name = f"irae__nlp_donor_{model_slug}"

        await self.run_etl(
            "--provider=azure",
            tasks=[
                donor_task_name,
            ],
        )

        self.assert_files_equal(
            f"{self.root_path}/donor-output.ndjson",
            f"{self.output_path}/{donor_task_name}/{donor_task_name}.000.ndjson",
        )

        self.assertEqual(self.mock_create.call_count, 1)
        self.assertEqual(
            {
                "messages": [
                    {
                        "role": "system",
                        "content": "You are a clinical chart reviewer for a kidney transplant outcomes study.\n"
                        "Your task is to extract patient-specific information from an unstructured clinical "
                        "document and map it into a predefined Pydantic schema.\n"
                        "\n"
                        "Core Rules:\n"
                        "1. Base all assertions ONLY on patient-specific information in the clinical document.\n"
                        "   - Never negate or exclude information just because it is not mentioned.\n"
                        "   - Never conflate family history or population-level risk with patient findings.\n"
                        "   - Do not count past medical history, prior episodes, or family history.\n"
                        "2. Do not invent or infer facts beyond what is documented.\n"
                        "3. Maintain high fidelity to the clinical document language when citing spans.\n"
                        "4. Answer patient outcomes with strongest available documented evidence:\n"
                        "    BIOPSY_PROVEN > CONFIRMED > SUSPECTED > NONE_OF_THE_ABOVE.\n"
                        "5. Always produce structured JSON that conforms to the Pydantic schema provided below.\n"
                        "\n"
                        "Pydantic Schema:\n"
                        + json.dumps(KidneyTransplantDonorGroupAnnotation.model_json_schema()),
                    },
                    {
                        "role": "user",
                        "content": "Evaluate the following clinical document for kidney "
                        "transplant variables and outcomes.\n"
                        "Here is the clinical document for you to analyze:\n\n"
                        "Test note 2 with Past surgical history: Two failed renal transplants",
                    },
                ],
                "model": model_id,
                "seed": 12345,
                "temperature": 0,
                "timeout": 120,
                "response_format": OpenAIProvider.pydantic_to_response_format(
                    KidneyTransplantDonorGroupAnnotation
                ),
            },
            self.mock_create.call_args_list[0][1],
        )

    @ddt.data(
        ("gpt_oss_120b", "gpt-oss-120b"),
        ("gpt4o", "gpt-4o"),
        ("gpt5", "gpt-5"),
        ("llama4_scout", "Llama-4-Scout-17B-16E-Instruct"),
    )
    @ddt.unpack
    async def test_basic_etl(self, model_slug, model_id):
        self.mock_azure(model_id)
        self.mock_response(
            content=self.longitudinal_content(
                # Have a little real data, just to confirm it converts and gets to end
                deceased_mention={
                    "has_mention": True,
                    "spans": ["note"],
                    "deceased": True,
                    "deceased_date": "2025-10-10",
                },
            )
        )
        longitudinal_task_name = f"irae__nlp_{model_slug}"

        await self.run_etl(
            "--provider=azure",
            tasks=[
                longitudinal_task_name,
            ],
        )

        self.assert_files_equal(
            f"{self.root_path}/longitudinal-output.ndjson",
            f"{self.output_path}/{longitudinal_task_name}/{longitudinal_task_name}.000.ndjson",
        )

        self.assertEqual(self.mock_create.call_count, 1)
        self.assertEqual(
            {
                "messages": [
                    {
                        "role": "system",
                        "content": "You are a clinical chart reviewer for a kidney transplant outcomes study.\n"
                        "Your task is to extract patient-specific information from an unstructured clinical "
                        "document and map it into a predefined Pydantic schema.\n"
                        "\n"
                        "Core Rules:\n"
                        "1. Base all assertions ONLY on patient-specific information in the clinical document.\n"
                        "   - Never negate or exclude information just because it is not mentioned.\n"
                        "   - Never conflate family history or population-level risk with patient findings.\n"
                        "   - Do not count past medical history, prior episodes, or family history.\n"
                        "2. Do not invent or infer facts beyond what is documented.\n"
                        "3. Maintain high fidelity to the clinical document language when citing spans.\n"
                        "4. Answer patient outcomes with strongest available documented evidence:\n"
                        "    BIOPSY_PROVEN > CONFIRMED > SUSPECTED > NONE_OF_THE_ABOVE.\n"
                        "5. Always produce structured JSON that conforms to the Pydantic schema provided below.\n"
                        "\n"
                        "Pydantic Schema:\n"
                        + json.dumps(KidneyTransplantLongitudinalAnnotation.model_json_schema()),
                    },
                    {
                        "role": "user",
                        "content": "Evaluate the following clinical document for kidney "
                        "transplant variables and outcomes.\n"
                        "Here is the clinical document for you to analyze:\n\n"
                        "Test note 2 with Past surgical history: Two failed renal transplants",
                    },
                ],
                "model": model_id,
                "seed": 12345,
                "temperature": 0,
                "timeout": 120,
                "response_format": OpenAIProvider.pydantic_to_response_format(
                    KidneyTransplantLongitudinalAnnotation
                ),
            },
            self.mock_create.call_args_list[0][1],
        )
