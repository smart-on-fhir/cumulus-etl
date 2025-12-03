"""Tests for etl/studies/irae/"""

import base64
import json

import ddt

from cumulus_etl.etl.studies.irae.irae_tasks import (
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
        content.update(kwargs)
        return KidneyTransplantLongitudinalAnnotation.model_validate(content)

    @ddt.data(
        ("gpt_oss_120b", "gpt-oss-120b"),
        ("gpt4o", "gpt-4o"),
        ("gpt5", "gpt-5"),
        ("llama4_scout", "Llama-4-Scout-17B-16E-Instruct"),
    )
    @ddt.unpack
    async def test_basic_etl(self, model_slug, model_id):
        self.mock_azure(model_id)
        # NOTE: Mock order needs to match the execution order, which
        #       funnily enough is not the order they're named in but is their
        #       alphabetical order by Task Name
        self.mock_response(
            content=KidneyTransplantDonorGroupAnnotation.model_validate(
                {
                    "donor_transplant_date_mention": {"has_mention": False, "spans": []},
                    "donor_type_mention": {"has_mention": False, "spans": []},
                    "donor_relationship_mention": {"has_mention": False, "spans": []},
                    "donor_hla_match_quality_mention": {"has_mention": False, "spans": []},
                    "donor_hla_mismatch_count_mention": {"has_mention": False, "spans": []},
                }
            )
        )
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
        donor_task_name = f"irae__nlp_donor_{model_slug}"
        longitudinal_task_name = f"irae__nlp_{model_slug}"

        await self.run_etl(
            "--provider=azure",
            tasks=[multiple_transplant_history_task_name, donor_task_name, longitudinal_task_name],
        )

        self.assert_files_equal(
            f"{self.root_path}/donor-output.ndjson",
            f"{self.output_path}/{donor_task_name}/{donor_task_name}.000.ndjson",
        )
        self.assert_files_equal(
            f"{self.root_path}/longitudinal-output.ndjson",
            f"{self.output_path}/{longitudinal_task_name}/{longitudinal_task_name}.000.ndjson",
        )
        self.assert_files_equal(
            f"{self.root_path}/multiple-transplant-history-output.ndjson",
            f"{self.output_path}/{multiple_transplant_history_task_name}/{multiple_transplant_history_task_name}.000.ndjson",
        )

        self.assertEqual(self.mock_create.call_count, 3)

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
            self.mock_create.call_args_list[1][1],
        )
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
            self.mock_create.call_args_list[2][1],
        )

    async def test_ordered_by_date(self):
        self.mock_azure("gpt-oss-120b")

        self.input_dir = self.make_tempdir()

        def prep_doc(year: str, add_date: bool = True) -> None:
            text = f"note {year[:4]}"
            kwargs = {
                "subject": {"reference": "Patient/x"},
                "context": {"encounter": [{"reference": "Encounter/x"}]},
                "content": [
                    {
                        "attachment": {
                            "contentType": "text/plain",
                            "data": base64.standard_b64encode(text.encode()).decode(),
                        },
                    },
                ],
            }
            if add_date:
                kwargs["date"] = year
            self.make_json("DocumentReference", year, **kwargs)
            self.mock_response(content=self.longitudinal_content())

        prep_doc("2022")
        prep_doc("2021")
        prep_doc("2024")
        prep_doc("null", add_date=False)
        prep_doc("2023-01-01T00:00:00+04:00")

        await self.run_etl(
            "--provider=azure", tasks=["irae__nlp_gpt_oss_120b"], input_path=self.input_dir
        )

        last_words = [
            x[1]["messages"][1]["content"].split()[-1] for x in self.mock_create.call_args_list
        ]
        self.assertEqual(last_words, ["2021", "2022", "2023", "2024", "null"])
