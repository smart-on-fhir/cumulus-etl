"""Tests for etl/studies/glioma/"""

import json

import ddt

from cumulus_etl.etl.studies.glioma.glioma_tasks import GliomaCaseAnnotation
from cumulus_etl.nlp.models import OpenAIProvider
from tests.etl import BaseEtlSimple
from tests.nlp.utils import NlpModelTestCase


@ddt.ddt
class TestGliomaTask(NlpModelTestCase, BaseEtlSimple):
    """Test case for Glioma tasks"""

    DATA_ROOT = "glioma"

    @staticmethod
    def glioma_case_annotation(**kwargs):
        content = {
            "topography_mention": {"has_mention": False, "spans": []},
            "morphology_mention": {"has_mention": False, "spans": []},
            "behavior_mention": {"has_mention": False, "spans": []},
            "grade_mention": {"has_mention": False, "spans": []},
            "target_genetic_test_mention": [],
            "variant_mention": [],
            "cancer_medication_mention": [],
            "surgery_mention": [],
        }
        content.update(kwargs)
        return GliomaCaseAnnotation.model_validate(content)

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
            content=self.glioma_case_annotation(
                # Have a little real data, just to confirm it converts and gets to end
                grade_mention={
                    "has_mention": True,
                    "spans": ["Grade II"],
                    "code": "2",
                    "display": "Grade II",
                },
            )
        )
        glioma_task_name = f"glioma__nlp_{model_slug}"

        await self.run_etl(
            "--provider=azure",
            tasks=[
                glioma_task_name,
            ],
        )

        self.assert_files_equal(
            f"{self.root_path}/glioma-output.ndjson",
            f"{self.output_path}/{glioma_task_name}/{glioma_task_name}.000.ndjson",
        )

        self.assertEqual(self.mock_create.call_count, 1)
        self.assertEqual(
            {
                "messages": [
                    {
                        "role": "system",
                        "content": "You are a clinical chart reviewer for a study examining the efficacy of various "
                        "treatments for pediatric low-grade glioma (pLGG) across various pathological/genetic "
                        "subtypes.\n"
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
                        "5. Always produce structured JSON that conforms to the Pydantic schema provided below.\n"
                        "\n"
                        "Pydantic Schema:\n" + json.dumps(GliomaCaseAnnotation.model_json_schema()),
                    },
                    {
                        "role": "user",
                        "content": "Evaluate the following clinical document for glioma variables and outcomes.\n"
                        "Here is the clinical document for you to analyze:\n\n"
                        "Test glioma note with mention of Grade II cancer",
                    },
                ],
                "model": model_id,
                "seed": 12345,
                "temperature": 0,
                "timeout": 120,
                "response_format": OpenAIProvider.pydantic_to_response_format(GliomaCaseAnnotation),
            },
            self.mock_create.call_args_list[0][1],
        )
