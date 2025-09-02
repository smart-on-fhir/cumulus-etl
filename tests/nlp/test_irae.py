"""Tests for etl/studies/irae/"""

import json

import ddt

from cumulus_etl.etl.studies.irae.irae_tasks import DSAMention, DSAPresent
from tests.etl import BaseEtlSimple
from tests.nlp.utils import OpenAITestCase


@ddt.ddt
class TestIraeTask(OpenAITestCase, BaseEtlSimple):
    """Test case for Irae tasks"""

    DATA_ROOT = "irae"

    @ddt.data(
        ("irae__nlp_gpt_oss_120b", "gpt-oss-120b"),
        ("irae__nlp_gpt4o", "gpt-4o"),
        ("irae__nlp_gpt5", "gpt-5"),
        ("irae__nlp_llama4_scout", "Llama-4-Scout-17B-16E-Instruct"),
    )
    @ddt.unpack
    async def test_basic_etl(self, task_name, model_id):
        self.mock_azure()
        self.mock_response(
            content=DSAMention(
                spans=["note"],
                dsa_mentioned=True,
                dsa_history=False,
                dsa_present=DSAPresent("DSA Treatment prescribed or DSA treatment administered"),
            )
        )

        await self.run_etl(tasks=[task_name])

        self.assert_files_equal(
            f"{self.root_path}/output.ndjson",
            f"{self.output_path}/{task_name}/{task_name}.000.ndjson",
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
                        "2. Do not invent or infer facts beyond what is documented.\n"
                        "3. Maintain high fidelity to the clinical document language when citing spans.\n"
                        "4. Always produce structured JSON that conforms to the Pydantic schema provided below.\n"
                        "\n"
                        "Pydantic Schema:\n" + json.dumps(DSAMention.model_json_schema()),
                    },
                    {
                        "role": "user",
                        "content": "Evaluate the following clinical document for kidney "
                        "transplant variables and outcomes.\n"
                        "Here is the clinical document for you to analyze:\n\n"
                        "Test note 1",
                    },
                ],
                "model": model_id,
                "seed": 12345,
                "temperature": 0,
                "timeout": 120,
                "response_format": DSAMention,
            },
            self.mock_create.call_args_list[0][1],
        )
