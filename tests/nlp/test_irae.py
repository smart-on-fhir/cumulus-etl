"""Tests for etl/studies/irae/"""

import ddt

from cumulus_etl.etl.studies.irae.irae_tasks import DSAMention, DSAPresent
from tests.etl import BaseEtlSimple
from tests.nlp.utils import OpenAITestCase


@ddt.ddt
class TestIraeTask(OpenAITestCase, BaseEtlSimple):
    """Test case for Irae tasks"""

    DATA_ROOT = "irae"

    @ddt.data(
        ("irae__nlp_gpt_oss_120b", "openai/gpt-oss-120b"),
        ("irae__nlp_gpt4o", "gpt-4o"),
        ("irae__nlp_gpt5", "gpt-5"),
        ("irae__nlp_llama4_scout", "nvidia/Llama-4-Scout-17B-16E-Instruct-FP8"),
    )
    @ddt.unpack
    async def test_basic_etl(self, task_name, model_id):
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
                        "content": "You are a helpful assistant reviewing kidney-transplant notes "
                        "for donor-specific antibody (DSA) information. Return *only* valid JSON.",
                    },
                    {
                        "role": "user",
                        "content": "Evaluate the following chart for donor-specific antibody (DSA) "
                        "information.\n"
                        "Here is the chart for you to analyze:\n"
                        "Test note 1\n"
                        "Keep the pydantic structure previously provided in mind when structuring "
                        "your output.\n"
                        "Finally, ensure that your final assertions are based on Patient-specific "
                        "information only.\n"
                        "For example, we should never deny the presence of an observation because "
                        "of a lack of family history.",
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
