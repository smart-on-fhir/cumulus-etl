"""Tests for etl/studies/irae/"""

from cumulus_etl.etl.studies.irae.irae_tasks import DSAMention, DSAPresent
from tests.etl import BaseEtlSimple
from tests.nlp.utils import OpenAITestCase


class TestIraeTask(OpenAITestCase, BaseEtlSimple):
    """Test case for Irae tasks"""

    DATA_ROOT = "irae"

    async def test_basic_etl(self):
        self.mock_response(
            content=DSAMention(
                spans=["note"],
                dsa_mentioned=True,
                dsa_history=False,
                dsa_present=DSAPresent("DSA Treatment prescribed or DSA treatment administered"),
            )
        )
        await self.run_etl(tasks=["irae__nlp_llama2"])
        self.assert_output_equal()

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
                "model": "meta-llama/Llama-2-13b-chat-hf",
                "seed": 12345,
                "temperature": 0,
                "timeout": 60,
                "response_format": DSAMention,
            },
            self.mock_create.call_args_list[0][1],
        )
