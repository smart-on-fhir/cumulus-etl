"""Tests for etl/studies/example/"""

import ddt
import pydantic

from cumulus_etl.etl.studies.example.example_tasks import AgeMention
from tests.etl import BaseEtlSimple
from tests.nlp.utils import NlpModelTestCase


@ddt.ddt
class TestExampleTask(NlpModelTestCase, BaseEtlSimple):
    """Test case for example tasks"""

    def default_content(self) -> pydantic.BaseModel:
        return AgeMention(has_mention=True, spans=["year-old"], age=20)

    @ddt.data(
        ("example_nlp__nlp_gpt_oss_120b", "gpt-oss-120b"),
        ("example_nlp__nlp_gpt4", "gpt-4"),
        ("example_nlp__nlp_gpt4o", "gpt-4o"),
        ("example_nlp__nlp_gpt5", "gpt-5"),
        ("example_nlp__nlp_llama4_scout", "Llama-4-Scout-17B-16E-Instruct"),
    )
    @ddt.unpack
    async def test_basic_etl(self, task_name, model_id):
        self.mock_azure(model_id)
        for _ in range(8):
            self.mock_response()
        await self.run_etl("--provider=azure", tasks=[task_name], input_path="%EXAMPLE-NLP%")
        self.assertEqual(self.mock_create.call_count, 8)
