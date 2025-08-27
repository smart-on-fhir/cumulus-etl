"""Define tasks for the example/sample study"""

import json

import pydantic

from cumulus_etl import nlp
from cumulus_etl.etl import tasks


class AgeMention(pydantic.BaseModel):
    has_mention: bool | None = pydantic.Field(None)
    spans: list[str] = pydantic.Field(default_factory=list, description="Supporting text spans")
    age: int | None = pydantic.Field(None, description="The age of the patient")


class BaseExampleTask(tasks.BaseOpenAiTaskWithSpans):
    task_version = 0
    # Task Version History:
    # ** 0 (2025-08): Initial work, still in flux **

    system_prompt = (
        "You are a clinical chart reviewer.\n"
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
        "Pydantic Schema:\n" + json.dumps(AgeMention.model_json_schema())
    )
    response_format = AgeMention


# Have a task for every ETL-supported model, to allow sites to choose whatever model works for them.


class ExampleGpt4Task(BaseExampleTask):
    name = "example_nlp__nlp_gpt4"
    client_class = nlp.Gpt4Model


class ExampleGpt4oTask(BaseExampleTask):
    name = "example_nlp__nlp_gpt4o"
    client_class = nlp.Gpt4oModel


class ExampleGpt5Task(BaseExampleTask):
    name = "example_nlp__nlp_gpt5"
    client_class = nlp.Gpt5Model


class ExampleGptOss120bTask(BaseExampleTask):
    name = "example_nlp__nlp_gpt_oss_120b"
    client_class = nlp.GptOss120bModel


class ExampleLlama4ScoutTask(BaseExampleTask):
    name = "example_nlp__nlp_llama4_scout"
    client_class = nlp.Llama4ScoutModel
