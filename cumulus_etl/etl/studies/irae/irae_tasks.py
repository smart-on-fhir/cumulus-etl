"""Define tasks for the irae study"""

import enum
import json
from typing import ClassVar

import pydantic

from cumulus_etl import nlp
from cumulus_etl.etl import tasks


class DSAPresent(str, enum.Enum):
    Confirmed = (
        "DSA diagnostic test positive, DSA diagnosis 'confirmed' or 'positive', "
        "or increase in immunosuppression due to DSA"
    )
    Treatment = "DSA Treatment prescribed or DSA treatment administered"
    Suspected = (
        "DSA suspected, DSA likely, DSA cannot be ruled out, DSA test result "
        "pending or DSA is a differential diagnosis"
    )
    NoneOfTheAbove = "None of the above"


class DSAMention(pydantic.BaseModel):
    spans: list[str] = pydantic.Field(default_factory=list, description="Supporting text spans")
    dsa_mentioned: bool = pydantic.Field(
        False,
        description="Are donor specific antibodies (DSA) mentioned?",
    )
    dsa_history: bool = pydantic.Field(
        False,
        description="Has the patient ever had donor specific antibodies (DSA)?",
    )
    dsa_present: DSAPresent = pydantic.Field(
        DSAPresent.NoneOfTheAbove,
        description=(
            "Is there documented evidence of donor specific antibodies (DSA) "
            "in the present encounter?"
        ),
    )


class BaseIraeTask(tasks.BaseOpenAiTaskWithSpans):
    task_version = 1
    # Task Version History:
    # ** 1 (2025-08): Updated prompt **
    # ** 0 (2025-08): Initial version **

    system_prompt = (
        "You are a clinical chart reviewer for a kidney transplant outcomes study.\n"
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
        "Pydantic Schema:\n" + json.dumps(DSAMention.model_json_schema())
    )
    user_prompt = (
        "Evaluate the following clinical document for kidney transplant variables and outcomes.\n"
        "Here is the clinical document for you to analyze:\n"
        "\n"
        "%CLINICAL-NOTE%"
    )
    response_format = DSAMention


class IraeGpt4oTask(BaseIraeTask):
    name = "irae__nlp_gpt4o"
    client_class = nlp.Gpt4oModel
    tags: ClassVar = {"irae", "cpu"}


class IraeGpt5Task(BaseIraeTask):
    name = "irae__nlp_gpt5"
    client_class = nlp.Gpt5Model
    tags: ClassVar = {"irae", "cpu"}


class IraeGptOss120bTask(BaseIraeTask):
    name = "irae__nlp_gpt_oss_120b"
    client_class = nlp.GptOss120bModel
    tags: ClassVar = {"irae", "gpu"}


class IraeLlama4ScoutTask(BaseIraeTask):
    name = "irae__nlp_llama4_scout"
    client_class = nlp.Llama4ScoutModel
    tags: ClassVar = {"irae", "gpu"}
