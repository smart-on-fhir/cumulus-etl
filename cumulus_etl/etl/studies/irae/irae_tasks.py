"""Define tasks for the irae study"""

import enum
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
    tags: ClassVar = {"irae", "gpu"}

    task_version = 0
    # Task Version History:
    # ** 0 (2025-08): Initial work, still in flux **

    system_prompt = (
        "You are a helpful assistant reviewing kidney-transplant notes for donor-specific "
        "antibody (DSA) information. Return *only* valid JSON."
    )
    user_prompt = (
        "Evaluate the following chart for donor-specific antibody (DSA) information.\n"
        "Here is the chart for you to analyze:\n"
        "%CLINICAL-NOTE%\n"
        "Keep the pydantic structure previously provided in mind when structuring your output.\n"
        "Finally, ensure that your final assertions are based on Patient-specific information "
        "only.\n"
        "For example, we should never deny the presence of an observation because of a lack of "
        "family history."
    )
    response_format = DSAMention


class IraeLlama2Task(BaseIraeTask):
    name = "irae__nlp_llama2"
    client_class = nlp.Llama2Model
