"""Tests for etl/studies/glioma/"""

import itertools
import json
from collections.abc import Callable
from typing import Any, ClassVar

import ddt
import pydantic

from cumulus_etl.nlp.models import OpenAIProvider
from tests.etl import BaseEtlSimple
from tests.nlp.utils import NlpModelTestCase

ModelOption = tuple[
    str,  # Model Slug
    str,  # Model ID
]

TaskOption = tuple[
    # Annotation model of interest
    Callable[[Any], type[pydantic.BaseModel]],
    # Return the task_name given the model of interest
    Callable[[str], str],
    # The system prompt given a particular annotation class
    Callable[[Any], str],
    # Return the user prompt given some note text
    Callable[[Any, str], str],
    # Return an instance of the relevant annotation class given the Test class and the data
    Callable[[Any, dict], pydantic.BaseModel],
    # The annotation_data we want to use for this test
    dict,
    # The name of the comparator fixture
    str,
]


@ddt.ddt
class TestGliomaTasks(NlpModelTestCase, BaseEtlSimple):
    """Test case for Glioma tasks"""

    #######################################################
    # Static methods for generating various annotation classes and instances
    @classmethod
    def glioma_document_type_annotation_model(cls):
        return cls.load_pydantic_model("glioma/glioma-document-type-annotation.json")

    @classmethod
    def glioma_document_type_annotation(cls, **kwargs):
        content = {
            "glioma_doc_type_mention": [],
        }
        content.update(kwargs)
        return cls.glioma_document_type_annotation_model().model_validate(content)

    @classmethod
    def glioma_diagnosis_annotation_model(cls):
        return cls.load_pydantic_model("glioma/glioma-diagnosis-annotation.json")

    @classmethod
    def glioma_diagnosis_annotation(cls, **kwargs):
        content = {
            "age_at_diagnosis_mention": {"has_mention": False, "spans": []},
            "tumor_location_mention": {"has_mention": False, "spans": []},
            "tumor_region_mention": {"has_mention": False, "spans": []},
            "tumor_size_mention": {"has_mention": False, "spans": []},
            "topography_mention": {"has_mention": False, "spans": []},
            "morphology_mention": {"has_mention": False, "spans": []},
            "behavior_mention": {"has_mention": False, "spans": []},
            "grade_mention": {"has_mention": False, "spans": []},
            "nf1_status_mention": {"has_mention": False, "spans": []},
        }
        content.update(kwargs)
        return cls.glioma_diagnosis_annotation_model().model_validate(content)

    @classmethod
    def glioma_gene_annotation_model(cls):
        return cls.load_pydantic_model("glioma/glioma-gene-annotation.json")

    @classmethod
    def glioma_gene_annotation(cls, **kwargs):
        content = {
            "molecular_driver_mention": [],
            "genetic_variant_mention": [],
        }
        content.update(kwargs)
        return cls.glioma_gene_annotation_model().model_validate(content)

    @classmethod
    def glioma_medications_annotation_model(cls):
        return cls.load_pydantic_model("glioma/glioma-medications-annotation.json")

    @classmethod
    def glioma_medications_annotation(cls, **kwargs):
        content = {
            "chemotherapy_mention": [],
            "targeted_therapy_mention": [],
        }
        content.update(kwargs)
        return cls.glioma_medications_annotation_model().model_validate(content)

    @classmethod
    def glioma_progression_annotation_model(cls):
        return cls.load_pydantic_model("glioma/glioma-progression-annotation.json")

    @classmethod
    def glioma_progression_annotation(cls, **kwargs):
        content = {"glioma_progression_mention": []}
        content.update(kwargs)
        return cls.glioma_progression_annotation_model().model_validate(content)

    @classmethod
    def glioma_surgical_annotation_model(cls):
        return cls.load_pydantic_model("glioma/glioma-surgical-annotation.json")

    @classmethod
    def glioma_surgical_annotation(cls, **kwargs):
        content = {
            "surgical_type_mention": {"has_mention": False, "spans": []},
            "approach_mention": {"has_mention": False, "spans": []},
            "extent_of_resection_mention": {"has_mention": False, "spans": []},
        }
        content.update(kwargs)
        return cls.glioma_surgical_annotation_model().model_validate(content)

    # Path to relevant Tasks files
    SYSTEM_PROMPT = """You are a clinical chart reviewer for a study examining the efficacy of various \
treatments for pediatric low-grade glioma (pLGG) across various pathological/genetic \
subtypes.
Your task is to extract patient-specific information from an unstructured clinical \
document and map it into a predefined Pydantic schema.

Core Rules:
1. Base all assertions ONLY on patient-specific information in the clinical document.
   - Never negate or exclude information just because it is not mentioned.
   - Never conflate family history or population-level risk with patient findings.
   - Do not count past medical history, prior episodes, or family history.
2. Do not invent or infer facts beyond what is documented.
3. Maintain high fidelity to the clinical document language when citing spans.
4. Answer patient outcomes with strongest available documented evidence:
5. Always produce structured JSON that conforms to the Pydantic schema provided below.

Pydantic Schema:
%JSON-SCHEMA%"""
    USER_PROMPT = """Evaluate the following clinical document for glioma variables and outcomes.
Here is the clinical document for you to analyze:

%CLINICAL-NOTE%"""

    DATA_ROOT = "glioma"
    SUPPORTED_MODELS: ClassVar[list[ModelOption]] = [
        ("gpt_oss_120b", "gpt-oss-120b"),
        ("gpt4o", "gpt-4o"),
        ("gpt5", "gpt-5"),
        ("llama4_scout", "Llama-4-Scout-17B-16E-Instruct"),
    ]
    TEST_TASK_OPTIONS: ClassVar[list[TaskOption]] = [
        (
            lambda test_cls: test_cls.glioma_document_type_annotation_model(),
            lambda model: f"glioma__nlp_document_type_{model}",
            lambda test_cls: test_cls.SYSTEM_PROMPT.replace(
                "%JSON-SCHEMA%",
                json.dumps(test_cls.glioma_document_type_annotation_model().model_json_schema()),
            ),
            lambda test_cls, note_text: test_cls.USER_PROMPT.replace("%CLINICAL-NOTE%", note_text),
            lambda test_cls, annotation_data: test_cls.glioma_document_type_annotation(
                **annotation_data
            ),
            {},
            "glioma-document-type-output.ndjson",
        ),
        (
            lambda test_cls: test_cls.glioma_diagnosis_annotation_model(),
            lambda model: f"glioma__nlp_diagnosis_{model}",
            lambda test_cls: test_cls.SYSTEM_PROMPT.replace(
                "%JSON-SCHEMA%",
                json.dumps(test_cls.glioma_diagnosis_annotation_model().model_json_schema()),
            ),
            lambda test_cls, note_text: test_cls.USER_PROMPT.replace("%CLINICAL-NOTE%", note_text),
            lambda test_cls, annotation_data: test_cls.glioma_diagnosis_annotation(
                **annotation_data
            ),
            {},
            "glioma-diagnosis-output.ndjson",
        ),
        (
            lambda test_cls: test_cls.glioma_gene_annotation_model(),
            lambda model: f"glioma__nlp_gene_{model}",
            lambda test_cls: test_cls.SYSTEM_PROMPT.replace(
                "%JSON-SCHEMA%",
                json.dumps(test_cls.glioma_gene_annotation_model().model_json_schema()),
            ),
            lambda test_cls, note_text: test_cls.USER_PROMPT.replace("%CLINICAL-NOTE%", note_text),
            lambda test_cls, annotation_data: test_cls.glioma_gene_annotation(**annotation_data),
            {},
            "glioma-gene-output.ndjson",
        ),
        (
            lambda test_cls: test_cls.glioma_medications_annotation_model(),
            lambda model: f"glioma__nlp_medications_{model}",
            lambda test_cls: test_cls.SYSTEM_PROMPT.replace(
                "%JSON-SCHEMA%",
                json.dumps(test_cls.glioma_medications_annotation_model().model_json_schema()),
            ),
            lambda test_cls, note_text: test_cls.USER_PROMPT.replace("%CLINICAL-NOTE%", note_text),
            lambda test_cls, annotation_data: test_cls.glioma_medications_annotation(
                **annotation_data
            ),
            {},
            "glioma-medications-output.ndjson",
        ),
        (
            lambda test_cls: test_cls.glioma_progression_annotation_model(),
            lambda model: f"glioma__nlp_progression_{model}",
            lambda test_cls: test_cls.SYSTEM_PROMPT.replace(
                "%JSON-SCHEMA%",
                json.dumps(test_cls.glioma_progression_annotation_model().model_json_schema()),
            ),
            lambda test_cls, note_text: test_cls.USER_PROMPT.replace("%CLINICAL-NOTE%", note_text),
            lambda test_cls, annotation_data: test_cls.glioma_progression_annotation(
                **annotation_data
            ),
            {},
            "glioma-progression-output.ndjson",
        ),
        (
            lambda test_cls: test_cls.glioma_surgical_annotation_model(),
            lambda model: f"glioma__nlp_surgical_{model}",
            lambda test_cls: test_cls.SYSTEM_PROMPT.replace(
                "%JSON-SCHEMA%",
                json.dumps(test_cls.glioma_surgical_annotation_model().model_json_schema()),
            ),
            lambda test_cls, note_text: test_cls.USER_PROMPT.replace("%CLINICAL-NOTE%", note_text),
            lambda test_cls, annotation_data: test_cls.glioma_surgical_annotation(
                **annotation_data
            ),
            {},
            "glioma-surgical-output.ndjson",
        ),
    ]

    @ddt.data(
        *[
            (*model_options, *task_options)
            for model_options, task_options in itertools.product(
                SUPPORTED_MODELS, TEST_TASK_OPTIONS
            )
        ],
    )
    @ddt.unpack
    async def test_basic_etl(
        self,
        model_slug,
        model_id,
        get_annotation_cls,
        get_task_name_for_model,
        get_system_prompt,
        get_user_prompt,
        get_sample_annotation,
        annotation_data,
        fixture_name,
    ):
        self.mock_azure(model_id)
        note_text = "Test glioma note with mention of Grade II cancer"
        content = get_sample_annotation(self, annotation_data)

        self.mock_response(
            # Needs the class to generate the sample annotation using the relevant helper method
            content=content
        )
        glioma_task_name = get_task_name_for_model(model_slug)

        await self.run_etl(
            "--provider=azure",
            tasks=[
                glioma_task_name,
            ],
        )

        self.assert_files_equal(
            f"{self.root_path}/{fixture_name}",
            f"{self.output_path}/{glioma_task_name}/{glioma_task_name}.000.ndjson",
        )

        self.assertEqual(self.mock_create.call_count, 1)
        self.assertEqual(
            {
                "model": model_id,
                "messages": [
                    {
                        "role": "system",
                        "content": get_system_prompt(self),
                    },
                    {
                        "role": "user",
                        "content": get_user_prompt(self, note_text),
                    },
                ],
                "seed": 12345,
                "temperature": 0,
                "timeout": 120,
                "response_format": OpenAIProvider.pydantic_to_response_format(
                    get_annotation_cls(self)
                ),
            },
            self.mock_create.call_args_list[0][1],
        )
