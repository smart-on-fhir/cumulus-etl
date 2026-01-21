"""Tests for etl/studies/glioma/"""

import itertools
import json
from collections.abc import Callable
from typing import Any, ClassVar

import ddt
import pydantic

from cumulus_etl.etl.studies.glioma.glioma_tasks import (
    BaseGliomaTask,
    GliomaDiagnosisAnnotation,
    GliomaDocumentTypeAnnotation,
    GliomaGeneAnnotation,
    GliomaMedicationsAnnotation,
    GliomaProgressionAnnotation,
    GliomaSurgicalAnnotation,
)
from cumulus_etl.nlp.models import OpenAIProvider
from tests.etl import BaseEtlSimple
from tests.nlp.utils import NlpModelTestCase

ModelOption = tuple[
    str,  # Model Slug
    str,  # Model ID
]

TaskOption = tuple[
    # The Annotation model of interest
    type[pydantic.BaseModel],
    # Return the task_name given the model of interest
    Callable[[str], str],
    # The system prompt given a particular annotation class
    str,
    # Return the user prompt given some note text
    Callable[[str], str],
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
    # Static methods for generating various
    # annotation classes
    @staticmethod
    def glioma_document_type_annotation(**kwargs):
        content = {
            "glioma_doc_type_mention": [],
        }
        content.update(kwargs)
        return GliomaDocumentTypeAnnotation.model_validate(content)

    @staticmethod
    def glioma_diagnosis_annotation(**kwargs):
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
        return GliomaDiagnosisAnnotation.model_validate(content)

    @staticmethod
    def glioma_gene_annotation(**kwargs):
        content = {
            "molecular_driver_mention": [],
            "genetic_variant_mention": [],
        }
        content.update(kwargs)
        return GliomaGeneAnnotation.model_validate(content)

    @staticmethod
    def glioma_medications_annotation(**kwargs):
        content = {
            "chemotherapy_mention": [],
            "targeted_therapy_mention": [],
        }
        content.update(kwargs)
        return GliomaMedicationsAnnotation.model_validate(content)

    @staticmethod
    def glioma_progression_annotation(**kwargs):
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
        return GliomaProgressionAnnotation.model_validate(content)

    @staticmethod
    def glioma_surgical_annotation(**kwargs):
        content = {
            "surgical_type_mention": {"has_mention": False, "spans": []},
            "approach_mention": {"has_mention": False, "spans": []},
            "extent_of_resection_mention": {"has_mention": False, "spans": []},
        }
        content.update(kwargs)
        return GliomaSurgicalAnnotation.model_validate(content)

    DATA_ROOT = "glioma"
    SUPPORTED_MODELS: ClassVar[list[ModelOption]] = [
        ("gpt_oss_120b", "gpt-oss-120b"),
        ("gpt4o", "gpt-4o"),
        ("gpt5", "gpt-5"),
        ("llama4_scout", "Llama-4-Scout-17B-16E-Instruct"),
    ]
    TEST_TASK_OPTIONS: ClassVar[list[TaskOption]] = [
        (
            GliomaDiagnosisAnnotation,
            lambda model: f"glioma__nlp_diagnosis_{model}",
            BaseGliomaTask.system_prompt.replace(
                "%JSON-SCHEMA%", json.dumps(GliomaDiagnosisAnnotation.model_json_schema())
            ),
            lambda note_text: BaseGliomaTask.user_prompt.replace("%CLINICAL-NOTE%", note_text),
            lambda test_cls, annotation_data: test_cls.glioma_diagnosis_annotation(
                **annotation_data
            ),
            {},
            "glioma-diagnosis-output.ndjson",
        ),
        (
            GliomaMedicationsAnnotation,
            lambda model: f"glioma__nlp_medications_{model}",
            BaseGliomaTask.system_prompt.replace(
                "%JSON-SCHEMA%", json.dumps(GliomaMedicationsAnnotation.model_json_schema())
            ),
            lambda note_text: BaseGliomaTask.user_prompt.replace("%CLINICAL-NOTE%", note_text),
            lambda test_cls, annotation_data: test_cls.glioma_medications_annotation(
                **annotation_data
            ),
            {},
            "glioma-medications-output.ndjson",
        ),
        (
            GliomaSurgicalAnnotation,
            lambda model: f"glioma__nlp_surgical_{model}",
            BaseGliomaTask.system_prompt.replace(
                "%JSON-SCHEMA%", json.dumps(GliomaSurgicalAnnotation.model_json_schema())
            ),
            lambda note_text: BaseGliomaTask.user_prompt.replace("%CLINICAL-NOTE%", note_text),
            lambda test_cls, annotation_data: test_cls.glioma_surgical_annotation(
                **annotation_data
            ),
            {},
            "glioma-surgical-output.ndjson",
        ),
        (
            GliomaGeneAnnotation,
            lambda model: f"glioma__nlp_gene_{model}",
            BaseGliomaTask.system_prompt.replace(
                "%JSON-SCHEMA%", json.dumps(GliomaGeneAnnotation.model_json_schema())
            ),
            lambda note_text: BaseGliomaTask.user_prompt.replace("%CLINICAL-NOTE%", note_text),
            lambda test_cls, annotation_data: test_cls.glioma_gene_annotation(**annotation_data),
            {},
            "glioma-gene-output.ndjson",
        ),
        (
            GliomaProgressionAnnotation,
            lambda model: f"glioma__nlp_progression_{model}",
            BaseGliomaTask.system_prompt.replace(
                "%JSON-SCHEMA%", json.dumps(GliomaProgressionAnnotation.model_json_schema())
            ),
            lambda note_text: BaseGliomaTask.user_prompt.replace("%CLINICAL-NOTE%", note_text),
            lambda test_cls, annotation_data: test_cls.glioma_progression_annotation(
                **annotation_data
            ),
            {},
            "glioma-progression-output.ndjson",
        ),
        (
            GliomaDocumentTypeAnnotation,
            lambda model: f"glioma__nlp_document_type_{model}",
            BaseGliomaTask.system_prompt.replace(
                "%JSON-SCHEMA%", json.dumps(GliomaDocumentTypeAnnotation.model_json_schema())
            ),
            lambda note_text: BaseGliomaTask.user_prompt.replace("%CLINICAL-NOTE%", note_text),
            lambda test_cls, annotation_data: test_cls.glioma_document_type_annotation(
                **annotation_data
            ),
            {},
            "glioma-document-type-output.ndjson",
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
        annotation_cls,
        get_task_name_for_model,
        system_prompt,
        get_user_prompt,
        get_sample_annotation,
        annotation_data,
        fixture_name,
    ):
        self.mock_azure(model_id)
        note_text = "Test glioma note with mention of Grade II cancer"
        self.mock_response(
            # Needs the class to generate the sample annotation using the relevant helper method
            content=get_sample_annotation(self, annotation_data)
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
                "messages": [
                    {
                        "role": "system",
                        "content": system_prompt,
                    },
                    {
                        "role": "user",
                        "content": get_user_prompt(note_text),
                    },
                ],
                "model": model_id,
                "seed": 12345,
                "temperature": 0,
                "timeout": 120,
                "response_format": OpenAIProvider.pydantic_to_response_format(annotation_cls),
            },
            self.mock_create.call_args_list[0][1],
        )  # This assertion is incorrect and needs to be fixed.
