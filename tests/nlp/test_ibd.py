"""Tests for etl/studies/ibd/"""

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
class TestIbdTasks(NlpModelTestCase, BaseEtlSimple):
    """Test case for Ibd tasks"""

    #######################################################
    # Static methods for generating various annotation classes and instances
    #
    # - ibd-diagnosis-annotation.json
    @classmethod
    def ibd_diagnosis_annotation_model(cls):
        return cls.load_pydantic_model("ibd/ibd-diagnosis-annotation.json")

    @classmethod
    def ibd_diagnosis_annotation(cls, **kwargs):
        content = {
            "ibd_type_mention": {"has_mention": False, "spans": [], "ibd_type": "Crohn's disease"},
            "age_at_diagnosis_mention": {"has_mention": False, "spans": []},
            "diagnosis_date_mention": {"has_mention": False, "spans": []},
            "diagnosis_date_endoscopy_mention": {"has_mention": False, "spans": []},
        }
        content.update(kwargs)
        return cls.ibd_diagnosis_annotation_model().model_validate(content)

    # - ibd-genetic-findings-annotation.json
    @classmethod
    def ibd_genetic_findings_annotation_model(cls):
        return cls.load_pydantic_model("ibd/ibd-genetic-findings-annotation.json")

    @classmethod
    def ibd_genetic_findings_annotation(cls, **kwargs):
        content = {
            "monogenic_primary_mention": {
                "has_mention": False,
                "spans": [],
                "ibd_monogenic": "None of the above",
            },
            "monogenic_secondary_mention": {
                "has_mention": False,
                "spans": [],
                "ibd_monogenic_secondary": "None of the above",
            },
        }
        content.update(kwargs)
        return cls.ibd_genetic_findings_annotation_model().model_validate(content)

    # - ibd-paris-classification-annotation.json
    @classmethod
    def ibd_paris_classification_annotation_model(cls):
        return cls.load_pydantic_model("ibd/ibd-paris-classification-annotation.json")

    @classmethod
    def ibd_paris_classification_annotation(cls, **kwargs):
        content = {
            "cd_paris_location_exclusive_mention": {
                "has_mention": False,
                "spans": [],
                "location": "None of the above (e.g. not mentioned, patient does not have CD, etc)",
            },
            "cd_paris_location_l4a_mention": {"has_mention": False, "spans": [], "l4a": False},
            "cd_paris_location_l4b_mention": {"has_mention": False, "spans": [], "l4b": False},
            "cd_paris_behavior_exclusive_mention": {
                "has_mention": False,
                "spans": [],
                "behavior": "None of the above (e.g. not mentioned, patient does not have CD, etc)",
            },
            "cd_paris_behavior_perianal_modifier_mention": {
                "has_mention": False,
                "spans": [],
                "perianal_disease": False,
            },
            "uc_paris_location_mention": {
                "has_mention": False,
                "spans": [],
                "location": "None of the above (e.g. not mentioned, patient does not have UC, etc)",
            },
            "uc_paris_severity_mention": {
                "has_mention": False,
                "spans": [],
                "severity": "None of the above (e.g. not mentioned, patient does not have UC, etc)",
            },
        }
        content.update(kwargs)
        return cls.ibd_paris_classification_annotation_model().model_validate(content)

    # - ibd-treatment-annotation.json
    @classmethod
    def ibd_treatment_annotation_model(cls):
        return cls.load_pydantic_model("ibd/ibd-treatment-annotation.json")

    @classmethod
    def ibd_treatment_annotation(cls, **kwargs):
        content = {
            "rx_start_date_mention": {"has_mention": False, "spans": []},
            "rx_tnf_start_date_mention": {"has_mention": False, "spans": []},
            "rx_annotations": [],
            "anti_tnf_response_mention": {
                "has_mention": False,
                "spans": [],
                "anti_tnf_response": "None of the above (e.g. not mentioned, patient was not given anti-TNF therapy, etc)",
            },
            "rx_effectiveness_mention": {
                "has_mention": False,
                "spans": [],
                "rx_effectiveness": "None of the above (e.g. not mentioned, patient was not given IBD medication, etc)",
            },
            "adverse_drug_event_ibd_mention": {
                "has_mention": False,
                "spans": [],
                "ade": "None of the above (e.g. not mentioned, patient does not have an allergy or adverse drug event related to this medication, etc)",
            },
            "adverse_drug_event_anti_tnf_mention": {
                "has_mention": False,
                "spans": [],
                "ade": "None of the above (e.g. not mentioned, patient does not have an allergy or adverse drug event related to this medication, etc)",
            },
        }
        content.update(kwargs)
        return cls.ibd_treatment_annotation_model().model_validate(content)

    # - ibd-surgery-annotation.json
    @classmethod
    def ibd_surgery_annotation_model(cls):
        return cls.load_pydantic_model("ibd/ibd-surgery-annotation.json")

    @classmethod
    def ibd_surgery_annotation(cls, **kwargs):
        content = {
            "surgery_mentions": [],
        }
        content.update(kwargs)
        return cls.ibd_surgery_annotation_model().model_validate(content)

    # - ibd-eim-annotation.json
    @classmethod
    def ibd_eim_annotation_model(cls):
        return cls.load_pydantic_model("ibd/ibd-eim-annotation.json")

    @classmethod
    def ibd_eim_annotation(cls, **kwargs):
        content = {
            "fever": {"has_mention": False, "spans": []},
            "uveitis": {"has_mention": False, "spans": []},
            "arthritis": {"has_mention": False, "spans": []},
            "erythema_nodosum": {"has_mention": False, "spans": []},
            "pyoderma_gangrenosum": {"has_mention": False, "spans": []},
        }
        content.update(kwargs)
        return cls.ibd_eim_annotation_model().model_validate(content)

    # - ibd-endoscopy-annotation.json
    @classmethod
    def ibd_endoscopy_annotation_model(cls):
        return cls.load_pydantic_model("ibd/ibd-endoscopy-annotation.json")

    @classmethod
    def ibd_endoscopy_annotation(cls, **kwargs):
        content = {
            "procedure": {"has_mention": False, "spans": []},
            "continuous": {"has_mention": False, "spans": []},
            "endoscopist_global_assessment": {"has_mention": False, "spans": []},
            "segment_findings": [],
        }
        content.update(kwargs)
        return cls.ibd_endoscopy_annotation_model().model_validate(content)

    # - ibd-mayo-score-annotation.json
    @classmethod
    def ibd_mayo_score_annotation_model(cls):
        return cls.load_pydantic_model("ibd/ibd-mayo-score-annotation.json")

    @classmethod
    def ibd_mayo_score_annotation(cls, **kwargs):
        content = {
            "score_variant": {"has_mention": False, "spans": []},
            "total_score": {"has_mention": False, "spans": []},
            "stool_frequency": {"has_mention": False, "spans": []},
            "rectal_bleeding": {"has_mention": False, "spans": []},
            "endoscopy": {"has_mention": False, "spans": []},
            "physician_global_assessment": {"has_mention": False, "spans": []},
        }
        content.update(kwargs)
        return cls.ibd_mayo_score_annotation_model().model_validate(content)

    # - ibd-pcdai-annotation.json
    @classmethod
    def ibd_pcdai_annotation_model(cls):
        return cls.load_pydantic_model("ibd/ibd-pcdai-annotation.json")

    @classmethod
    def ibd_pcdai_annotation(cls, **kwargs):
        content = {
            "total_score": {"has_mention": False, "spans": []},
            "abdominal_pain": {"has_mention": False, "spans": []},
            "abdominal_tenderness": {"has_mention": False, "spans": []},
            "stools_per_day": {"has_mention": False, "spans": []},
            "general_well_being": {"has_mention": False, "spans": []},
            "height": {"has_mention": False, "spans": []},
            "weight": {"has_mention": False, "spans": []},
            "perianal_disease": {"has_mention": False, "spans": []},
            "eim": {
                "fever": {"has_mention": False, "spans": []},
                "uveitis": {"has_mention": False, "spans": []},
                "arthritis": {"has_mention": False, "spans": []},
                "erythema_nodosum": {"has_mention": False, "spans": []},
                "pyoderma_gangrenosum": {"has_mention": False, "spans": []},
            },
            "lab_hematocrit": {"has_mention": False, "spans": []},
            "lab_esr": {"has_mention": False, "spans": []},
            "lab_albumin": {"has_mention": False, "spans": []},
        }
        content.update(kwargs)
        return cls.ibd_pcdai_annotation_model().model_validate(content)

    # - ibd-pucai-annotation.json
    @classmethod
    def ibd_pucai_annotation_model(cls):
        return cls.load_pydantic_model("ibd/ibd-pucai-annotation.json")

    @classmethod
    def ibd_pucai_annotation(cls, **kwargs):
        content = {
            "total_score": {"has_mention": False, "spans": []},
            "abdominal_pain": {"has_mention": False, "spans": []},
            "rectal_bleeding": {"has_mention": False, "spans": []},
            "stool_consistency": {"has_mention": False, "spans": []},
            "stools_per_day": {"has_mention": False, "spans": []},
            "nocturnal_stools": {"has_mention": False, "spans": []},
            "activity_level": {"has_mention": False, "spans": []},
        }
        content.update(kwargs)
        return cls.ibd_pucai_annotation_model().model_validate(content)

    # - ibd-sescd-annotation.json
    @classmethod
    def ibd_sescd_annotation_model(cls):
        return cls.load_pydantic_model("ibd/ibd-sescd-annotation.json")

    @classmethod
    def ibd_sescd_annotation(cls, **kwargs):
        content = {
            "segment_findings": {
                "ileum": {"has_mention": False, "spans": []},
                "right_colon": {"has_mention": False, "spans": []},
                "transverse_colon": {"has_mention": False, "spans": []},
                "left_colon": {"has_mention": False, "spans": []},
                "rectum": {"has_mention": False, "spans": []},
            },
            "segment_subscores": {
                "ileum": {"has_mention": False, "spans": []},
                "right_colon": {"has_mention": False, "spans": []},
                "transverse_colon": {"has_mention": False, "spans": []},
                "left_colon": {"has_mention": False, "spans": []},
                "rectum": {"has_mention": False, "spans": []},
            },
        }
        content.update(kwargs)
        return cls.ibd_sescd_annotation_model().model_validate(content)

    # - ibd-stooling-annotation.json
    @classmethod
    def ibd_stooling_annotation_model(cls):
        return cls.load_pydantic_model("ibd/ibd-stooling-annotation.json")

    @classmethod
    def ibd_stooling_annotation(cls, **kwargs):
        content = {
            "stool_frequency": {"has_mention": False, "spans": []},
            "stool_consistency": {"has_mention": False, "spans": []},
            "stool_urgency": {"has_mention": False, "spans": []},
            "tenesmus": {"has_mention": False, "spans": []},
            "abdominal_pain": {"has_mention": False, "spans": []},
            "rectal_bleeding": {"has_mention": False, "spans": []},
        }
        content.update(kwargs)
        return cls.ibd_stooling_annotation_model().model_validate(content)

    # - ibd-uceis-annotation.json
    @classmethod
    def ibd_uceis_annotation_model(cls):
        return cls.load_pydantic_model("ibd/ibd-uceis-annotation.json")

    @classmethod
    def ibd_uceis_annotation(cls, **kwargs):
        content = {
            "vascular_pattern": {"has_mention": False, "spans": []},
            "bleeding": {"has_mention": False, "spans": []},
            "erosion_ulcer": {"has_mention": False, "spans": []},
        }
        content.update(kwargs)
        return cls.ibd_uceis_annotation_model().model_validate(content)

    # Path to relevant Tasks files
    SYSTEM_PROMPT = """You are a clinical chart reviewer for an IBD (inflammatory bowel disease) outcomes study.
Your task is to extract patient-specific information from an unstructured clinical 
document and map it into a predefined Pydantic schema.

Core Rules:
1. Base all assertions ONLY on patient-specific information in the clinical document.
   - Never negate or exclude information just because it is not mentioned.
   - Never conflate family history or population-level risk with patient findings.
   - Do not count past medical history, prior episodes, or family history.
   - Spans provided should ALWAYS be verbatim.
2. Do not invent or infer facts beyond what is documented.
3. Citing spans should be exact text from the clinical document, not paraphrased or shorthand.
4. Answer patient outcomes with strongest available documented evidence. E.g.
    BIOPSY_PROVEN > CONFIRMED > SUSPECTED > NONE_OF_THE_ABOVE.
5. Always produce structured JSON that conforms to the Pydantic schema provided below.

Pydantic Schema:
%JSON-SCHEMA%"""
    USER_PROMPT = """Evaluate the following clinical document for IBD variables and outcomes.
Here is the clinical document for you to analyze:

%CLINICAL-NOTE%"""

    DATA_ROOT = "ibd"
    SUPPORTED_MODELS: ClassVar[list[ModelOption]] = [
        ("gpt_oss_120b", "gpt-oss-120b"),
        ("gpt5", "gpt-5"),
    ]
    TEST_TASK_OPTIONS: ClassVar[list[TaskOption]] = [
        (
            lambda test_cls: test_cls.ibd_diagnosis_annotation_model(),
            lambda model: f"ibd__nlp_diagnosis_{model}",
            lambda test_cls: test_cls.SYSTEM_PROMPT.replace(
                "%JSON-SCHEMA%",
                json.dumps(test_cls.ibd_diagnosis_annotation_model().model_json_schema()),
            ),
            lambda test_cls, note_text: test_cls.USER_PROMPT.replace("%CLINICAL-NOTE%", note_text),
            lambda test_cls, annotation_data: test_cls.ibd_diagnosis_annotation(**annotation_data),
            {},
            "ibd-diagnosis-output.ndjson",
        ),
        (
            lambda test_cls: test_cls.ibd_genetic_findings_annotation_model(),
            lambda model: f"ibd__nlp_genetic_findings_{model}",
            lambda test_cls: test_cls.SYSTEM_PROMPT.replace(
                "%JSON-SCHEMA%",
                json.dumps(test_cls.ibd_genetic_findings_annotation_model().model_json_schema()),
            ),
            lambda test_cls, note_text: test_cls.USER_PROMPT.replace("%CLINICAL-NOTE%", note_text),
            lambda test_cls, annotation_data: test_cls.ibd_genetic_findings_annotation(
                **annotation_data
            ),
            {},
            "ibd-genetic-findings-output.ndjson",
        ),
        (
            lambda test_cls: test_cls.ibd_paris_classification_annotation_model(),
            lambda model: f"ibd__nlp_paris_classification_{model}",
            lambda test_cls: test_cls.SYSTEM_PROMPT.replace(
                "%JSON-SCHEMA%",
                json.dumps(
                    test_cls.ibd_paris_classification_annotation_model().model_json_schema()
                ),
            ),
            lambda test_cls, note_text: test_cls.USER_PROMPT.replace("%CLINICAL-NOTE%", note_text),
            lambda test_cls, annotation_data: test_cls.ibd_paris_classification_annotation(
                **annotation_data
            ),
            {},
            "ibd-paris-classification-output.ndjson",
        ),
        (
            lambda test_cls: test_cls.ibd_treatment_annotation_model(),
            lambda model: f"ibd__nlp_treatment_{model}",
            lambda test_cls: test_cls.SYSTEM_PROMPT.replace(
                "%JSON-SCHEMA%",
                json.dumps(test_cls.ibd_treatment_annotation_model().model_json_schema()),
            ),
            lambda test_cls, note_text: test_cls.USER_PROMPT.replace("%CLINICAL-NOTE%", note_text),
            lambda test_cls, annotation_data: test_cls.ibd_treatment_annotation(**annotation_data),
            {},
            "ibd-treatment-output.ndjson",
        ),
        (
            lambda test_cls: test_cls.ibd_surgery_annotation_model(),
            lambda model: f"ibd__nlp_surgery_{model}",
            lambda test_cls: test_cls.SYSTEM_PROMPT.replace(
                "%JSON-SCHEMA%",
                json.dumps(test_cls.ibd_surgery_annotation_model().model_json_schema()),
            ),
            lambda test_cls, note_text: test_cls.USER_PROMPT.replace("%CLINICAL-NOTE%", note_text),
            lambda test_cls, annotation_data: test_cls.ibd_surgery_annotation(**annotation_data),
            {},
            "ibd-surgery-output.ndjson",
        ),
        (
            lambda test_cls: test_cls.ibd_eim_annotation_model(),
            lambda model: f"ibd__nlp_eim_{model}",
            lambda test_cls: test_cls.SYSTEM_PROMPT.replace(
                "%JSON-SCHEMA%",
                json.dumps(test_cls.ibd_eim_annotation_model().model_json_schema()),
            ),
            lambda test_cls, note_text: test_cls.USER_PROMPT.replace("%CLINICAL-NOTE%", note_text),
            lambda test_cls, annotation_data: test_cls.ibd_eim_annotation(**annotation_data),
            {},
            "ibd-eim-output.ndjson",
        ),
        (
            lambda test_cls: test_cls.ibd_endoscopy_annotation_model(),
            lambda model: f"ibd__nlp_endoscopy_{model}",
            lambda test_cls: test_cls.SYSTEM_PROMPT.replace(
                "%JSON-SCHEMA%",
                json.dumps(test_cls.ibd_endoscopy_annotation_model().model_json_schema()),
            ),
            lambda test_cls, note_text: test_cls.USER_PROMPT.replace("%CLINICAL-NOTE%", note_text),
            lambda test_cls, annotation_data: test_cls.ibd_endoscopy_annotation(**annotation_data),
            {},
            "ibd-endoscopy-output.ndjson",
        ),
        (
            lambda test_cls: test_cls.ibd_mayo_score_annotation_model(),
            lambda model: f"ibd__nlp_mayo_score_{model}",
            lambda test_cls: test_cls.SYSTEM_PROMPT.replace(
                "%JSON-SCHEMA%",
                json.dumps(test_cls.ibd_mayo_score_annotation_model().model_json_schema()),
            ),
            lambda test_cls, note_text: test_cls.USER_PROMPT.replace("%CLINICAL-NOTE%", note_text),
            lambda test_cls, annotation_data: test_cls.ibd_mayo_score_annotation(**annotation_data),
            {},
            "ibd-mayo-score-output.ndjson",
        ),
        (
            lambda test_cls: test_cls.ibd_pcdai_annotation_model(),
            lambda model: f"ibd__nlp_pcdai_{model}",
            lambda test_cls: test_cls.SYSTEM_PROMPT.replace(
                "%JSON-SCHEMA%",
                json.dumps(test_cls.ibd_pcdai_annotation_model().model_json_schema()),
            ),
            lambda test_cls, note_text: test_cls.USER_PROMPT.replace("%CLINICAL-NOTE%", note_text),
            lambda test_cls, annotation_data: test_cls.ibd_pcdai_annotation(**annotation_data),
            {},
            "ibd-pcdai-output.ndjson",
        ),
        (
            lambda test_cls: test_cls.ibd_pucai_annotation_model(),
            lambda model: f"ibd__nlp_pucai_{model}",
            lambda test_cls: test_cls.SYSTEM_PROMPT.replace(
                "%JSON-SCHEMA%",
                json.dumps(test_cls.ibd_pucai_annotation_model().model_json_schema()),
            ),
            lambda test_cls, note_text: test_cls.USER_PROMPT.replace("%CLINICAL-NOTE%", note_text),
            lambda test_cls, annotation_data: test_cls.ibd_pucai_annotation(**annotation_data),
            {},
            "ibd-pucai-output.ndjson",
        ),
        (
            lambda test_cls: test_cls.ibd_sescd_annotation_model(),
            lambda model: f"ibd__nlp_sescd_{model}",
            lambda test_cls: test_cls.SYSTEM_PROMPT.replace(
                "%JSON-SCHEMA%",
                json.dumps(test_cls.ibd_sescd_annotation_model().model_json_schema()),
            ),
            lambda test_cls, note_text: test_cls.USER_PROMPT.replace("%CLINICAL-NOTE%", note_text),
            lambda test_cls, annotation_data: test_cls.ibd_sescd_annotation(**annotation_data),
            {},
            "ibd-sescd-output.ndjson",
        ),
        (
            lambda test_cls: test_cls.ibd_stooling_annotation_model(),
            lambda model: f"ibd__nlp_stooling_{model}",
            lambda test_cls: test_cls.SYSTEM_PROMPT.replace(
                "%JSON-SCHEMA%",
                json.dumps(test_cls.ibd_stooling_annotation_model().model_json_schema()),
            ),
            lambda test_cls, note_text: test_cls.USER_PROMPT.replace("%CLINICAL-NOTE%", note_text),
            lambda test_cls, annotation_data: test_cls.ibd_stooling_annotation(**annotation_data),
            {},
            "ibd-stooling-output.ndjson",
        ),
        (
            lambda test_cls: test_cls.ibd_uceis_annotation_model(),
            lambda model: f"ibd__nlp_uceis_{model}",
            lambda test_cls: test_cls.SYSTEM_PROMPT.replace(
                "%JSON-SCHEMA%",
                json.dumps(test_cls.ibd_uceis_annotation_model().model_json_schema()),
            ),
            lambda test_cls, note_text: test_cls.USER_PROMPT.replace("%CLINICAL-NOTE%", note_text),
            lambda test_cls, annotation_data: test_cls.ibd_uceis_annotation(**annotation_data),
            {},
            "ibd-uceis-output.ndjson",
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
        note_text = "Test ibd note with mention of Crohn's disease diagnosis"
        content = get_sample_annotation(self, annotation_data)

        self.mock_response(
            # Needs the class to generate the sample annotation using the relevant helper method
            content=content
        )
        ibd_task_name = get_task_name_for_model(model_slug)

        await self.run_etl(
            "--provider=azure",
            tasks=[
                ibd_task_name,
            ],
        )

        self.assert_files_equal(
            f"{self.root_path}/{fixture_name}",
            self.result_path(ibd_task_name),
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
