import json
import shutil
from typing import ClassVar

import pydantic

from cumulus_etl.nlp.models import OpenAIProvider
from tests.etl import BaseEtlSimple
from tests.nlp.utils import NlpModelTestCase

#########
# Prompts fixed at the module level
# System Prompt
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

# User Prompt
USER_PROMPT = """Evaluate the following clinical document for IBD variables and outcomes.
Here is the clinical document for you to analyze:

%CLINICAL-NOTE%"""


class IbdTaskTestBase(NlpModelTestCase, BaseEtlSimple):
    """Abstract base for all IBD NLP task tests."""

    __test__ = False  # prevents unittest from trying to run this base class as a test case
    DATA_ROOT: ClassVar[str] = "ibd"
    SUPPORTED_MODELS: ClassVar[list[tuple[str, str]]] = [
        ("gpt_oss_120b", "gpt-oss-120b"),
        ("gpt5", "gpt-5"),
    ]

    # Subclasses define these four:
    ANNOTATION_SCHEMA_PATH: ClassVar[str]  # e.g. "ibd/ibd-diagnosis-annotation.json"
    TASK_NAME_SLUG: ClassVar[str]  # e.g. "diagnosis"
    DEFAULT_CONTENT: ClassVar[dict]  # default fields for model_validate()
    FIXTURE_NAME: ClassVar[str]  # e.g. "ibd-diagnosis-output.ndjson"

    @classmethod
    def annotation_model(cls) -> type[pydantic.BaseModel]:
        return cls.load_pydantic_model(cls.ANNOTATION_SCHEMA_PATH)

    @classmethod
    def make_annotation(cls, **kwargs) -> pydantic.BaseModel:
        content = dict(cls.DEFAULT_CONTENT)
        content.update(kwargs)
        return cls.annotation_model().model_validate(content)

    @classmethod
    def system_prompt(cls) -> str:
        return SYSTEM_PROMPT.replace(
            "%JSON-SCHEMA%",
            json.dumps(cls.annotation_model().model_json_schema()),
        )

    @staticmethod
    def user_prompt(note_text: str) -> str:
        return USER_PROMPT.replace("%CLINICAL-NOTE%", note_text)

    async def _run_task_test(self, model_slug: str, model_id: str) -> None:
        self.mock_azure(model_id)
        note_text = "Test ibd note with mention of Crohn's disease diagnosis"
        self.mock_response(content=self.make_annotation())
        task_name = f"ibd__nlp_{self.TASK_NAME_SLUG}_{model_slug}"
        await self.run_etl("--provider=azure", tasks=[task_name])
        self.assert_files_equal(
            f"{self.root_path}/{self.FIXTURE_NAME}",
            self.result_path(task_name),
        )
        self.assertEqual(self.mock_create.call_count, 1)
        self.assertEqual(
            {
                "model": model_id,
                "messages": [
                    {"role": "system", "content": self.system_prompt()},
                    {"role": "user", "content": self.user_prompt(note_text)},
                ],
                "seed": 12345,
                "temperature": 0,
                "timeout": 120,
                "response_format": OpenAIProvider.pydantic_to_response_format(
                    self.annotation_model()
                ),
            },
            self.mock_create.call_args_list[0][1],
        )

    async def test_basic_etl(self) -> None:
        for i, (model_slug, model_id) in enumerate(self.SUPPORTED_MODELS):
            # Reinitialize the set-up for each model to reset the mocks, call counts, and tempdirs
            super().setUp()
            with self.subTest(model_slug=model_slug, model_id=model_id):
                await self._run_task_test(model_slug, model_id)


class TestIbdDiagnosisTask(IbdTaskTestBase):
    __test__ = True  # re-enable test discovery for subclasses
    ANNOTATION_SCHEMA_PATH = "ibd/ibd-diagnosis-annotation.json"
    TASK_NAME_SLUG = "diagnosis"
    FIXTURE_NAME = "ibd-diagnosis-output.ndjson"
    DEFAULT_CONTENT: ClassVar[dict] = {
        "ibd_type_mention": {"has_mention": False, "spans": [], "ibd_type": "Crohn's disease"},
        "age_at_diagnosis_mention": {"has_mention": False, "spans": []},
        "diagnosis_date_mention": {"has_mention": False, "spans": []},
        "diagnosis_date_endoscopy_mention": {"has_mention": False, "spans": []},
    }


class TestIbdGeneticFindingsTask(IbdTaskTestBase):
    __test__ = True  # re-enable test discovery for subclasses
    ANNOTATION_SCHEMA_PATH = "ibd/ibd-genetic-findings-annotation.json"
    TASK_NAME_SLUG = "genetic_findings"
    FIXTURE_NAME = "ibd-genetic-findings-output.ndjson"
    DEFAULT_CONTENT: ClassVar[dict] = {
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


class TestIbdParisClassificationTask(IbdTaskTestBase):
    __test__ = True  # re-enable test discovery for subclasses
    ANNOTATION_SCHEMA_PATH = "ibd/ibd-paris-classification-annotation.json"
    TASK_NAME_SLUG = "paris_classification"
    FIXTURE_NAME = "ibd-paris-classification-output.ndjson"
    DEFAULT_CONTENT: ClassVar[dict] = {
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


class TestIbdTreatmentTask(IbdTaskTestBase):
    __test__ = True  # re-enable test discovery for subclasses
    ANNOTATION_SCHEMA_PATH = "ibd/ibd-treatment-annotation.json"
    TASK_NAME_SLUG = "treatment"
    FIXTURE_NAME = "ibd-treatment-output.ndjson"
    DEFAULT_CONTENT: ClassVar[dict] = {
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


class TestIbdSurgeryTask(IbdTaskTestBase):
    __test__ = True  # re-enable test discovery for subclasses
    ANNOTATION_SCHEMA_PATH = "ibd/ibd-surgery-annotation.json"
    TASK_NAME_SLUG = "surgery"
    FIXTURE_NAME = "ibd-surgery-output.ndjson"
    DEFAULT_CONTENT: ClassVar[dict] = {
        "surgery_mentions": [],
    }


class TestIbdEimTask(IbdTaskTestBase):
    __test__ = True  # re-enable test discovery for subclasses
    ANNOTATION_SCHEMA_PATH = "ibd/ibd-eim-annotation.json"
    TASK_NAME_SLUG = "eim"
    FIXTURE_NAME = "ibd-eim-output.ndjson"
    DEFAULT_CONTENT: ClassVar[dict] = {
        "fever": {"has_mention": False, "spans": []},
        "uveitis": {"has_mention": False, "spans": []},
        "arthritis": {"has_mention": False, "spans": []},
        "erythema_nodosum": {"has_mention": False, "spans": []},
        "pyoderma_gangrenosum": {"has_mention": False, "spans": []},
    }


class TestIbdEndoscopyTask(IbdTaskTestBase):
    __test__ = True  # re-enable test discovery for subclasses
    ANNOTATION_SCHEMA_PATH = "ibd/ibd-endoscopy-annotation.json"
    TASK_NAME_SLUG = "endoscopy"
    FIXTURE_NAME = "ibd-endoscopy-output.ndjson"
    DEFAULT_CONTENT: ClassVar[dict] = {
        "procedure": {"has_mention": False, "spans": []},
        "continuous": {"has_mention": False, "spans": []},
        "endoscopist_global_assessment": {"has_mention": False, "spans": []},
        "segment_findings": [],
    }


class TestIbdMayoScoreTask(IbdTaskTestBase):
    __test__ = True  # re-enable test discovery for subclasses
    ANNOTATION_SCHEMA_PATH = "ibd/ibd-mayo-score-annotation.json"
    TASK_NAME_SLUG = "mayo_score"
    FIXTURE_NAME = "ibd-mayo-score-output.ndjson"
    DEFAULT_CONTENT: ClassVar[dict] = {
        "score_variant": {"has_mention": False, "spans": []},
        "total_score": {"has_mention": False, "spans": []},
        "stool_frequency": {"has_mention": False, "spans": []},
        "rectal_bleeding": {"has_mention": False, "spans": []},
        "endoscopy": {"has_mention": False, "spans": []},
        "physician_global_assessment": {"has_mention": False, "spans": []},
    }


class TestIbdPcdaiTask(IbdTaskTestBase):
    __test__ = True  # re-enable test discovery for subclasses
    ANNOTATION_SCHEMA_PATH = "ibd/ibd-pcdai-annotation.json"
    TASK_NAME_SLUG = "pcdai"
    FIXTURE_NAME = "ibd-pcdai-output.ndjson"
    DEFAULT_CONTENT: ClassVar[dict] = {
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


class TestIbdPucaiTask(IbdTaskTestBase):
    __test__ = True  # re-enable test discovery for subclasses
    ANNOTATION_SCHEMA_PATH = "ibd/ibd-pucai-annotation.json"
    TASK_NAME_SLUG = "pucai"
    FIXTURE_NAME = "ibd-pucai-output.ndjson"
    DEFAULT_CONTENT: ClassVar[dict] = {
        "total_score": {"has_mention": False, "spans": []},
        "abdominal_pain": {"has_mention": False, "spans": []},
        "rectal_bleeding": {"has_mention": False, "spans": []},
        "stool_consistency": {"has_mention": False, "spans": []},
        "stools_per_day": {"has_mention": False, "spans": []},
        "nocturnal_stools": {"has_mention": False, "spans": []},
        "activity_level": {"has_mention": False, "spans": []},
    }


class TestIbdSescdTask(IbdTaskTestBase):
    __test__ = True  # re-enable test discovery for subclasses
    ANNOTATION_SCHEMA_PATH = "ibd/ibd-sescd-annotation.json"
    TASK_NAME_SLUG = "sescd"
    FIXTURE_NAME = "ibd-sescd-output.ndjson"
    DEFAULT_CONTENT: ClassVar[dict] = {
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


class TestIbdStoolingTask(IbdTaskTestBase):
    __test__ = True  # re-enable test discovery for subclasses
    ANNOTATION_SCHEMA_PATH = "ibd/ibd-stooling-annotation.json"
    TASK_NAME_SLUG = "stooling"
    FIXTURE_NAME = "ibd-stooling-output.ndjson"
    DEFAULT_CONTENT: ClassVar[dict] = {
        "stool_frequency": {"has_mention": False, "spans": []},
        "stool_consistency": {"has_mention": False, "spans": []},
        "stool_urgency": {"has_mention": False, "spans": []},
        "tenesmus": {"has_mention": False, "spans": []},
        "abdominal_pain": {"has_mention": False, "spans": []},
        "rectal_bleeding": {"has_mention": False, "spans": []},
    }


class TestIbdUceisTask(IbdTaskTestBase):
    __test__ = True  # re-enable test discovery for subclasses
    ANNOTATION_SCHEMA_PATH = "ibd/ibd-uceis-annotation.json"
    TASK_NAME_SLUG = "uceis"
    FIXTURE_NAME = "ibd-uceis-output.ndjson"
    DEFAULT_CONTENT: ClassVar[dict] = {
        "vascular_pattern": {"has_mention": False, "spans": []},
        "bleeding": {"has_mention": False, "spans": []},
        "erosion_ulcer": {"has_mention": False, "spans": []},
    }


class TestIbdDocumentTypeTask(IbdTaskTestBase):
    __test__ = True  # re-enable test discovery for subclasses
    ANNOTATION_SCHEMA_PATH = "ibd/ibd-document-type-annotation.json"
    TASK_NAME_SLUG = "document_type"
    FIXTURE_NAME = "ibd-document-type-output.ndjson"
    DEFAULT_CONTENT: ClassVar[dict] = {
        "document_type": {"has_mention": False, "confidence": None},
    }


class TestIbdTopicRelevanceTask(IbdTaskTestBase):
    __test__ = True  # re-enable test discovery for subclasses
    ANNOTATION_SCHEMA_PATH = "ibd/ibd-topic-relevance-annotation.json"
    TASK_NAME_SLUG = "topic_relevance"
    FIXTURE_NAME = "ibd-topic-relevance-output.ndjson"
    DEFAULT_CONTENT: ClassVar[dict] = {
        "pucai": {"has_mention": False, "confidence": 0.0},
        "pcdai": {"has_mention": False, "confidence": 0.0},
        "eim": {"has_mention": False, "confidence": 0.0},
        "mayo_score": {"has_mention": False, "confidence": 0.0},
        "uceis": {"has_mention": False, "confidence": 0.0},
        "ses_cd": {"has_mention": False, "confidence": 0.0},
        "paris_classification": {"has_mention": False, "confidence": 0.0},
        "endoscopy": {"has_mention": False, "confidence": 0.0},
        "diagnosis": {"has_mention": False, "confidence": 0.0},
        "genetics": {"has_mention": False, "confidence": 0.0},
        "surgery": {"has_mention": False, "confidence": 0.0},
        "stooling": {"has_mention": False, "confidence": 0.0},
        "medications": {"has_mention": False, "confidence": 0.0},
    }
