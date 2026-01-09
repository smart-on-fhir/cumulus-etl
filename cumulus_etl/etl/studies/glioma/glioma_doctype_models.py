"""
document_types.py

Enums + Pydantic "mention" models for extracting which document types are present/relevant
for each pLGG annotation module.

Design notes:
- We treat "document type" as something the reviewer/LLM can mark as present/referenced.
- Each module gets its own Enum + Mention class so you can keep module-specific taxonomies.
- Defaults are NOT_MENTIONED for the enum, and has_mention=False per SpanAugmentedMention.
- `document_types` is a list so you can capture multiple doc types in one note set.
"""

from enum import StrEnum
from typing import Optional
from pydantic import BaseModel, Field
from cumulus_etl.etl.studies.glioma.glioma_base_models import SpanAugmentedMention


# ------------------------------------------------------------------------------
# Document Types relevant to Glioma (pLGG)
# ------------------------------------------------------------------------------
class DocumentType(StrEnum):
    RADIOLOGY = "Radiology report: MRI brain/spine, CT Head, or MR spectroscopy"
    ONCOLOGY = "Oncology consult, oncology progress note, or neuro-oncology note type"
    NEURO = "Neurology consult, neurology progress note, or Neuropsychology note type"
    NEURO_SURGERY = "Neurosurgery consult, progress note, or operative report"
    OPHTHALMOLOGY = "Ophthalmology or neuro-ophthalmology consult or progress note"
    ENDOCRINOLOGY = "Endocrinology consult or progress note"
    GENETICS = "Genetics, molecular pathology, cytogenetics, or sequencing report"
    PATHOLOGY_REPORT_SURGICAL = "Surgical pathology report"
    PATHOLOGY_REPORT_BIOPSY = "Biopsy pathology report"
    TUMOR_BOARD_NOTE = "Tumor board or multidisciplinary conference note"
    ADMISSION_NOTE = "Admission note"
    DISCHARGE_SUMMARY = "Discharge summary"
    EMERGENCY_DEPARTMENT = "Emergency department note"
    MEDICATION_ORDER = "Medication order"
    MEDICATION_ADMIN = "Medication administration or infusion center note"
    CHEMOTHERAPY = "Chemotherapy treatment plan, protocol, or regimen"
    PHARMACY = "Pharmacy note"
    ADVERSE_EVENT = "Adverse event or drug toxicity note"
    CLINICAL_TRIAL = "Clinical trial consent, enrollment, or protocol"
    NONE = "None of the above"


class DocumentTypeMention(SpanAugmentedMention):
    doc_type: DocumentType = Field(
        DocumentType.NONE, description="What type of document/note is this?"
    )

    is_administrative: bool = Field(
        False,
        description="Is this document/note administrative and lacking clinical significance relevant to pLGG?",
    )

    doc_type_confidence: Optional[float] = Field(
        None, description="LLM confidence score (0-1) for the document type classification."
    )


# ------------------------------------------------------------------------------
# Annotation BaseModel
# ------------------------------------------------------------------------------
class DocumentTypeAnnotation(BaseModel):
    doc_type: list[DocumentTypeMention] = Field(
        default_factory=list, description="All document type mentions"
    )


# ------------------------------------------------------------------------------
# List document types for `diagnosis.py`
# ------------------------------------------------------------------------------
DIAGNOSIS_LIST = [
    DocumentType.NEURO,
    DocumentType.NEURO_SURGERY,
    DocumentType.OPHTHALMOLOGY,
    DocumentType.ONCOLOGY,
    DocumentType.RADIOLOGY,
    DocumentType.PATHOLOGY_REPORT_SURGICAL,
    DocumentType.PATHOLOGY_REPORT_BIOPSY,
    DocumentType.GENETICS,
    DocumentType.TUMOR_BOARD_NOTE,
    DocumentType.EMERGENCY_DEPARTMENT,
    DocumentType.ADMISSION_NOTE,
    DocumentType.DISCHARGE_SUMMARY,
]

# ------------------------------------------------------------------------------
# List document types for `drug_glioma.py`
# ------------------------------------------------------------------------------
DRUG_LIST = [
    DocumentType.ONCOLOGY,
    DocumentType.PHARMACY,
    DocumentType.MEDICATION_ORDER,
    DocumentType.MEDICATION_ADMIN,
    DocumentType.CHEMOTHERAPY,
    DocumentType.ADVERSE_EVENT,
    DocumentType.CLINICAL_TRIAL,
    DocumentType.TUMOR_BOARD_NOTE,
    DocumentType.ADMISSION_NOTE,
    DocumentType.DISCHARGE_SUMMARY,
]

# ------------------------------------------------------------------------------
# List document types for `gene.py`
# ------------------------------------------------------------------------------
GENE_LIST = [
    DocumentType.NEURO,
    DocumentType.ONCOLOGY,
    DocumentType.GENETICS,
    DocumentType.PATHOLOGY_REPORT_SURGICAL,
    DocumentType.PATHOLOGY_REPORT_BIOPSY,
    DocumentType.TUMOR_BOARD_NOTE,
    DocumentType.ADMISSION_NOTE,
    DocumentType.DISCHARGE_SUMMARY,
]

# ------------------------------------------------------------------------------
# List document types `surgery.py`
# ------------------------------------------------------------------------------
SURGERY_LIST = [
    DocumentType.NEURO_SURGERY,
    DocumentType.RADIOLOGY,
    DocumentType.TUMOR_BOARD_NOTE,
    DocumentType.DISCHARGE_SUMMARY,
    DocumentType.PATHOLOGY_REPORT_SURGICAL,
]


# ------------------------------------------------------------------------------
# List document types for `progression.py`
# ------------------------------------------------------------------------------
PROGRESSION_LIST = [
    DocumentType.NEURO,
    DocumentType.ONCOLOGY,
    DocumentType.RADIOLOGY,
    DocumentType.OPHTHALMOLOGY,
    DocumentType.ENDOCRINOLOGY,
    DocumentType.TUMOR_BOARD_NOTE,
    DocumentType.EMERGENCY_DEPARTMENT,
    DocumentType.ADMISSION_NOTE,
    DocumentType.DISCHARGE_SUMMARY,
]
