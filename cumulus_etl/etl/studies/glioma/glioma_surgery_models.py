from enum import StrEnum

from pydantic import Field

from cumulus_etl.etl.studies.glioma.glioma_base_models import SpanAugmentedMention


###############################################################################
# Surgery
###############################################################################
class SurgicalType(StrEnum):
    BIOPSY = "BIOPSY"
    RESECTION = "RESECTION"
    DEBULKING = "DEBULKING"
    ABLATION = "ABLATION"  # LITT, RFA, etc.
    ENDOSCOPIC = "ENDOSCOPIC"
    CRANIOTOMY = "CRANIOTOMY"
    OTHER = "OTHER"
    NOT_MENTIONED = "NOT_MENTIONED"


class SurgicalApproach(StrEnum):
    OPEN = "OPEN"
    AWAKE = "AWAKE"
    ENDOSCOPIC = "ENDOSCOPIC"
    STEREOTACTIC = "STEREOTACTIC"
    LASER = "LASER"
    KEYHOLE = "KEYHOLE"
    NOT_MENTIONED = "NOT_MENTIONED"


class SurgicalExtentOfResection(StrEnum):
    GROSS_TOTAL = "GROSS_TOTAL"
    SUBTOTAL = "SUBTOTAL"
    PARTIAL = "PARTIAL"
    BIOPSY_ONLY = "BIOPSY_ONLY"
    SUPRATOTAL = "SUPRATOTAL"  # used in LGG for seizure control
    NOT_APPLICABLE = "NOT_APPLICABLE"
    NOT_MENTIONED = "NOT_MENTIONED"


class SurgeryMention(SpanAugmentedMention):
    """
    Structured representation of a surgical procedure performed for a cancer
    (e.g., low-grade glioma). Includes surgical type, approach, and extent of
    resection when identifiable from the clinical note.
    """

    surgical_type: SurgicalType = Field(
        default=SurgicalType.NOT_MENTIONED,
        description="High-level categorization of the surgery (biopsy, resection, ablation, etc.)",
    )

    approach: SurgicalApproach = Field(
        default=SurgicalApproach.NOT_MENTIONED,
        description="Technical approach used during the surgery (open, awake, stereotactic, laser).",
    )

    extent_of_resection: SurgicalExtentOfResection = Field(
        default=SurgicalExtentOfResection.NOT_MENTIONED,
        description="Reported extent of tumor resection.",
    )

    anatomical_site: str | None = Field(
        default=None,
        description="Anatomical site of surgery (e.g., 'left frontal lobe', 'right temporal lobe').",
    )

    technique_details: str | None = Field(
        default=None,
        description="Additional operative details (e.g., 'intraoperative mapping', 'iMRI-guided').",
    )

    complications: str | None = Field(
        default=None, description="Intraoperative or postoperative complications if mentioned."
    )
