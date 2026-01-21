from enum import StrEnum

from pydantic import BaseModel, Field

from cumulus_etl.etl.studies.glioma.glioma_base_models import SpanAugmentedMention


###############################################################################
# Surgical Type
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


class SurgicalTypeMention(SpanAugmentedMention):
    surgical_type: SurgicalType = Field(
        default=SurgicalType.NOT_MENTIONED,
        description="High-level categorization of the surgery (biopsy, resection, ablation, etc.)",
    )


###############################################################################
# Extent of Resection
###############################################################################
class SurgicalExtentOfResection(StrEnum):
    GROSS_TOTAL = "GROSS_TOTAL"
    SUBTOTAL = "SUBTOTAL"
    PARTIAL = "PARTIAL"
    BIOPSY_ONLY = "BIOPSY_ONLY"
    SUPRATOTAL = "SUPRATOTAL"  # used in LGG for seizure control
    NOT_APPLICABLE = "NOT_APPLICABLE"
    UNRESECTABLE = "UNRESECTABLE"
    NOT_MENTIONED = "NOT_MENTIONED"


class SurgicalExtentOfResectionMention(SpanAugmentedMention):
    extent_of_resection: SurgicalExtentOfResection = Field(
        default=SurgicalExtentOfResection.NOT_MENTIONED,
        description="Reported extent of tumor resection.",
    )


###############################################################################
# Surgical Approach
###############################################################################
class SurgicalApproach(StrEnum):
    OPEN = "OPEN"
    AWAKE = "AWAKE"
    ENDOSCOPIC = "ENDOSCOPIC"
    STEREOTACTIC = "STEREOTACTIC"
    LASER = "LASER"
    KEYHOLE = "KEYHOLE"
    NOT_MENTIONED = "NOT_MENTIONED"


class SurgicalApproachMention(SpanAugmentedMention):
    approach: SurgicalApproach = Field(
        default=SurgicalApproach.NOT_MENTIONED,
        description="Technical approach used during the surgery (open, awake, stereotactic, laser).",
    )


###############################################################################
# Annotation BaseModel
###############################################################################
class GliomaSurgicalAnnotation(BaseModel):
    surgical_type_mention: SurgicalTypeMention
    approach_mention: SurgicalApproachMention
    extent_of_resection_mention: SurgicalExtentOfResectionMention
