from enum import StrEnum

from pydantic import BaseModel, Field

from cumulus_etl.etl.studies.irae.irae_base_models import SpanAugmentedMention

###############################################################################
# Donor Characteristics
#
# For a given transplant, these should be static over time
###############################################################################


class Serostatus(StrEnum):
    """
    Serostatus classification based on IgG serology or explicit seropositive/seronegative statements.
    - SEROPOSITIVE: Documented IgG positive / seropositive
    - SERONEGATIVE: Documented IgG negative / seronegative
    - NOT_MENTIONED: No serostatus documentation found
    """

    SEROPOSITIVE = "SEROPOSITIVE"
    SERONEGATIVE = "SERONEGATIVE"
    NOT_MENTIONED = "NOT_MENTIONED"


class SerostatusDonorMention(SpanAugmentedMention):
    """
    Overall serostatus of the donor at the time of first renal transplant.
    """

    serostatus: Serostatus = Field(
        Serostatus.NOT_MENTIONED,
        description=(
            "Overall serostatus of the renal donor for ANY virus. "
            "Set SEROPOSITIVE if the note documents the donor as seropositive for "
            "ANY virus (including CMV, EBV, HBV, HCV, HSV, VZV, or an unspecified virus), "
            "or uses generic language such as 'donor is seropositive' without naming the virus. "
            "Set SERONEGATIVE only if it explicitly states the donor was seronegative for all IgG tests. "
            "Otherwise use NOT_MENTIONED."
        ),
    )


class SerostatusDonorCMVMention(SpanAugmentedMention):
    """
    CMV serostatus of the donor at the time of first renal transplant.
    """

    serostatus: Serostatus = Field(
        Serostatus.NOT_MENTIONED,
        description=(
            "CMV serostatus of the renal donor. Choose one of: 'SEROPOSITIVE', 'SERONEGATIVE', 'NOT_MENTIONED'. "
            "Look for patterns like 'CMV D+', 'CMV D-', 'donor CMV IgG positive', 'donor CMV IgG negative', etc."
        ),
    )


class SerostatusDonorEBVMention(SpanAugmentedMention):
    """
    EBV serostatus of the donor at the time of first renal transplant.
    """

    serostatus: Serostatus = Field(
        Serostatus.NOT_MENTIONED,
        description=(
            "EBV serostatus of the renal donor. Choose one of: 'SEROPOSITIVE', 'SERONEGATIVE', 'NOT_MENTIONED'. "
            "Look for patterns like 'EBV D+', 'EBV D-', 'donor EBV IgG positive', 'donor EBV IgG negative', etc."
        ),
    )


class SerostatusRecipientMention(SpanAugmentedMention):
    """
    Overall serostatus of the recipient at the time of first renal transplant.
    """

    serostatus: Serostatus = Field(
        Serostatus.NOT_MENTIONED,
        description=(
            "Serostatus of the recipient at the time of renal transplant for ANY virus. "
            "Set SEROPOSITIVE if the note documents the recipient as seropositive for "
            "ANY virus (including CMV, EBV, HBV, HCV, HSV, VZV, or an unspecified virus), "
            "or if the text states 'recipient is seropositive' without naming the virus. "
            "Set SERONEGATIVE only if explicitly stated the recipient was seronegative. "
            "Otherwise use NOT_MENTIONED."
        ),
    )


class SerostatusRecipientCMVMention(SpanAugmentedMention):
    """
    CMV serostatus of the recipient at the time of first renal transplant.
    """

    serostatus: Serostatus = Field(
        Serostatus.NOT_MENTIONED,
        description=(
            "CMV serostatus of the recipient at the time of renal transplant. "
            "Choose one of: 'SEROPOSITIVE', 'SERONEGATIVE', 'NOT_MENTIONED'. "
            "Look for patterns like 'CMV R+', 'CMV R-', 'recipient CMV IgG positive', 'recipient CMV IgG negative', etc."
        ),
    )


class SerostatusRecipientEBVMention(SpanAugmentedMention):
    """
    EBV serostatus of the recipient at the time of first renal transplant.
    """

    serostatus: Serostatus = Field(
        Serostatus.NOT_MENTIONED,
        description=(
            "EBV serostatus of the recipient at the time of renal transplant. "
            "Choose one of: 'SEROPOSITIVE', 'SERONEGATIVE', 'NOT_MENTIONED'. "
            "Look for patterns like 'EBV R+', 'EBV R-', 'recipient EBV IgG positive', 'recipient EBV IgG negative', etc."
        ),
    )


# Dates are treated as strings - no enum needed
class DonorTransplantDateMention(SpanAugmentedMention):
    """
    Date of the first renal transplant. If there is only one transplant mentioned,
    assume that this is the first transplant. If a POD (post-operative day) is mentioned,
    use that to infer the date.
    """

    donor_transplant_date: str | None = Field(
        None,
        description="Exact date of first renal transplant; use YYYY-MM-DD format in your response. Only highlight date mentions with an explicit day, month, and year (e.g. 2020-01-15). All other date mentions, or an absence of a date mention, should be indicated with None.",
    )


class DonorType(StrEnum):
    LIVING = "Donor was alive at time of renal transplant"
    DECEASED = "Donor was deceased at time of renal transplant"
    NOT_MENTIONED = "Donor was not mentioned as living or deceased"


class DonorTypeMention(SpanAugmentedMention):
    """
    Type of donor in the first renal transplant. If there is only one transplant
    mentioned, assume that this is the first transplant. Exclude donors that are only
    hypothetical or under evaluation/assessment.
    """

    donor_type: DonorType = Field(
        DonorType.NOT_MENTIONED,
        description="Was the first renal donor living at the time of renal transplant?",
    )


class DonorRelationship(StrEnum):
    RELATED = "Donor was biologically related to the renal transplant recipient"
    UNRELATED = "Donor was biologically unrelated to the renal transplant recipient"
    NOT_MENTIONED = "Donor relationship status was not mentioned"


class DonorRelationshipMention(SpanAugmentedMention):
    """
    Relatedness of the donor and recipient in the first renal transplant. If there is only one
    transplant mentioned, assume that this is the first transplant. Exclude donors that are only
    hypothetical or under evaluation/assessment.
    """

    donor_relationship: DonorRelationship = Field(
        DonorRelationship.NOT_MENTIONED,
        description="Was the first renal transplant donor biologically related to the recipient?",
    )


class DonorHlaMatchQuality(StrEnum):
    WELL = "Well matched (0-1 mismatches) OR recipient explicitly documented as not sensitized"
    MODERATE = (
        "Moderately matched (2-4 mismatches) OR recipient explicitly documented as sensitized"
    )
    POOR = "Poorly matched (5-6 mismatches) OR recipient explicitly documented as highly sensitized"
    NOT_MENTIONED = "HLA match quality not mentioned"


class DonorHlaMatchQualityMention(SpanAugmentedMention):
    """
    Human leukocyte antigen (HLA) match quality of the first renal transplant. If there is only one
    transplant mentioned, assume that this is the first transplant.
    """

    donor_hla_match_quality: DonorHlaMatchQuality = Field(
        DonorHlaMatchQuality.NOT_MENTIONED,
        description="What was the HLA match quality for the first renal transplant?",
    )


class DonorHlaMismatchCount(StrEnum):
    ZERO = "0"
    ONE = "1"
    TWO = "2"
    THREE = "3"
    FOUR = "4"
    FIVE = "5"
    SIX = "6"
    NOT_MENTIONED = "HLA mismatch count not mentioned"


class DonorHlaMismatchCountMention(SpanAugmentedMention):
    """
    Human leukocyte antigen (HLA) mismatch count for the first renal transplant. If there is only
    one transplant mentioned, assume that this is the first transplant.
    """

    donor_hla_mismatch_count: DonorHlaMismatchCount = Field(
        DonorHlaMismatchCount.NOT_MENTIONED,
        description="What was the donor-recipient HLA mismatch count for the first renal transplant?",
    )


###############################################################################
# Aggregated Annotation and Mention Classes
#
# This is the top-level structure for the pydantic models used in IRAE tasks.
###############################################################################


class KidneyTransplantDonorGroupAnnotation(BaseModel):
    """
    An object-model for annotations of immune related adverse event (IRAE)
    observations found in a patient's chart, relating specifically to kidney
    transplants.
    Take care to avoid false positives, like confusing information that only
    appears in family history for patient history. Annotations should indicate
    the relevant details of the finding, as well as some additional evidence
    metadata to validate findings post-hoc.
    """

    donor_transplant_date_mention: DonorTransplantDateMention
    donor_type_mention: DonorTypeMention
    donor_relationship_mention: DonorRelationshipMention
    donor_hla_match_quality_mention: DonorHlaMatchQualityMention
    donor_hla_mismatch_count_mention: DonorHlaMismatchCountMention
    donor_serostatus_mention: SerostatusDonorMention
    donor_serostatus_cmv_mention: SerostatusDonorCMVMention
    donor_serostatus_ebv_mention: SerostatusDonorEBVMention
    recipient_serostatus_mention: SerostatusRecipientMention
    recipient_serostatus_cmv_mention: SerostatusRecipientCMVMention
    recipient_serostatus_ebv_mention: SerostatusRecipientEBVMention
