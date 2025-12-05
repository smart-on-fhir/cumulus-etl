from enum import StrEnum
from pydantic import BaseModel, Field

from cumulus_etl.etl.studies.irae.irae_base_models import SpanAugmentedMention


###############################################################################
# Donor Characteristics
#
# For a given transplant, these should be static over time
###############################################################################


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
