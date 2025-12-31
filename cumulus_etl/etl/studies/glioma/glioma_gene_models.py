from enum import StrEnum

from pydantic import Field

from cumulus_etl.etl.studies.glioma.glioma_base_models import SpanAugmentedMention


###############################################################################
# Clinical recommendations:
#   https://www.nice.org.uk/guidance/ng99/chapter/Recommendations
#
# NCBI Genetic Testing Registry
#   https://www.ncbi.nlm.nih.gov/gtr/conditions/C0017638/
###############################################################################
class TargetGeneticTestMention(SpanAugmentedMention):
    """
    Minimal set of clinically actionable genetic alterations for glioma.
    """

    # --- Core actionable alteration ---
    braf_altered: bool | None = Field(
        None, description="Whether a BRAF alteration is present (any type)."
    )

    braf_v600e: bool | None = Field(None, description="Whether a BRAF V600E mutation is present.")

    braf_fusion: bool | None = Field(
        None, description="Whether a BRAF fusion (e.g., KIAA1549-BRAF) is present."
    )

    # --- Common glioma-defining markers ---
    idh_mutant: bool | None = Field(
        None, description="Whether an IDH1 or IDH2 mutation is present."
    )

    h3k27m_mutant: bool | None = Field(
        None, description="Whether an H3 K27M (H3-3A or H3C2) mutation is present."
    )

    # --- Tumor suppressors / pathway-level relevance ---
    tp53_altered: bool | None = Field(
        None, description="Whether TP53 mutation or loss is reported."
    )

    # --- Copy number / pathway surrogates ---
    cdkn2a_deleted: bool | None = Field(None, description="Whether CDKN2A deletion is reported.")


###############################################################################
# Genetic Variants
###############################################################################
class VariantInterpretation(StrEnum):
    B = "BENIGN"
    LB = "LIKELY BENIGN"
    VUS = "VARIANT OF UNKNOWN SIGNIFICANCE"
    P = "PATHOGENIC"
    LP = "LIKELY PATHOGENIC"
    NOT_MENTIONED = "NOT MENTIONED"


class VariantMention(SpanAugmentedMention):
    """
    Clinical interpretation of genetic variant
    """

    hgnc_name: str = Field(default=None, description="HGNC hugo gene naming convention")

    interpretation: VariantInterpretation = Field(
        VariantInterpretation.NOT_MENTIONED,
        description="Clinical interpretation of genetic variant or genetic test result",
    )

    hgvs_variant: str = Field(str, description="Human Genome Variation Society (HGVS) variant")
