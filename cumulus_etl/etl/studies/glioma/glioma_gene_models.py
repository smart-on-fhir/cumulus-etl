from enum import StrEnum

from pydantic import BaseModel, Field

from cumulus_etl.etl.studies.glioma.glioma_base_models import SpanAugmentedMention

###############################################################################
# Clinical recommendations:
#   https://www.nice.org.uk/guidance/ng99/chapter/Recommendations
#
# NCBI Genetic Testing Registry
#   https://www.ncbi.nlm.nih.gov/gtr/conditions/C0017638/
###############################################################################


###############################################################################
# Molecular Driver
###############################################################################
class MolecularDriverMention(SpanAugmentedMention):
    """
    Minimal set of clinically actionable genetic alterations for glioma.
    """

    # --- Core actionable alteration ---
    braf_altered: bool | None = Field(None, description="BRAF alteration is present (any type).")

    braf_v600e: bool | None = Field(None, description="BRAF V600E mutation is present.")

    braf_fusion: bool | None = Field(
        None, description=" BRAF fusion (e.g., KIAA1549-BRAF) is present."
    )

    idh_mutant: bool | None = Field(None, description="IDH1 or IDH2 mutation is present.")

    h3k27m_mutant: bool | None = Field(
        None, description="Histone H3 K27M (H3-3A or H3C2) mutation is present."
    )

    tp53_altered: bool | None = Field(None, description="TP53 mutation or loss is present.")

    # --- Copy number / pathway surrogates ---
    cdkn2a_deleted: bool | None = Field(None, description="CDKN2A deletion is present.")

    nf1_mapk_activation: bool | None = Field(None, description="NF1 MAPK activation is present.")

    other_raf_alteration: bool | None = Field(None, description="Other RAF alteration is present.")

    fgfr_alteration: bool | None = Field(None, description="FGFR alteration is present.")

    ntrk_fusion: bool | None = Field(None, description="NTRK fusion is present.")

    alk_fusion: bool | None = Field(None, description="ALK fusion is present.")

    ros1_fusion: bool | None = Field(None, description="ROS1 fusion is present.")


###############################################################################
# Genetic Variants
###############################################################################
class GeneticVariantInterpretation(StrEnum):
    B = "BENIGN"
    LB = "LIKELY BENIGN"
    VUS = "VARIANT OF UNKNOWN SIGNIFICANCE"
    P = "PATHOGENIC"
    LP = "LIKELY PATHOGENIC"
    NOT_MENTIONED = "NOT MENTIONED"


class GeneticVariantMention(SpanAugmentedMention):
    """
    Clinical interpretation of genetic variant
    """

    hgnc_name: str | None = Field(default=None, description="HGNC/HUGO gene naming convention")

    interpretation: GeneticVariantInterpretation = Field(
        GeneticVariantInterpretation.NOT_MENTIONED,
        description="Clinical interpretation of genetic variant or genetic test result",
    )

    hgvs_variant: str | None = Field(
        None, description="HGVS variant string (e.g., NM_004333.6(BRAF):c.1799T>A)."
    )


###############################################################################
# Annotation BaseModel
###############################################################################
class GliomaGeneAnnotation(BaseModel):
    molecular_driver_mention: list[MolecularDriverMention] = Field(
        default_factory=list, description="All mentions of pLGG Molecular drivers"
    )

    genetic_variant_mention: list[GeneticVariantMention] = Field(
        default_factory=list, description="All mentions of pLGG genetic variants"
    )
