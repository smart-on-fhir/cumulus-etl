from enum import StrEnum

from pydantic import BaseModel, Field

from cumulus_etl.etl.studies.glioma.glioma_base_models import SpanAugmentedMention


###############################################################################
# Age at Diagnosis
###############################################################################
class AgeAtDiagnosisMention(SpanAugmentedMention):
    age_years: int | None = Field(
        None, description="Age at glioma diagnosis in years, if explicitly stated."
    )


###############################################################################
# Tumor Location
###############################################################################
class TumorLocation(StrEnum):
    NOT_MENTIONED = "NOT_MENTIONED"
    CEREBELLUM = "CEREBELLUM"
    BRAINSTEM = "BRAINSTEM"
    OPTIC_PATHWAY = "OPTIC_PATHWAY"
    HYPOTHALAMUS = "HYPOTHALAMUS"
    OPTIC_PATHWAY_HYPOTHALAMIC = "OPTIC_PATHWAY_HYPOTHALAMIC"
    THALAMUS = "THALAMUS"
    OTHER = "OTHER"


class TumorLocationMention(SpanAugmentedMention):
    location: TumorLocation = Field(
        TumorLocation.NOT_MENTIONED, description="Primary tumor anatomic location."
    )


###############################################################################
# Tumor Region
###############################################################################
class TumorRegion(StrEnum):
    NOT_MENTIONED = "NOT_MENTIONED"
    POSTERIOR_FOSSA = "POSTERIOR_FOSSA"
    DIENCEPHALIC = "DIENCEPHALIC"
    CEREBRAL_HEMISPHERE = "CEREBRAL_HEMISPHERE"
    SPINAL = "SPINAL"
    OTHER = "OTHER"


class TumorRegionMention(SpanAugmentedMention):
    region: TumorRegion = Field(
        TumorRegion.NOT_MENTIONED,
        description="Tumor region or anatomic compartment (e.g., posterior fossa, diencephalic).",
    )


###############################################################################
# Tumor Mass Effect
###############################################################################
class TumorMassEffect(StrEnum):
    NOT_MENTIONED = "NOT_MENTIONED"
    NONE = "NONE"
    MASS_EFFECT_PRESENT = "MASS_EFFECT_PRESENT"
    HYDROCEPHALUS = "HYDROCEPHALUS"
    MIDLINE_SHIFT = "MIDLINE_SHIFT"
    IMPENDING_HERNIATION = "IMPENDING_HERNIATION"
    OTHER = "OTHER"


class TumorSizeMassEffectMention(SpanAugmentedMention):
    mass_effect: TumorMassEffect = Field(
        TumorMassEffect.NOT_MENTIONED,
        description="Whether MRI/CT describes mass effect or hydrocephalus.",
    )
    size_text: str | None = Field(
        None, description="Free-text size description (e.g., '3.2 x 2.1 x 2.4 cm') if present."
    )


###############################################################################
# Topography
###############################################################################
class TopographyMention(SpanAugmentedMention):
    """
    ICD-O Oncology topography for a cancer lesion, plus whether this site
    represents the primary tumor or a metastatic tumor.
    """

    code: str | None = Field(
        default=None,
        pattern=r"^C\d{2}(\.\d)?$",
        description=(
            "ICD-O topography code (Cxx or Cxx.x) for the anatomic site of the malignant neoplasm, "
            "e.g., C18.0 for cecum."
        ),
    )
    display: str | None = Field(
        None,
        description="Human-readable ICD-O anatomic site description corresponding to the topography code.",
    )


###############################################################################
# Histology
###############################################################################
class Histology(StrEnum):
    NOT_MENTIONED = "NOT_MENTIONED"
    PILOCYTIC_ASTROCYTOMA = "PILOCYTIC_ASTROCYTOMA"
    PILOMYXOID_ASTROCYTOMA = "PILOMYXOID_ASTROCYTOMA"
    DIFFUSE_ASTROCYTOMA = "DIFFUSE_ASTROCYTOMA"
    GANGLIOGLIOMA = "GANGLIOGLIOMA"
    DNET = "DNET"  # Dysembryoplastic neuroepithelial tumor (often in DDx)
    OTHER_LGG = "OTHER_LGG"
    UNKNOWN = "UNKNOWN"


class HistologyMention(SpanAugmentedMention):
    histology: Histology = Field(
        Histology.NOT_MENTIONED, description="Histologic subtype if stated."
    )


###############################################################################
# Morphology
###############################################################################
class MorphologyMention(SpanAugmentedMention):
    """
    ICD-O Oncology Morphology (Histology + Behavior).

    Morphology represents the microscopic tumor cell type (e.g., adenocarcinoma,
    squamous cell carcinoma, lymphoma) combined with the behavior code.
    """

    code: str | None = Field(
        default=None,
        pattern=r"^\d{4}/[0-9]$",
        description=(
            "ICD-O morphology code (M-####/x). Example: 8140/3 = Adenocarcinoma, NOS (malignant)."
        ),
    )
    display: str | None = Field(
        None, description="Human-readable ICD-O histologic type (e.g., 'Adenocarcinoma, NOS')."
    )


###############################################################################
# Behavior
###############################################################################
class ICDOBehaviorCode(StrEnum):
    BENIGN = "/0"
    UNCERTAIN = "/1"
    IN_SITU = "/2"
    MALIGNANT_PRIMARY = "/3"
    MALIGNANT_METASTATIC = "/6"
    MALIGNANT_RECURRENT = "/9"


class BehaviorMention(SpanAugmentedMention):
    code: ICDOBehaviorCode | None = Field(
        None, description="ICD-O slash behavior code (e.g., /3 = malignant primary site)."
    )
    display: str | None = Field(
        None, description="Human-readable description of the cancer behavior."
    )


###############################################################################
# Grade
###############################################################################
class GradeCode(StrEnum):
    GRADE_I = "1"  # Well differentiated
    GRADE_II = "2"  # Moderately differentiated
    GRADE_III = "3"  # Poorly differentiated
    GRADE_IV = "4"  # Undifferentiated / Anaplastic
    GRADE_UNKNOWN = "9"  # Not determined / Cannot be assessed


class GradeMention(SpanAugmentedMention):
    """
    ICD-O Oncology Grade (Tumor cell differentiation grade).

    Grade reflects how closely the tumor cells resemble normal tissue:
      1 = Well differentiated
      2 = Moderately differentiated
      3 = Poorly differentiated
      4 = Undifferentiated / Anaplastic
      9 = Grade cannot be assessed
    """

    code: GradeCode | None = Field(
        None, description="ICD-O tumor differentiation grade (1, 2, 3, 4, or 9)."
    )
    display: str | None = Field(
        None, description="Human-readable ICD-O description of the tumor grade."
    )


###############################################################################
# Neurofibromatosis 1
###############################################################################
class NF1Status(StrEnum):
    NOT_MENTIONED = "NOT_MENTIONED"
    POSITIVE = "POSITIVE"
    SUSPECTED = "SUSPECTED"
    NEGATIVE = "NEGATIVE"
    MENTIONED_BUT_UNKNOWN = "MENTIONED_BUT_UNKNOWN"


class NF1StatusMention(SpanAugmentedMention):
    nf1_status: NF1Status = Field(NF1Status.NOT_MENTIONED, description="Whether NF1 is present.")


###############################################################################
# Annotation BaseModel
###############################################################################
class GliomaDiagnosisAnnotation(BaseModel):
    age_at_diagnosis_mention: AgeAtDiagnosisMention
    tumor_location_mention: TumorLocationMention
    tumor_region_mention: TumorRegionMention
    tumor_size_mention: TumorSizeMassEffectMention
    topography_mention: TopographyMention
    morphology_mention: MorphologyMention
    behavior_mention: BehaviorMention
    grade_mention: GradeMention
    nf1_status_mention: NF1StatusMention
