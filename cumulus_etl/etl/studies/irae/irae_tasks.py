"""Define tasks for the irae study"""

import json
from enum import StrEnum

from pydantic import BaseModel, Field

from cumulus_etl import nlp
from cumulus_etl.etl import tasks


class SpanAugmentedMention(BaseModel):
    has_mention: bool | None  # True, False, or None
    spans: list[str]


###############################################################################
# Donor Characteristics
#
# For a given transplant, these should be static over time
###############################################################################


# Dates are treated as strings - no enum needed
class DonorTransplantDateMention(SpanAugmentedMention):
    donor_transplant_date: str | None = Field(None, description="Date of renal transplant")


class DonorType(StrEnum):
    LIVING = "Donor was alive at time of renal transplant"
    DECEASED = "Donor was deceased at time of renal transplant"
    NOT_MENTIONED = "Donor was not mentioned as living or deceased"


class DonorTypeMention(SpanAugmentedMention):
    donor_type: DonorType = Field(
        DonorType.NOT_MENTIONED, description="Was the renal donor living at the time of transplant?"
    )


class DonorRelationship(StrEnum):
    RELATED = "Donor was related to the renal transplant recipient"
    UNRELATED = "Donor was unrelated to the renal transplant recipient"
    NOT_MENTIONED = "Donor relationship status was not mentioned"


class DonorRelationshipMention(SpanAugmentedMention):
    donor_relationship: DonorRelationship = Field(
        DonorRelationship.NOT_MENTIONED, description="Was the renal donor related to the recipient?"
    )


class DonorHlaMatchQuality(StrEnum):
    WELL = "Well matched (0-1 mismatches)"
    MODERATE = "Moderately matched (2-4 mismatches)"
    POOR = "Poorly matched (5-6 mismatches)"
    NOT_MENTIONED = "HLA match quality not mentioned"


class DonorHlaMatchQualityMention(SpanAugmentedMention):
    donor_hla_match_quality: DonorHlaMatchQuality = Field(
        DonorHlaMatchQuality.NOT_MENTIONED,
        description="What was the renal transplant HLA match quality?",
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
    donor_hla_mismatch_count: DonorHlaMismatchCount = Field(
        DonorHlaMismatchCount.NOT_MENTIONED,
        description="What was the renal donor-recipient HLA mismatch count?",
    )


###############################################################################
# Therapeutic Status Compliance
###############################################################################
class RxTherapeuticStatus(StrEnum):
    THERAPEUTIC = (
        "Immunosuppression levels are documented as therapeutic, adequate, or within target range."
    )
    SUB_THERAPEUTIC = "Immunosuppression levels are documented as subtherapeutic, insufficient, or below target range."
    SUPRA_THERAPEUTIC = "Immunosuppression levels are documented as supratherapeutic, above therapeutic level, or above target range."
    NONE_OF_THE_ABOVE = "None of the above"


class RxTherapeuticStatusMention(SpanAugmentedMention):
    rx_therapeutic_status: RxTherapeuticStatus = Field(
        RxTherapeuticStatus.NONE_OF_THE_ABOVE,
        description="In the present encounter, what is the documented immunosuppression level?",
    )


###############################################################################
# Medication Compliance
###############################################################################
class RxCompliance(StrEnum):
    COMPLIANT = "Patient is documented as compliant with immunosuppressive medications."
    PARTIALLY_COMPLIANT = (
        "Patient is documented as only partially compliant with immunosuppressive medications."
    )
    NON_COMPLIANT = "Patient is documented as noncompliant with immunosuppressive medications."
    NONE_OF_THE_ABOVE = "None of the above"


class RxComplianceMention(SpanAugmentedMention):
    rx_compliance: RxCompliance = Field(
        RxCompliance.NONE_OF_THE_ABOVE,
        description="In the present encounter, is the patient documented as compliant with immunosuppressive medications?",
    )


###############################################################################
# THE FOLLOWING DATA ELEMENTS TRACK BOTH
# THE HISTORY AND THE PRESENT STATUS OF VARIABLES
###############################################################################


###############################################################################
# DSA Donor Specific Antibody
###############################################################################
class DSAPresent(StrEnum):
    """
    Notice: DSA is strongly related to `GraftRejectionPresent`.

    Treatment of DSA includes immunosuppressive drugs, IVIG, and plasmapheresis (PLEX).

    Treatment with immunosuppressive drugs does *NOT* imply SUSPECTED DSA,
    as many of immunosuppressive drugs are routinely used for "maintenance" therapy.

    IVIG and plasmapheresis (PLEX) during the post-transplant (post induction) phase DOES imply
    --> DSAPresent >  SUSPECTED (and possibly CONFIRMED)
    --> `GraftRejectionPresent` > SUSPECTED (and possibly CONFIRMED or BIOPSY_PROVEN)
    """

    CONFIRMED = "DSA diagnostic test positive, DSA diagnosis 'confirmed' or 'positive', or increase in immunosuppression due to DSA"
    SUSPECTED = "DSA suspected, DSA likely, DSA cannot be ruled out, DSA test result pending, or treatment with IVIG/plasmapheresis"
    NONE_OF_THE_ABOVE = "None of the above"


class DSAMention(SpanAugmentedMention):
    dsa_history: bool = Field(
        False,
        description="Does the patient have a past medical history of donor specific antibodies (DSA)?",
    )
    dsa: DSAPresent = Field(
        DSAPresent.NONE_OF_THE_ABOVE,
        description="What evidence documents donor specific antibodies (DSA) as current, active, or being evaluated/treated now?",
    )


############################################################################################################
# Infection (** Any **)
#   * necessary for PNA and UTI infections that often do not have a confirmed infection type!
#   * useful as a secondary check to ensure more specific infection types are not missed
############################################################################################################
class InfectionPresent(StrEnum):
    CONFIRMED = "Infection confirmed by laboratory test or imaging, infection diagnosis was 'confirmed' or 'positive', or reduced immunosuppression due to infection"
    SUSPECTED = "Infection is suspected, likely, cannot be ruled out, infection is a differential diagnosis or infectious test result is pending"
    NONE_OF_THE_ABOVE = "None of the above"


class InfectionMention(SpanAugmentedMention):
    infection_history: bool = Field(
        False, description="Does the patient have a past medical history of an infection?"
    )
    infection: InfectionPresent = Field(
        InfectionPresent.NONE_OF_THE_ABOVE,
        description="What evidence documents infection as current, active, or being evaluated/treated now?",
    )


###############################################################################
# Infection (Viral)
###############################################################################
class ViralInfectionPresent(StrEnum):
    CONFIRMED = "Viral infection confirmed by laboratory test or imaging, viral infection diagnosis was 'confirmed' or 'positive', or reduced immunosuppression due to viral infection"
    SUSPECTED = "Viral infection is suspected, likely, cannot be ruled out, viral infection is a differential diagnosis or viral test result is pending"
    NONE_OF_THE_ABOVE = "None of the above"


class ViralInfectionMention(SpanAugmentedMention):
    viral_infection_history: bool = Field(
        False, description="Does the patient have a past medical history of a viral infection?"
    )
    viral_infection: ViralInfectionPresent = Field(
        ViralInfectionPresent.NONE_OF_THE_ABOVE,
        description="What evidence documents viral infection as current, active, or being evaluated/treated now?",
    )


###############################################################################
# Infection (Bacterial)
###############################################################################
class BacterialInfectionPresent(StrEnum):
    CONFIRMED = "Bacterial infection confirmed by laboratory test or imaging, bacterial infection diagnosis was 'confirmed' or 'positive', or reduced immunosuppression due to bacterial infection"
    SUSPECTED = "Bacterial infection is suspected, likely, cannot be ruled out, bacterial infection is a differential diagnosis or bacterial test result is pending"
    NONE_OF_THE_ABOVE = "None of the above"


class BacterialInfectionMention(SpanAugmentedMention):
    bacterial_infection_history: bool = Field(
        False, description="Does the patient have a past medical history of a bacterial infection?"
    )
    bacterial_infection: BacterialInfectionPresent = Field(
        BacterialInfectionPresent.NONE_OF_THE_ABOVE,
        description="What evidence documents bacterial infection as current, active, or being evaluated/treated now?",
    )


###############################################################################
# Infection (Fungal)
###############################################################################
class FungalInfectionPresent(StrEnum):
    CONFIRMED = "Fungal infection confirmed by laboratory test or imaging, fungal infection diagnosis was 'confirmed' or 'positive', or reduced immunosuppression due to fungal infection"
    SUSPECTED = "Fungal infection is suspected, likely, cannot be ruled out, fungal infection is a differential diagnosis or fungal test result is pending"
    NONE_OF_THE_ABOVE = "None of the above"


class FungalInfectionMention(SpanAugmentedMention):
    fungal_infection_history: bool = Field(
        False, description="Does the patient have a past medical history of a fungal infection?"
    )
    fungal_infection: FungalInfectionPresent = Field(
        FungalInfectionPresent.NONE_OF_THE_ABOVE,
        description="What evidence documents fungal infection as current, active, or being evaluated/treated now?",
    )


###############################################################################
# Graft Rejection
###############################################################################
class GraftRejectionPresent(StrEnum):
    """
    Notice: Graft rejection is strongly related to `DSAPresent`.

    Treatment of graft rejection includes immunosuppressive drugs, IVIG, and plasmapheresis (PLEX).

    Treatment with immunosuppressive drugs does *NOT* imply outcome is SUSPECTED,
    as many of immunosuppressive drugs are routinely used for "maintenance" therapy.

    IVIG and plasmapheresis (PLEX) during the post-transplant (post induction) phase DOES imply
    --> `GraftRejectionPresent` > SUSPECTED (and possibly CONFIRMED or BIOPSY_PROVEN)
    --> DSAPresent >  SUSPECTED (and possibly CONFIRMED)
    """

    BIOPSY_PROVEN = (
        "Biopsy proven kidney graft rejection or pathology proven kidney graft rejection"
    )
    CONFIRMED = "Kidney graft rejection was 'diagnosed', 'confirmed' or 'positive'"
    SUSPECTED = "Kidney graft rejection presumed, suspected, likely, cannot be ruled out, biopsy result pending, or treatment with IVIG/plasmapheresis"
    NONE_OF_THE_ABOVE = "None of the above"


class GraftRejectionMention(SpanAugmentedMention):
    graft_rejection_history: bool = Field(
        False, description="Does the patient have a past medical history of kidney graft rejection?"
    )
    graft_rejection: GraftRejectionPresent = Field(
        GraftRejectionPresent.NONE_OF_THE_ABOVE,
        description="What evidence documents kidney graft rejection as current, active, or being evaluated/treated now?",
    )


###############################################################################
# Graft Failure
###############################################################################
class GraftFailurePresent(StrEnum):
    CONFIRMED = "Kidney graft has failed or kidney graft loss"
    SUSPECTED = "Kidney graft failure presumed, suspected, likely, or cannot be ruled out"
    NONE_OF_THE_ABOVE = "None of the above"


class GraftFailureMention(SpanAugmentedMention):
    graft_failure_history: bool = Field(
        False, description="Does the patient have a past medical history of kidney graft failure?"
    )
    graft_failure: GraftFailurePresent = Field(
        GraftFailurePresent.NONE_OF_THE_ABOVE,
        description="What evidence documents kidney graft failure as current, active, or being evaluated/treated now?",
    )


###############################################################################
# PTLD
###############################################################################
class PTLDPresent(StrEnum):
    """
    Notice: PTLD treatments may also be used in 'rescue' therapy (DSA/graft rejection) or other cancers.
    One notable difference from other cancers (such as skin cancer) is the absence of "surgical excision" (lymphoma).
    """

    BIOPSY_PROVEN = "Biopsy proven or pathology proven PTLD"
    CONFIRMED = "PTLD was 'diagnosed', 'confirmed' or 'positive' or viral positive lymphoma"
    SUSPECTED = "PTLD presumed, suspected, likely, cannot be ruled out, PTLD biopsy result pending, or treatment with chemotherapy/radiation"
    NONE_OF_THE_ABOVE = "None of the above"


class PTLDMention(SpanAugmentedMention):
    ptld_history: bool = Field(
        False,
        description="Does the patient have a past medical history of post transplant lymphoproliferative disorder (PTLD)?",
    )
    ptld: PTLDPresent = Field(
        PTLDPresent.NONE_OF_THE_ABOVE,
        description="What evidence documents post transplant lymphoproliferative disorder (PTLD) as current, active, or being evaluated/treated now?",
    )


###############################################################################
# Cancer
###############################################################################
class CancerPresent(StrEnum):
    """
    Notice: Cancer treatments may also be used in 'rescue' therapy (DSA/graft rejection).
    PTLD is a type of cancer. PTLD is a lymphoma and thus not treated with "surgical excision".
    Skin cancer (of which there are many types carcinoma and melanoma) is treated with surgical excision.
    """

    BIOPSY_PROVEN = "Biopsy proven or pathology proven cancer"
    CONFIRMED = "Cancer was 'diagnosed', 'confirmed' or 'positive'"
    SUSPECTED = "Cancer is presumed, suspected, likely, cannot be ruled out, biopsy of any lesion, or treatment with chemotherapy/radiation"
    NONE_OF_THE_ABOVE = "None of the above"


class CancerMention(SpanAugmentedMention):
    cancer_history: bool = Field(
        False, description="Does the patient have a past medical history of cancer?"
    )
    cancer: CancerPresent = Field(
        CancerPresent.NONE_OF_THE_ABOVE,
        description="What evidence documents cancer as current, active, or being evaluated/treated now?",
    )


###############################################################################
# Deceased
#  For tracking if the patient is noted to be deceased in any notes
###############################################################################
class DeceasedMention(SpanAugmentedMention):
    deceased: bool | None = Field(
        None, description="Does the present encounter document that the patient is deceased?"
    )
    deceased_date: str | None = Field(
        None,
        description=(
            "If the patient is deceased, include the date the patient became deceased. "
            "Use None if there is no date recorded or if the patient is not observed as deceased."
        ),
    )


###############################################################################
# Aggregated Annotation and Mention Classes
###############################################################################


class KidneyTransplantAnnotation(BaseModel):
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
    rx_therapeutic_status_mention: RxTherapeuticStatusMention
    rx_compliance_mention: RxComplianceMention
    dsa_mention: DSAMention
    infection_mention: InfectionMention
    viral_infection_mention: ViralInfectionMention
    bacterial_infection_mention: BacterialInfectionMention
    fungal_infection_mention: FungalInfectionMention
    graft_rejection_mention: GraftRejectionMention
    graft_failure_mention: GraftFailureMention
    ptld_mention: PTLDMention
    cancer_mention: CancerMention
    deceased_mention: DeceasedMention


class BaseIraeTask(tasks.BaseOpenAiTaskWithSpans):
    task_version = 2
    # Task Version History:
    # ** 2 (2025-09): Updated prompt and pydantic models **
    # ** 1 (2025-08): Updated prompt **
    # ** 0 (2025-08): Initial version **

    system_prompt = (
        "You are a clinical chart reviewer for a kidney transplant outcomes study.\n"
        "Your task is to extract patient-specific information from an unstructured clinical "
        "document and map it into a predefined Pydantic schema.\n"
        "\n"
        "Core Rules:\n"
        "1. Base all assertions ONLY on patient-specific information in the clinical document.\n"
        "   - Never negate or exclude information just because it is not mentioned.\n"
        "   - Never conflate family history or population-level risk with patient findings.\n"
        "   - Do not count past medical history, prior episodes, or family history.\n"
        "2. Do not invent or infer facts beyond what is documented.\n"
        "3. Maintain high fidelity to the clinical document language when citing spans.\n"
        "4. Answer patient outcomes with strongest available documented evidence:\n"
        "    BIOPSY_PROVEN > CONFIRMED > SUSPECTED > NONE_OF_THE_ABOVE.\n"
        "5. Always produce structured JSON that conforms to the Pydantic schema provided below.\n"
        "\n"
        "Pydantic Schema:\n" + json.dumps(KidneyTransplantAnnotation.model_json_schema())
    )
    user_prompt = (
        "Evaluate the following clinical document for kidney transplant variables and outcomes.\n"
        "Here is the clinical document for you to analyze:\n"
        "\n"
        "%CLINICAL-NOTE%"
    )
    response_format = KidneyTransplantAnnotation


class IraeGpt4oTask(BaseIraeTask):
    name = "irae__nlp_gpt4o"
    client_class = nlp.Gpt4oModel


class IraeGpt5Task(BaseIraeTask):
    name = "irae__nlp_gpt5"
    client_class = nlp.Gpt5Model


class IraeGptOss120bTask(BaseIraeTask):
    name = "irae__nlp_gpt_oss_120b"
    client_class = nlp.GptOss120bModel


class IraeLlama4ScoutTask(BaseIraeTask):
    name = "irae__nlp_llama4_scout"
    client_class = nlp.Llama4ScoutModel
