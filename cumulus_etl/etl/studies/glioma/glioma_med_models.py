from enum import StrEnum

from pydantic import BaseModel, Field

from cumulus_etl.etl.studies.glioma.glioma_base_models import SpanAugmentedMention


###############################################################################
# Generic Medication Attributes/Models
###############################################################################
#
###############################################################################
# FHIR MedicationRequest.status
class RxStatus(StrEnum):
    """
    Medication Status (including Intent because chart review is NOT always identical to Med Request)
    https://build.fhir.org/valueset-medicationrequest-status.html
    https://build.fhir.org/valueset-medicationrequest-intent.html
    """

    ACTIVE = "Medication order is active (currently prescribed and intended for ongoing use)."
    INTENDED = "Medication is planned/ordered/prescribed but therapy has not yet started."
    COMPLETED = "Medication course is finished (all doses given or intended duration completed)."
    STOPPED = "Medication was stopped or permanently discontinued before completion."
    CANCELED = "Medication order was canceled/withdrawn before any doses were administered."
    ON_HOLD = "Medication is temporarily paused (on-hold, suspended, or interrupted)."
    NONE = "None of the above"
    NOT_MENTIONED = "Medication status was not mentioned."


class RxStatusMention(SpanAugmentedMention):
    rx_status: RxStatus = Field(
        RxStatus.NOT_MENTIONED, description="what is the status of this medication?"
    )


###############################################################################
# Treatment Phase
class RxTreatmentPhase(StrEnum):
    """
    Treatment Phase
    """

    NOT_MENTIONED = "NOT_MENTIONED"
    INDUCTION = "Induction therapy"
    MAINTENANCE = "Maintenance therapy"
    RESCUE = "Rescue therapy"
    NONE = "None of the above"


class RxTreatmentPhaseMention(SpanAugmentedMention):
    treatment_phase: RxTreatmentPhase = Field(
        default=RxTreatmentPhase.NOT_MENTIONED, description="Treatment phase"
    )


###############################################################################
# Drug Response
###############################################################################
class RxResponse(StrEnum):
    NOT_MENTIONED = "NOT_MENTIONED"
    COMPLETE_RESPONSE = "COMPLETE_RESPONSE"
    PARTIAL_RESPONSE = "PARTIAL_RESPONSE"
    MINOR_RESPONSE = "MINOR_RESPONSE"  # commonly used in LGG trials
    STABLE_DISEASE = "STABLE_DISEASE"
    PROGRESSIVE_DISEASE = "PROGRESSIVE_DISEASE"
    MIXED_RESPONSE = "MIXED_RESPONSE"
    PSEUDOPROGRESSION = "PSEUDOPROGRESSION"
    NOT_EVALUABLE = "NOT_EVALUABLE"


class RxResponseMention(SpanAugmentedMention):
    response: RxResponse = Field(RxResponse.NOT_MENTIONED, description="Response to therapy.")

    response_confidence: int | None = Field(
        None, description="LLM confidence score (0-100) if used."
    )

    response_days: int | None = Field(
        None, description="Days from therapy start to first response."
    )

    duration_on_therapy_days: int | None = Field(
        None, description="Total duration of exposure in days."
    )

    duration_of_response_days: int | None = Field(
        None, description="Duration response was maintained."
    )


###############################################################################
# Therapy Discontinued
###############################################################################
class RxDiscontinued(StrEnum):
    NOT_MENTIONED = "NOT_MENTIONED"
    PROGRESSION = "PROGRESSION"
    TOXICITY = "TOXICITY"
    LACK_OF_RESPONSE = "LACK_OF_RESPONSE"
    COMPLETED_PLANNED_THERAPY = "COMPLETED_PLANNED_THERAPY"
    PATIENT_PREFERENCE = "PATIENT_PREFERENCE"
    TRANSITION_TO_TRIAL = "TRANSITION_TO_TRIAL"
    OTHER = "OTHER"


class RxDiscontinuedMention(SpanAugmentedMention):
    discontinued: RxDiscontinued = Field(
        RxDiscontinued.NOT_MENTIONED, description="Therapy discontinued reason"
    )


###############################################################################
# Drug Toxicity
###############################################################################
class RxToxicitySeverity(StrEnum):
    NOT_MENTIONED = "NOT_MENTIONED"
    MILD = "MILD"
    MODERATE = "MODERATE"
    SEVERE = "SEVERE"
    DOSE_LIMITING = "DOSE_LIMITING"
    OTHER = "OTHER"


class RxToxicitySeverityMention(SpanAugmentedMention):
    severity: RxToxicitySeverity = Field(
        RxToxicitySeverity.NOT_MENTIONED, description="Overall toxicity severity as described."
    )


###############################################################################
# Glioma Targeted Therapy
###############################################################################
class GliomaTargetedTherapy(StrEnum):
    NOT_MENTIONED = "NOT_MENTIONED"
    MEK_INHIBITOR = "MEK_INHIBITOR"
    BRAF_INHIBITOR = "BRAF_INHIBITOR"
    BRAF_MEK_COMBINATION = "BRAF_MEK_COMBINATION"
    PAN_RAF_INHIBITOR = "PAN_RAF_INHIBITOR"
    FGFR_INHIBITOR = "FGFR_INHIBITOR"
    NTRK_INHIBITOR = "NTRK_INHIBITOR"
    ALK_INHIBITOR = "ALK_INHIBITOR"
    ROS1_INHIBITOR = "ROS1_INHIBITOR"
    RET_INHIBITOR = "RET_INHIBITOR"
    MTOR_INHIBITOR = "MTOR_INHIBITOR"
    IDH_INHIBITOR = "IDH_INHIBITOR"
    OTHER = "OTHER"


class GliomaTargetedTherapyMention(SpanAugmentedMention):
    targeted_therapy: GliomaTargetedTherapy = Field(
        GliomaTargetedTherapy.NOT_MENTIONED, description="Glioma targeted therapy"
    )

    status: RxStatus = Field(
        RxStatus.NOT_MENTIONED,
        description="Glioma targeted therapy status (active/completed/stopped/etc)",
    )

    treatment_phase: RxTreatmentPhase = Field(
        RxTreatmentPhase.NOT_MENTIONED, description="Glioma targeted therapy treatment phase"
    )

    treatment_response: RxResponse = Field(
        RxResponse.NOT_MENTIONED,
        description="What was the glioma targeted therapy treatment response?",
    )

    treatment_discontinued: RxDiscontinued = Field(
        RxDiscontinued.NOT_MENTIONED,
        description="Was glioma targeted therapy discontinued, and why?",
    )

    toxicity_severity: RxToxicitySeverity = Field(
        RxToxicitySeverity.NOT_MENTIONED,
        description="What was the toxicity severity of glioma targeted therapy?",
    )


###############################################################################
# Glioma Traditional Chemotherapy
###############################################################################
class GliomaChemotherapyClass(StrEnum):
    NOT_MENTIONED = "NOT_MENTIONED"
    ALKYLATING_AGENT = "ALKYLATING_AGENT"  # e.g., temozolomide, lomustine (protocol-dependent)
    PLATINUM_AGENT = "PLATINUM_AGENT"  # e.g., carboplatin, cisplatin
    VINCA_ALKALOID = "VINCA_ALKALOID"  # e.g., vincristine, vinblastine
    TOP1_INHIBITOR = "TOP1_INHIBITOR"  # e.g., irinotecan
    ANTIMETABOLITE = "ANTIMETABOLITE"  # e.g., methotrexate (less common, context-dependent)
    MULTI_AGENT_CHEMOTHERAPY = (
        "MULTI_AGENT_CHEMOTHERAPY"  # protocol bundle when class not decomposed
    )
    OTHER = "OTHER"


class GliomaChemotherapyRegimen(StrEnum):
    NOT_MENTIONED = "NOT_MENTIONED"
    CARBOPLATIN = "CARBOPLATIN"
    CARBOPLATIN_VINCRISTINE = "CARBOPLATIN_VINCRISTINE"
    VINBLASTINE = "VINBLASTINE"
    TPCV = "TPCV"  # thioguanine/procarbazine/CCNU/vincristine (legacy)
    BEVACIZUMAB_IRINOTECAN = "BEVACIZUMAB_IRINOTECAN"
    BEVACIZUMAB = "BEVACIZUMAB"
    OTHER = "OTHER"


class GliomaChemotherapyMention(SpanAugmentedMention):
    chemotherapy_class: GliomaChemotherapyClass = Field(
        GliomaChemotherapyClass.NOT_MENTIONED, description="Glioma chemotherapy drug class"
    )

    chemotherapy_regimen: GliomaChemotherapyRegimen = Field(
        GliomaChemotherapyRegimen.NOT_MENTIONED, description="Glioma chemotherapy regimen"
    )

    status: RxStatus = Field(
        RxStatus.NOT_MENTIONED,
        description="Glioma chemotherapy status (active/completed/stopped/etc)",
    )

    treatment_phase: RxTreatmentPhase = Field(
        RxTreatmentPhase.NOT_MENTIONED, description="Glioma chemotherapy treatment phase"
    )

    treatment_response: RxResponse = Field(
        RxResponse.NOT_MENTIONED, description="What was the glioma chemotherapy response?"
    )

    toxicity_severity: RxToxicitySeverity = Field(
        RxToxicitySeverity.NOT_MENTIONED,
        description="What was the toxicity severity of glioma chemotherapy?",
    )

    treatment_discontinued: RxDiscontinued = Field(
        RxDiscontinued.NOT_MENTIONED, description="Was glioma chemotherapy discontinued?"
    )


###############################################################################
# Glioma Medications --> Annotation BaseModel (Chemo + Targeted Therapy)
###############################################################################
class GliomaMedicationsAnnotation(BaseModel):
    chemotherapy_mention: list[GliomaChemotherapyMention] = Field(
        default_factory=list, description="All mentions of pLGG chemotherapy"
    )
    targeted_therapy_mention: list[GliomaTargetedTherapyMention] = Field(
        default_factory=list, description="All mentions of glioma targeted therapy"
    )
