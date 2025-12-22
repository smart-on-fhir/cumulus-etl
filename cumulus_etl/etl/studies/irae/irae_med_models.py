from enum import StrEnum

from pydantic import BaseModel, Field

from cumulus_etl.etl.studies.irae.irae_base_models import SpanAugmentedMention

GTS_SYSTEM = "http://terminology.hl7.org/CodeSystem/v3-GTSAbbreviation"


###############################################################################
# Timing related to MedicationRequest.frequency


class RxFrequency(StrEnum):
    QD = "QD"  # 1X (once daily)
    BID = "BID"  # 2X (twice daily)
    TID = "TID"  # 3X (three times daily)
    QID = "QID"  # 4X (four times daily)
    QOD = "QOD"  # 1/2X (every other day)
    Q6H = "Q6H"  # 4X (every 6 hours)
    Q8H = "Q8H"  # 3X (every 8 hours)
    Q12H = "Q12H"  # 2X (every 12 hours)
    WEEKLY = "WEEKLY"  # 1/7X (once every 7 days)
    Q2W = "Q2W"  # 1/14X (once every 2 weeks = 14 days)
    Q4W = "Q4W"  # 1/28X (once every 4 weeks = 28 days)
    MONTHLY = "MONTHLY"  # 1/28X (once every 4 weeks = 28 days)
    OTHER = "OTHER"  # use timing_text
    NONE = "None of the above"


###############################################################################
# MedicationRequest.status


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


###############################################################################
# MedicationRequest.category


class RxCategory(StrEnum):
    """
    https://build.fhir.org/valueset-medicationrequest-admin-location.html
    """

    INPATIENT = "Medication ordered/administered during an inpatient/acute care setting"
    OUTPATIENT = "Medication ordered/administered during an outpatient setting"
    COMMUNITY = "Medication ordered/consumed by the patient in their home (including long term care, nursing homes, etc)"
    NONE = "None of the above"


###############################################################################
# MedicationRequest.route


class RxRoute(StrEnum):
    """
    Route of Administration can "help" (but not deterministic) for drug metadata, examples
    * Injection --> antibody for induction/rescue therapy
    * Topical --> skin lesions
    * Inhalation --> Steroid
    https://build.fhir.org/valueset-route-codes.html
    """

    PO = "Oral (includes swallowed and sublingual routes)"
    NG = "Nasogastric/Feeding tube (NG/PEG)"
    INJECTION = "Injection (IV, SC, or IM)"
    INHALATION = "Inhalation (respiratory route)"
    TOPICAL = "Topical (skin or mucosal surface)"
    NONE = "None of the above"


###############################################################################
# MedicationRequest.dispenseRequest
#
# MedicationRequest.dispenseRequest.validityPeriod


class RxExpectedSupplyDaysMention(SpanAugmentedMention):
    """
    http://hl7.org/fhir/us/core/STU4/StructureDefinition-us-core-medicationrequest-definitions.html#MedicationRequest.dispenseRequest.expectedSupplyDuration
    """

    expected_supply_days: int | None = Field(
        default=None,
        description="Number of days the medication supply is supposed to last (stale dating the prescription)",
    )


# MedicationRequest.dispenseRequest.expectedSupplyDuration
class RxValidityPeriodMention(SpanAugmentedMention):
    """
    http://hl7.org/fhir/us/core/STU4/StructureDefinition-us-core-medicationrequest-definitions.html#MedicationRequest.dispenseRequest.validityPeriod
    """

    start_date: str | None = Field(
        default=None, description="Start date of the prescribed or administered medication"
    )

    end_date: str | None = Field(
        default=None, description="End date of the prescribed or administered medication"
    )


class RxQuantityUnit(StrEnum):
    # Mass
    MG = "mg"
    G = "g"
    UG = "ug"  # microgram (mcg)
    KG = "kg"

    # Volume
    ML = "mL"
    L = "L"

    # International Units
    U = "U"
    IU = "[iU]"

    # Countable units
    TABLET = "{tablet}"
    CAPSULE = "{capsule}"
    PUFF = "{puff}"
    PATCH = "{patch}"
    SUPPOSITORY = "{suppository}"

    # Ratios
    MG_PER_ML = "mg/mL"
    MG_PER_KG = "mg/kg"
    U_PER_KG = "U/kg"
    UG_PER_KG_PER_MIN = "ug/kg/min"

    # Time units (for infusion rates)
    H = "h"
    MIN = "min"
    D = "d"
    NONE = "None of the above"


# MedicationRequest.dispenseRequest.quantity
class RxQuantity(SpanAugmentedMention):
    """
    http://hl7.org/fhir/us/core/STU4/StructureDefinition-us-core-medicationrequest-definitions.html#MedicationRequest.dispenseRequest.quantity
    """

    unit: RxQuantityUnit = Field(
        default=RxQuantityUnit.NONE,
        description="Medication prescribed unit (examples: 'mg', 'ug/kg/min', 'tablet', etc)",
    )

    value: str | None = Field(
        default=None,
        description="Numeric amount of medication prescribed or administered (FHIR Quantity.value)",
    )


###############################################################################
# Treatment Phase


class TreatmentPhase(StrEnum):
    """
    Treatment Phase
    """

    INDUCTION = "Induction therapy"
    MAINTENANCE = "Maintenance therapy"
    RESCUE = "Rescue therapy"
    NONE = "None of the above"


###############################################################################
# helper: Describe Drug Type (class or therapy modality) or drug ingredient.


def drug_type_field(default=None, drug_type=None) -> str | None:
    return Field(
        default=default,
        description=f"Extract the {drug_type} class or therapy modality documented for this medication, if present",
    )


def ingredient_field(default=None, ingredient=None) -> str | None:
    return Field(
        default=default,
        description=f"Extract the {ingredient} ingredient documented for this medication, if present",
    )


###############################################################################
# Base class for Medication Mentions


class MedicationMention(SpanAugmentedMention):
    """
    https://build.fhir.org/valueset-medicationrequest-status.html
    https://build.fhir.org/valueset-medicationrequest-admin-location.html
    http://hl7.org/fhir/us/core/STU4/StructureDefinition-us-core-medicationrequest-definitions.html#MedicationRequest.dispenseRequest.expectedSupplyDuration
    http://hl7.org/fhir/us/core/STU4/StructureDefinition-us-core-medicationrequest-definitions.html#MedicationRequest.dispenseRequest.validityPeriod
    http://hl7.org/fhir/us/core/STU4/StructureDefinition-us-core-medicationrequest-definitions.html#MedicationRequest.dispenseRequest.numberOfRepeatsAllowed
    http://hl7.org/fhir/us/core/STU4/StructureDefinition-us-core-medicationrequest-definitions.html#MedicationRequest.dispenseRequest.quantity
    """

    status: RxStatus = Field(
        default=RxStatus.NONE, description="What is the status of this medication?"
    )

    category: RxCategory = Field(
        default=RxCategory.NONE,
        description="In which healthcare setting is this medication prescribed/administered?",
    )

    route: RxRoute = Field(
        default=RxRoute.NONE,
        description="What is the the route of administration for this medication?",
    )

    phase: TreatmentPhase = Field(
        default=TreatmentPhase.NONE, description="What is the treatment phase for this medication?"
    )

    expected_supply_days: int | None = Field(
        default=None,
        description="Number of days the medication supply is supposed to last (stale dating the prescription)",
    )

    number_of_repeats_allowed: int | None = Field(
        default=None,
        description="number of times (aka refills or repeats) that the patient can receive the prescribed medication",
    )

    frequency: RxFrequency = Field(
        default=RxFrequency.NONE, description="What is the frequency of this medication?"
    )

    start_date: str | None = Field(
        None, description="Start date of the prescribed or administered medication"
    )

    end_date: str | None = Field(
        None, description="End date of the prescribed or administered medication"
    )

    quantity_unit: RxQuantityUnit = Field(
        RxQuantityUnit.NONE, description="Medication prescribed unit"
    )

    quantity_value: str | None = Field(
        None,
        description="Numeric amount of medication prescribed or administered (FHIR Quantity.value)",
    )


##########################################################
#
#           Immunosuppression ** DRUG CLASS **
#
##########################################################
class RxClassImmunosuppression(StrEnum):
    """
    RxClass Immunosuppression
    """

    ANTIMET = "Anti-Metabolite (ANTIMET)"
    CNI = "Calcineurin Inhibitor (CNI)"
    STEROID = "Corticosteroid (CS)"
    MTOR = "mTOR Inhibitor (MTOR)"
    COSTIM = "Costimulation Blocker/blockade (COSTIM)"
    IVIG = "Immunoglobulin (IVIG)"
    POLYCLONAL = "Polyclonal antibody (e.g., ATG, ALG)"
    MONOCLONAL = "Monoclonal antibody (mAb, e.g., basiliximab, rituximab)"
    OTHER = "Other immunosuppressive drug"
    NONE = "None of the above"


class RxIngredientImmunosuppression(StrEnum):
    """
    The specific immunosuppressive drug ingredient
    """

    # ANTIMET Types
    AZA = "Azathioprine"
    MMF = "Mycophenolate Mofetil"
    ANTIMET_OTHER = "Other anti-metabolite ingredient"
    # CNI Types
    CYA = "Cyclosporine"
    TAC = "Tacrolimus"
    CNI_OTHER = "Other calcineurin inhibitor ingredient"
    # STEROID Types
    MEDROL = "Methylprednisolone"
    PDL = "Prednisolone"
    PRED = "Prednisone"
    STEROID_OTHER = "Other corticosteroid ingredient"
    # COSTIM Types
    BEL = "Belatacept"
    ABA = "Abatacept"
    COSTIM_OTHER = "Other Costimulation blocker ingredient"
    # IVIG Types
    IVIG = "Intravenous Immunoglobulin (IVIG)"
    CYTOGAM = "Cytogam (CMV-specific hyperimmune globulin)"
    IVIG_OTHER = "Other immunoglobulin therapy"
    # MTOR Types
    EVE = "Everolimus"
    SRL = "Sirolimus"
    MTOR_OTHER = "Other mTOR inhibitor ingredient"
    # MONOCLONAL Types
    ALEM = "Alemtuzumab"
    BASI = "Basiliximab"
    DAC = "Daclizumab"
    RTX = "Rituximab"
    MONOCLONAL_OTHER = "Other Monoclonal antibody drug"
    # POLYCLONAL Types
    ATG = "Antithymocyte Globulin (ATG)"
    POLYCLONAL_OTHER = "Other polyclonal antibodies ingredient"
    NONE = "None of the above"


class ImmunosuppressiveMedicationMention(MedicationMention):
    """
    Mentions of ImmunosuppressiveMedications for this given chart.
    """

    drug_class: RxClassImmunosuppression = drug_type_field(
        default=RxClassImmunosuppression.NONE,
        drug_type="Immunosuppressive drug class or therapy modality",
    )

    ingredient: RxIngredientImmunosuppression = ingredient_field(
        default=RxIngredientImmunosuppression.NONE,
        ingredient="Specific immunosuppressive drug ingredient",
    )


##############################################################################
# Aggregated Annotation and Mention Classes
#
# This is the top-level structure for the pydantic models used in IRAE tasks.
###############################################################################


class ImmunosuppressiveMedicationsAnnotation(BaseModel):
    """
    All mentions of ImmunosuppressiveMedications for this given chart.
    """

    immunosuppressive_medication_mentions: list[ImmunosuppressiveMedicationMention] = Field(
        default_factory=list,
        description="All mentions of ImmunosuppressiveMedications for this given chart.",
    )
