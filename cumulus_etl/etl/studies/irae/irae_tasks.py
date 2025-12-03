"""Define tasks for the irae study"""

import datetime
import logging
from collections.abc import Generator, Iterator
from enum import StrEnum

import cumulus_fhir_support as cfs
from pydantic import BaseModel, Field

from cumulus_etl import common, nlp, store
from cumulus_etl.etl import tasks


class SpanAugmentedMention(BaseModel):
    has_mention: bool = Field(
        False, description="Whether there is any mention of this variable in the text."
    )
    spans: list[str] = Field(
        default_factory=list, description="The text spans where this variable is mentioned."
    )


###############################################################################
# History of Multiple Transplants
#
# Mentions relevant in tracking if this patient has a history of multiple transplants,
# renal or otherwise.
###############################################################################
class MultipleTransplantHistoryMention(SpanAugmentedMention):
    """
    Does this patient have a history of multiple transplants, renal or otherwise?
    For use in reevaluating the patients in our cohort, excluding patients with a history
    of multiple transplants from our analysis.
    """

    multiple_transplant_history: bool = Field(
        False, description="Whether there is any mention of a history of multiple transplants."
    )


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
            "If the patient is deceased, include the date the patient became deceased. Use YYYY-MM-DD format if possible. "
            "Use None if there is no date recorded or if the patient is not observed as deceased."
        ),
    )


###############################################################################
# Aggregated Annotation and Mention Classes
###############################################################################
class MultipleTransplantHistoryAnnotation(BaseModel):
    """
    An object-model for annotations of patients with a history of multiple transplants.
    Take care to avoid false positives, like confusing information that only
    appears in family history for patient history. Annotations should indicate
    the relevant details of the finding, as well as some additional evidence
    metadata to validate findings post-hoc.
    """

    multiple_transplant_history_mention: MultipleTransplantHistoryMention


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


class KidneyTransplantLongitudinalAnnotation(BaseModel):
    """
    An object-model for annotations of immune related adverse event (IRAE)
    observations found in a patient's chart, relating specifically to kidney
    transplants.

    This class only includes longitudinally variable mentions, i.e. those
    that can change over time, such as therapeutic status, compliance, infections,
    graft rejection/failure, DSA, PTLD, cancer, and deceased status.

    Take care to avoid false positives, like confusing information that only
    appears in family history for patient history. Annotations should indicate
    the relevant details of the finding, as well as some additional evidence
    metadata to validate findings post-hoc.
    """

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


class BaseIraeTask(tasks.BaseModelTaskWithSpans):
    task_version = 6
    # Task Version History:
    # ** 6 (2025-11): Pydantic updates (donors refer to 1st transplant;
    #                 POD inference guidance; new multiple transplant task) **
    # ** 5 (2025-10): Update pydantic model (biological relation;
    #                 Defaults for SpanAugmentedMention properties) **
    # ** 4 (2025-10): Split into donor & longitudinal models **
    # ** 3 (2025-10): New serialized format **
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
        "Pydantic Schema:\n"
        "%JSON-SCHEMA%"
    )
    user_prompt = (
        "Evaluate the following clinical document for kidney transplant variables and outcomes.\n"
        "Here is the clinical document for you to analyze:\n"
        "\n"
        "%CLINICAL-NOTE%"
    )


class BaseMultipleTransplantHistoryIraeTask(BaseIraeTask):
    response_format = MultipleTransplantHistoryAnnotation


class BaseDonorIraeTask(BaseIraeTask):
    response_format = KidneyTransplantDonorGroupAnnotation


class BaseLongitudinalIraeTask(BaseIraeTask):
    response_format = KidneyTransplantLongitudinalAnnotation

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.subject_refs_to_skip = set()

    @staticmethod
    def ndjson_in_order(input_root: store.Root, resource: str) -> Generator[dict]:
        # To avoid loading all the notes into memory, we'll first go through each note, and keep
        # track of their byte offset on disk and their date. Then we'll grab each from disk in
        # order.

        # Get a list of all files we're going to be working with here
        filenames = common.ls_resources(input_root, {resource})

        # Go through all files, keeping a record of each line's dates and offsets.
        note_info = []
        for file_index, path in enumerate(filenames):
            for row in cfs.read_multiline_json_with_details(path, fsspec_fs=input_root.fs):
                date = nlp.get_note_date(row["json"]) or datetime.datetime.max
                if not date.tzinfo:  # to compare, we need everything to be aware
                    date = date.replace(tzinfo=datetime.UTC)
                note_info.append((date, file_index, row["byte_offset"]))

        # Now yield each note again in order, reading each from disk
        note_info.sort()
        for _date, file_index, offset in note_info:
            rows = cfs.read_multiline_json_with_details(
                filenames[file_index],
                offset=offset,
                fsspec_fs=input_root.fs,
            )
            # StopIteration errors shouldn't happen here, because we just went through these
            # files above, but just to be safe, we'll gracefully intercept it.
            try:
                yield next(rows)["json"]
            except StopIteration:  # pragma: no cover
                logging.warning(
                    f"File '{filenames[file_index]}' changed while reading, skipping some notes."
                )
                continue

    # Override the read-from-disk portion, so we can order notes in oldest-to-newest order
    def read_ndjson_from_disk(self, input_root: store.Root, resource: str) -> Iterator[dict]:
        yield from self.ndjson_in_order(input_root, resource)

    def should_skip(self, orig_note: dict) -> bool:
        subject_ref = nlp.get_note_subject_ref(orig_note)
        return subject_ref in self.subject_refs_to_skip or super().should_skip(orig_note)


class IraeMultipleTransplantHistoryGpt4oTask(BaseMultipleTransplantHistoryIraeTask):
    name = "irae__nlp_multiple_transplant_history_gpt4o"
    client_class = nlp.Gpt4oModel


class IraeDonorGpt4oTask(BaseDonorIraeTask):
    name = "irae__nlp_donor_gpt4o"
    client_class = nlp.Gpt4oModel


class IraeLongitudinalGpt4oTask(BaseLongitudinalIraeTask):
    name = "irae__nlp_gpt4o"
    client_class = nlp.Gpt4oModel


class IraeMultipleTransplantHistoryGpt5Task(BaseMultipleTransplantHistoryIraeTask):
    name = "irae__nlp_multiple_transplant_history_gpt5"
    client_class = nlp.Gpt5Model


class IraeDonorGpt5Task(BaseDonorIraeTask):
    name = "irae__nlp_donor_gpt5"
    client_class = nlp.Gpt5Model


class IraeLongitudinalGpt5Task(BaseLongitudinalIraeTask):
    name = "irae__nlp_gpt5"
    client_class = nlp.Gpt5Model


class IraeMultipleTransplantHistoryGptOss120bTask(BaseMultipleTransplantHistoryIraeTask):
    name = "irae__nlp_multiple_transplant_history_gpt_oss_120b"
    client_class = nlp.GptOss120bModel


class IraeDonorGptOss120bTask(BaseDonorIraeTask):
    name = "irae__nlp_donor_gpt_oss_120b"
    client_class = nlp.GptOss120bModel


class IraeLongitudinalGptOss120bTask(BaseLongitudinalIraeTask):
    name = "irae__nlp_gpt_oss_120b"
    client_class = nlp.GptOss120bModel


class IraeMultipleTransplantHistoryLlama4ScoutTask(BaseMultipleTransplantHistoryIraeTask):
    name = "irae__nlp_multiple_transplant_history_llama4_scout"
    client_class = nlp.Llama4ScoutModel


class IraeDonorLlama4ScoutTask(BaseDonorIraeTask):
    name = "irae__nlp_donor_llama4_scout"
    client_class = nlp.Llama4ScoutModel


class IraeLongitudinalLlama4ScoutTask(BaseLongitudinalIraeTask):
    name = "irae__nlp_llama4_scout"
    client_class = nlp.Llama4ScoutModel


class IraeMultipleTransplantHistoryClaudeSonnet45Task(BaseMultipleTransplantHistoryIraeTask):
    name = "irae__nlp_multiple_transplant_history_claude_sonnet45"
    client_class = nlp.ClaudeSonnet45Model


class IraeDonorClaudeSonnet45Task(BaseDonorIraeTask):
    name = "irae__nlp_donor_claude_sonnet45"
    client_class = nlp.ClaudeSonnet45Model


class IraeLongitudinalClaudeSonnet45Task(BaseLongitudinalIraeTask):
    name = "irae__nlp_claude_sonnet45"
    client_class = nlp.ClaudeSonnet45Model
