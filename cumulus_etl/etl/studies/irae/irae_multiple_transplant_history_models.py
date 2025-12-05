from pydantic import BaseModel, Field

from cumulus_etl.etl.studies.irae.irae_base_models import SpanAugmentedMention


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
# Aggregated Annotation and Mention Classes
#
# This is the top-level structure for the pydantic models used in IRAE tasks.
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
