from enum import Enum
from base import FHIR, ClinicalTerm


class Severity(Enum):
    severe = ('24484000', 'Severe')
    moderate= ('6736007', 'Moderate')
    mild = ('255604002', 'Mild')

    def __init__(self, code, display=None):
        self.system = FHIR.Severity
        self.code = code
        self.display = display


class ConditionCategory(Enum):

    problem_list = ('problem-list-item', 'problem list (history of and current problems)')
    encounter_dx = ('encounter-diagnosis', 'Problem at the time of the encounter')

    def __init__(self, code: str, display=None):
        self.system = FHIR.ConditionCategory
        self.code = code
        self.display = display


class Condition(ClinicalTerm):
    """
    See also https://www.hl7.org/fhir/codesystem.html#hierarchy
    """
    def __init__(self, code=None, display=None, bodysite=None, severity=None, category=None, status=None):
        """
        :param code:
        :param display:
        :param bodysite: FHIR.BodySite
        :param severity: FHIR.Severity
        :param status:
        """
        self.system = FHIR.ClinicalTerm
        self.code= code
        self.display = display
        self.bodysite= bodysite
        self.severity = severity
        self.category = category
        self.status = status

        self.library = dict()

