from enum import Enum
from base import FHIR, ClinicalTerm, BodySite


class Procedure(ClinicalTerm):
    def __init__(self, code=None, display=None, bodysite=None, category=None):
        """
        :param code: FHIR.ProcedureCode
        :param display:
        :param bodysite: BodySite
        :param category: ProcedureCategory
        """
        self.system = FHIR.Procedure
        self.code = code
        self.display = display
        self.bodysite = bodysite
        self.category = category


class ProcedureCategory(Enum):
    psych = ('24642003', 'Psychiatry procedure or service')
    counselling = ('409063005','Counselling')
    education = ('409073007','Education')
    surgical = ('387713003','Surgical procedure')
    diagnostic = ('103693007','Diagnostic procedure')
    chiro = ('46947000','Chiropractic manipulation')
    social = ('410606002','Social service procedure')

    def __init__(self, code, display):
        self.system = FHIR.ProcedureCategory
        self.code = code
        self.display = display

