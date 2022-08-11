from base import FHIR, Coding


class VaccineCode(Coding):
    """
    SMART Health Cards CDC (CVX) definitions of vaccine codes
    """
    def __init__(self, code, display):
        self.system = FHIR.VaccineCode
        self.code = code
        self.display = display


class Immunization(Coding):
    """
    Patient matching required (usually) to access Immunization records
    """
    def __init__(self, code, display):
        self.system = FHIR.Immunization
        self.code = code
        self.display = display

