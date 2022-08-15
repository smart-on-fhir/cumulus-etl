from enum import Enum
from base import FHIR, Coding


class Medication(Coding):
    def __init__(self, code=None, display=None, category=None):
        self.system = FHIR.Medication
        self.code = code
        self.display = display
        self.category = category


class MedicationForm(Coding):

    def __init__(self, code=None, display=None):
        self.system = FHIR.MedicationForm
        self.code = code
        self.display = display


class MedicationCategory(Enum):
    inpatient = ('inpatient', 'Includes orders for medications to be administered or consumed in an inpatient or acute care setting')
    outpatient = ('outpatient', 'Includes orders for medications to be administered or consumed in an inpatient or acute care setting')
    community = ('community', 'Includes orders for medications to be administered or consumed by the patient in their home')
    patientspecified = ('patientspecified', 'Patient Specified')

    def __init__(self, code, display=None):
        self.system = FHIR.MedicationCategory
        self.code = code
        self.display = display

