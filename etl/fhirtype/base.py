from datetime import date
from datetime import timedelta
from enum import Enum


class FHIR(Enum):
    """
    FHIR Conformance (partial conformance towards USCDI)
    """
    Population = 'http://hl7.org/fhir/population-definitions.html'
    Patient = 'http://hl7.org/fhir/patient.html'
    Gender = 'http://hl7.org/fhir/ValueSet/administrative-gender'
    Race = 'http://hl7.org/fhir/us/core/StructureDefinition/us-core-race'
    Ethnicity = 'http://hl7.org/fhir/us/core/ValueSet/detailed-ethnicity'
    ObservationCode = 'http://hl7.org/fhir/ValueSet/observation-codes'
    ObservationCategory = 'http://hl7.org/fhir/ValueSet/observation-category'
    ObservationValue = 'http://hl7.org/fhir/observation-definitions.html#Observation.value_x_'
    ObservationInterpretation = 'http://hl7.org/fhir/ValueSet/observation-interpretation'
    VitalSign = 'http://hl7.org/fhir/observation-vitalsigns.html'
    Encounter = 'http://hl7.org/fhir/encounter.html'
    EncounterPeriod = 'http://hl7.org/fhir/encounter-definitions.html#Encounter.period'
    EncounterStatus = 'http://hl7.org/fhir/encounter-status'
    EncounterCode = 'http://hl7.org/fhir/v3/ActEncounterCode/vs.html'
    Hospitalization = 'http://hl7.org/fhir/encounter-definitions.html#Encounter.hospitalization'
    AdmitSource = 'http://hl7.org/fhir/ValueSet/encounter-admit-source'
    DischargeDisposition = 'http://hl7.org/fhir/ValueSet/encounter-discharge-disposition'
    ServiceType = 'http://hl7.org/fhir/ValueSet/service-type'
    ClinicalTerm = 'http://snomed.info/sct'
    BodySite = 'http://hl7.org/fhir/ValueSet/body-site'
    Finding = 'http://hl7.org/fhir/ValueSet/clinical-findings'
    Condition = 'http://hl7.org/fhir/condition-definitions.html'
    ConditionCode = 'http://hl7.org/fhir/ValueSet/condition-code'
    ConditionCategory = 'http://hl7.org/fhir/ValueSet/condition-category'
    Severity = 'http://hl7.org/fhir/ValueSet/condition-severity'
    Procedure = 'http://hl7.org/fhir/procedure.html'
    ProcedureCode = 'http://hl7.org/fhir/ValueSet/procedure-code'
    ProcedureCategory = 'http://hl7.org/fhir/ValueSet/procedure-category'
    Medication = 'http://www.nlm.nih.gov/research/umls/rxnorm'
    MedicationForm = 'http://hl7.org/fhir/ValueSet/medication-form-codes'
    MedicationCategory = 'http://hl7.org/fhir/ValueSet/medication-statement-category'
    VaccineCode = 'http://hl7.org/fhir/ValueSet/vaccine-code'
    Immunization = 'http://hl7.org/fhir/immunization.html'

    # basic datatypes
    Coding = 'http://hl7.org/fhir/datatypes.html#Coding'
    Range = 'https://hl7.org/fhir/datatypes.html#Range'
    Period = 'http://hl7.org/fhir/datatypes.html#Period'
    Duration = 'http://hl7.org/fhir/datatypes.html#Duration'
    DurationUnits = 'http://hl7.org/fhir/valueset-duration-units.html'
    Units = 'http://hl7.org/fhir/ValueSet/ucum-units'
    Date = 'https://www.hl7.org/fhir/datatypes.html#date'
    PostalCode = 'http://hl7.org/fhir/datatypes-definitions.html#Address.postalCode'
    Count = 'http://hl7.org/fhir/search.html#count'
    Int = 'http://hl7.org/fhir/datatypes.html#positiveInt'

    COVID19_Symptoms = 'http://build.fhir.org/ig/HL7/fhir-COVID19Library-ig/ValueSet-covid19-signs-1nd-symptoms-value-set.html'

    # UMLS other common Vocabs
    UMLS = 'http://www.nlm.nih.gov/research/umls/'
    LOINC = 'http://loinc.org'
    ICD9 = 'http://hl7.org/fhir/sid/icd-9-cm'
    ICD10 = 'http://hl7.org/fhir/sid/icd-10-cm'
    CPT = 'http://www.ama-assn.org/go/cpt'


class DurationUnits(Enum):
    milliseconds = ('ms', 'milliseconds')
    seconds = ('s', 'seconds')
    minutes = ('min', 'minutes')
    hours = ('h', 'hours')
    days = ('d', 'days')
    weeks = ('wk', 'weeks')
    months = ('mo', 'months')
    years = ('a', 'years')

    def __init__(self, code, display=None):
        self.system = FHIR.DurationUnits.value
        self.code = code
        self.display = display


class Period:
    def __init__(self, start=None, end=None):
        """
        :param start: date
        :param end: date
        """
        self.system = FHIR.Period
        self.start = start
        self.end = end


class Range:
    def __init__(self, low=None, high=None):
        self.system = FHIR.Range
        self.low = low
        self.high = high


class Coding:
    def __init__(self, code=None, display=None):
        self.system = FHIR.Coding
        self.code = code
        self.display = display


class ClinicalTerm(Coding):
    def __init__(self, code=None, display=None):
        self.system = FHIR.ClinicalTerm
        self.code = code
        self.display = display


class BodySite(Coding):
    def __init__(self, code=None, display=None):
        self.system = FHIR.BodySite
        self.code = code
        self.display = display


class UMLS(Coding):
    """
    https://www.ncbi.nlm.nih.gov/books/NBK9685/table/ch03.T.concept_names_and_sources_file_mr/
    """
    def __init__(self, code=None, display=None, sab='http://www.nlm.nih.gov/research/umls/sourcereleasedocs/'):
        self.system = FHIR.UMLS
        self.code = code
        self.display = display
        self.sab = sab


class ICD10(Coding):
    """
    https://www.hl7.org/fhir/icd.html
    """
    def __init__(self, code=None, display=None):
        self.system = FHIR.ICD10
        self.code = code
        self.display = display


class ICD9(Coding):
    """
    https://www.hl7.org/fhir/icd.html
    """
    def __init__(self, code=None, display=None):
        self.system = FHIR.ICD9
        self.code = code
        self.display = display
