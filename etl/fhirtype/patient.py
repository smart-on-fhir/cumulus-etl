from enum import Enum
from base import FHIR, Coding


class Patient:

    def __init__(self, age=None, gender=None, race=None, ethnicity=None, zipcode=None):
        """
        :param age: Range
        :param gender: Gender
        :param race: Race
        :param ethnicity: Ethnicity
        :param zipcode: str
        """
        self.system = FHIR.Patient
        self.age = age
        self.gender = gender
        self.race = race
        self.ethnicity = ethnicity
        self.zipcode = zipcode


class Age:
    def __init__(self, code: range, display=None):
        self.system = FHIR.Range
        self.code = code
        self.display = display if display else 'Patient Age'


class Gender(Enum):
    """
    http://hl7.org/fhir/codesystem-gender-identity.html
    http://hl7.org/fhir/patient.html#gender
    http://hl7.org/fhir/valueset-administrative-gender.html
    """
    male = ('male', 'M')
    female = ('female', 'F')
    trans_female = ('transgender-female', 'T')
    trans_male = ('transgender-male', 'T')
    non_binary = ('non-binary', 'U')
    non_disclose = ('non-disclose', 'U')
    other = ('other', 'U')
    unknown = ('unknown', 'U')

    def __init__(self, code=None, display=None):
        self.system = FHIR.Gender
        self.code = code
        self.display = display if display else code


class Race(Enum):
    """
    Race coding has 5 "root" levels, called the R5 shown below.
    http://hl7.org/fhir/r4/v3/Race/cs.html
    """
    native = ('1002-5', 'American Indian or Alaska Native')
    asian  = ('2028-9', 'Asian')
    black = ('2054-5', 'Black or African American')
    pacific= ('2076-8', 'Native Hawaiian or Other Pacific Islander')
    white = ('2106-3', 'White')

    def __init__(self, code, display=None):
        self.system = FHIR.Race
        self.code=code
        self.display = display


class Ethnicity(Enum):
    """
    RWD usually has only this binary YES/NO hispanic or latino feature populated.
    For a complete list of codes, see Ethnicity.system.
    """
    hispanic_or_latino = ('2135-2', 'Hispanic or Latino	Hispanic or Latino')
    not_hispanic_or_latino = ('2186-5','Not Hispanic or Latino')

    def __init__(self, code=None, display=None):
        self.system = FHIR.Ethnicity
        self.code = code
        self.display = display
