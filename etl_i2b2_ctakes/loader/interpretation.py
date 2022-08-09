from typing import List
from enum import Enum
from base import FHIR, Coding

class ObservationInterpretationDetection(Enum):
    positive = ('POS', 'Positive')
    negative = ('NEG', 'Negative')
    indeterminate = ('IND', 'Indeterminate')
    equivocal = ('E', 'Equivocal')
    detected = ('DET', 'Detected')
    not_detected = ('ND', 'Not detected')

    def __init__(self, code, display):
        """
        Cumulus Library Note: PCR testing should use "POS", "NEG", and "IND" codes.
        http://hl7.org/fhir/R4/v3/ObservationInterpretation/cs.html#v3-ObservationInterpretation-ObservationInterpretationDetection

        Interpretations of the presence or absence of a component / analyte or organism in a test or of a sign in a clinical observation.
        In keeping with laboratory data processing practice, these concepts provide a categorical interpretation of the "meaning" of the quantitative value for the same observation.

        POS =
        A presence finding of the specified component / analyte, organism or clinical sign based on the established threshold of the performed test or procedure.

        NEG =
        An absence finding of the specified component / analyte, organism or clinical sign based on the established threshold of the performed test or procedure.

        IND =
        The specified component / analyte, organism or clinical sign could neither be declared positive / negative nor detected / not detected by the performed test or procedure.
        Usage Note: For example, if the specimen was degraded, poorly processed, or was missing the required anatomic structures, then "indeterminate" (i.e. "cannot be determined") is the appropriate response, not "equivocal".
        """
        self.system = FHIR.ObservationInterpretation
        self.code = code
        self.display = display


class GeneticObservationInterpretation(Enum):
    carrier = ('CAR', 'Carrier')

    def __init__(self, code, display):
        self.system = FHIR.ObservationInterpretation
        self.code = code
        self.display = display


class ObservationInterpretationChange(Enum):
    better = ('B', 'Better')
    down = ('D', 'Significant change down')
    up = ('U', 'Significant change up')
    worse = ('W', 'Worse')

    def __init__(self, code, display):
        self.system = FHIR.ObservationInterpretation
        self.code = code
        self.display = display


class ObservationInterpretationExceptions(Enum):
    off_scale_low = ('<', 'Off scale LOW')
    off_scale_high = ('>', 'Off scale HIGH')
    insufficient = ('IE', 'Insufficient Evidence')

    def __init__(self, code, display):
        self.system = FHIR.ObservationInterpretation
        self.code = code
        self.display = display


class ObservationInterpretationNormality(Enum):
    abnormal = ('A', 'Abnormal')
    critical = ('AA', 'Critical')
    critical_high = ('HH', 'Critical high')
    critical_low = ('LL', 'Critical low')
    high = ('H', 'High')
    high_significant = ('HU', 'Significantly high')
    low = ('L', 'Low')
    low_significant = ('LU', 'Significantly')
    normal = ('N', 'Normal')

    def __init__(self, code, display):
        self.system = FHIR.ObservationInterpretation
        self.code = code
        self.display = display

class ObservationInterpretationSusceptibility(Enum):
    intermediate = ('I', 'Intermediate')
    ncl = ('NCL', 'No CLSI defined breakpoint (Clinical and Laboratory Standards Institutes)')
    non_susceptible = ('NS', 'Non-susceptible')
    resistant = ('R', 'Resistant')
    synergy_resistant = ('SYN-R', 'Synergy')
    susceptible = ('S', 'Susceptible')
    susceptible_dose = ('SDD', 'Susceptible-dose dependent')
    synergy = ('SYN-S', 'Synergy susceptible')
    threshold_outside = ('EX', 'outside threshold')
    threshold_high = ('HX', 'above high threshold')
    threshold_low = ('LX', 'below low threshold')

    def __init__(self, code, display):
        self.system = FHIR.ObservationInterpretation
        self.code = code
        self.display = display


class ObservationInterpretationExpectation(Enum):
    expected = ('EXP', 'Expected')
    unexpected = ('UNE', 'Unexpected')

    def __init__(self, code, display):
        self.system = FHIR.ObservationInterpretation
        self.code = code
        self.display = display


class ReactivityObservationInterpretation(Enum):
    reactive = ('RR', 'Reactive')
    reactive_no = ('NR', 'Non-reactive')
    reactive_weak = ('WR', 'Weakly reactive')

    def __init__(self, code, display):
        self.system = FHIR.ObservationInterpretation
        self.code = code
        self.display = display


class ObservationInterpretation(Enum):


    def __init__(self, code, display):
        """
        Clinical significance of observation
        * GeneticObservationInterpretation
        * ObservationInterpretationChange
        * ObservationInterpretationExceptions
        * ObservationInterpretationNormality
        * ObservationInterpretationSusceptibility
        * ObservationInterpretationDetection
        * ObservationInterpretationExpectation
        * ReactivityObservationInterpretation

        :param code:
        :param display:
        :param normalcy:
        """
        self.system = FHIR.ObservationInterpretation
        self.code = code
        self.display = display

