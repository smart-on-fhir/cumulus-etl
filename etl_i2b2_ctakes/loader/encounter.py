from enum import Enum
from base import FHIR


class EncounterStatus(Enum):

    planned = ('planned', 'The Encounter has not yet started')
    arrived = ('arrived', 'Patient is present for the encounter, however is not currently meeting with a practitioner')
    triaged = ('triaged', 'assessed for the priority of their treatment based on the severity of their condition')
    progress = ('in-progress', 'Encounter has begun and  patient is present /  practitioner and patient are meeting')
    onleave = ('onleave', 'Encounter has begun, but the patient is temporarily on leave')
    finished = ('finished', 'Encounter has ended')
    cancelled = ('cancelled', 'Encounter has ended before it has begun')
    error = ('entered-in-error', 'instance should not have been part of this patient medical record')
    unknown = ('unknown', 'unknown is last resort and every attempt should be made to provide a meaningful value')

    def __init__(self, code, display):
        self.system = FHIR.EncounterCode.value
        self.code = code
        self.display = display


class EncounterCode(Enum):
    AMB = ('AMB', 'ambulatory')
    EMER = ('EMER', 'emergency')
    FLD = ('FLD', 'field')
    HH = ('HH', 'home health')
    IMP = ('IMP', 'inpatient encounter')
    ACUTE = ('ACUTE', 'inpatient acute')
    NONAC = ('NONAC', 'inpatient non-acute')
    OBSENC = ('OBSENC', 'observation encounter')
    PRENC = ('PRENC', 'pre-admission')
    SS = ('SS', 'short stay')
    VR = ('VR', 'virtual')

    def __init__(self, code, display=None):
        self.system = FHIR.EncounterCode.value
        self.code = code
        self.display = display


class ServiceType(Enum):
    """
    ADT messages from "HL7v2 interfaces" often include the ServiceType.
    Below is a partial list of services applicable to CDC cumulus / infectious disease monitoring.

    @see FHIR.ServiceType
    """
    referral = ('425', 'Referral')
    inpatients = ('557', 'Inpatients')
    respiratory = ('430', 'Respiratory')
    infectious = ('173', 'Infectious Diseases')
    immunology = ('172', 'Immunology & Allergy')
    immunization = ('57', 'Immunization')
    maternal = ('58', 'Maternal')
    babies = ('257', 'Babies')
    gynaecology = ('481', 'Gynaecology')
    sex_transmit = ('441', 'Sexually Transmitted Diseases')
    sex_std = ('450', 'STD')
    sex_sti = ('451', 'STI')
    drug_council = ('105', 'Drug and/or Alcohol Counselling')
    drug_referral = ('106', 'Drug/Alcohol Information/Referral')
    drug_needles = ('107', 'Needle & Syringe Exchange')
    drug_methadone = ('386', 'Methadone')
    cardiology = ('165', 'Cardiology')
    diabetes = ('318', 'Diabetes')
    icu = ('174', 'Intensive Care Medicine')
    nursing = ('59', 'Nursing')
    med = ('382', 'Medical Services')
    surgery = ('221', 'Surgery - General')
    injury = ('371', 'Injury')
    neurology = ('177', 'Neurology')
    neuro_surgery = ('216', 'Neurosurgery')
    cardiothoracic_surgery = ('215', 'Cardiothoracic Surgery')
    vascular_surgery = ('223', 'Vascular Surgery')
    """
    @see  FHIR.ServiceType
    """
    def __init__(self, code, display=None):
        self.system = FHIR.ServiceType.value
        self.code = code
        self.display = display


class AdmitSource(Enum):

    hosp_transfer = ('hosp-trans', 'transferred from another hospital for this encounter')
    emergency = ('emd', 'From accident/emergency department')
    outp = ('outp', 'From outpatient department')
    born = ('born', 'Born in hospital')
    referral_gp = ('gp', 'General Practitioner referral')
    referral_mp = ('mp', 'Medical Practitioner/physician referral')
    nursing = ('nursing', 'From nursing home')
    psych = ('psych', 'From psychiatric hospital')
    rehab = ('rehab', 'From rehabilitation facility')
    other = ('other', 'Other: admitted from a source otherwise not specified')

    def __init__(self, code, display=None):
        self.system = FHIR.AdmitSource.value
        self.code = code
        self.display = display


class DischargeDisposition(Enum):

    home = ('home', 'Home')
    alt_home = ('alt-home', 'Alternative home')
    other_hcf = ('other-hcf', 'Other healthcare facility')
    hosp = ('hosp', 'Hospice')
    long = ('long', 'Long-term care')
    ama = ('aadvice', 'Left against advice')
    exp = ('exp', 'Expired')
    psych = ('psy', 'Psychiatric hospital')
    rehab = ('rehab', 'Rehabilitation')
    snf = ('snf', 'Skilled nursing facility')
    other = ('oth', 'Other: discharge disposition has not otherwise defined')

    def __init__(self, code, display=None):
        self.system = FHIR.DischargeDisposition.value
        self.code = code
        self.display = display


class Encounter:
    def __init__(self, period=None, duration=None, hospitalization=None, code=None, service=None, status=None):
        """
        :param period: FHIR.Period
        :param duration: timedelta
        :param hospitalized: boolean
        :param code: FHIR.EncounterCode
        :param service: FHIR.EncounterStatus
        """
        self.period = period
        self.duration = duration
        self.hospitalization = hospitalization
        self.code = code
        self.service = service
        self.status = status
