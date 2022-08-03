import uuid
import hashlib
import i2b2

class Codebook:
    def __init__(self):
        self.phi = dict()
        self.phi['patient_num'] = dict()

    def patient(self, patient_num):
        if patient_num not in self.phi['patient_num'].keys():
            self.phi['patient_num'][patient_num] = dict()
            self.phi['patient_num'][patient_num]['deid'] = str(uuid.uuid4())
            self.phi['patient_num'][patient_num]['encounter_num'] = dict()

    def encounter(self, patient_num, encounter_num):
        self.patient(patient_num)

        if encounter_num not in self.phi['patient_num'][patient_num]['encounter_num'].keys():
            self.phi['patient_num'][patient_num]['encounter_num'][encounter_num] = dict()
            self.phi['patient_num'][patient_num]['encounter_num'][encounter_num]['deid'] = str(uuid.uuid4())
            self.phi['patient_num'][patient_num]['encounter_num'][encounter_num]['note'] = dict()

    def note(self, patient_num, encounter_num, note_hash, note_meta=None):
        self.encounter(patient_num, encounter_num)

        if note_hash not in self.phi['patient_num'][patient_num]['encounter_num'][encounter_num]['note'].keys():
            self.phi['patient_num'][patient_num]['encounter_num'][encounter_num]['note'][note_hash] = note_meta


class CodebookEntry:
    def __init__(self):
        self.patient_num = None
        self.patient_uuid = None
        self.encounter_num = None
        self.encounter_uuid = None

def deid_link() -> uuid:
    """
    Randomly generate a linked Patient identifier
    :return: long universally unique ID
    """
    return str(uuid.uuid4())

def hash_clinical_text(text:str):
    """
    Get "fingerprint" of clinical text to check if two inputs of the same text
    were both sent to ctakes. This is the intent of this method.
    :param text: clinical text
    :return: md5 digest
    """
    return hashlib.md5(text.encode('utf-8')).hexdigest()


###############################################################################
#
# I2b2 Codebook
#
###############################################################################

def deid_i2b2(observation:i2b2.ObservationFact) -> i2b2.ObservationFact:
    """
    :param observation: i2b2 values to replace with deid_link (UUID)
    :return: observation with no real PHI uniquely identifing patient
    """
    empty = dict()
    for col in i2b2.Column:
        empty[col.value] = None

    out = i2b2.ObservationFact(empty)
    out.observation_blob = str(hash_clinical_text(observation.observation_blob))

    out.patient_num = str(deid_link())
    out.encounter_num = str(deid_link())

    out.concept_cd = observation.concept_cd
    out.start_date = observation.start_date
    out.end_date = observation.end_date

    return out

###############################################################################
#
# SQL Codebook (TODO)
#
###############################################################################

def phi_get_patient_from_deid(deid_uuid: str) -> str:
    """
    :param deid_uuid: see "deid_make_uuid"
    :return: SQL statement for Hospital local codebook table
    """
    return f"select * from codebook where uuid='{deid_uuid}'"


def phi_get_patient_from_mrn(mrn: str) -> str:
    """
    Get patient identifiers for a given MRN (medical record number)
    http://hl7.org/fhir/patient-definitions.html#Patient.identifier
    :param mrn: Medical Record Number
    :return: SQL statement for Hospital local codebook table
    """
    return f"select * from codebook where mrn='{mrn}'"