import uuid
import hashlib
import i2b2

def deid_link() -> uuid:
    """
    Randomly generate a linked Patient identifier
    :return: long universally unique ID
    """
    return uuid.uuid4()

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