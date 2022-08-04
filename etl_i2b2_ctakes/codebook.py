import logging
import uuid
import hashlib
import i2b2

class Codebook:
    def __init__(self, saved=None):
        """
        Preserve  scientific accuracy of patient counting and linkage while preserving patient privacy.

        Codebook replaces sensitive PHI identifiers with DEID linked identifiers.
        https://www.ncbi.nlm.nih.gov/pmc/articles/PMC2244902

        codebook::= (patient (encounter note))+
        mrn::= text
        encounter::= encounter_id period_start period_end
        note::= md5sum

        :param saved: load from file (optional)
        """
        self.mrn = dict()
        if saved:
            self._load_saved(saved)

    def patient(self, mrn):
        """
        FHIR Patient
        :param mrn: Medical Record Number https://www.hl7.org/fhir/patient-definitions.html#Patient.identifier
        :return: record mapping MRN to a fake ID
        """
        if mrn:
            if mrn not in self.mrn.keys():
                self.mrn[mrn] = dict()
                self.mrn[mrn]['deid'] = str(uuid.uuid4())
                self.mrn[mrn]['encounter_id'] = dict()
            return self.mrn[mrn]

    def encounter(self, mrn, encounter_id, period_start=None, period_end=None):
        """
        FHIR Encounter

        :param mrn: Medical Record Number
        :param encounter_id: https://hl7.org/fhir/encounter-definitions.html#Encounter.identifier
        :param period_start: http://hl7.org/fhir/encounter-definitions.html#Encounter.period
        :param period_end: http://hl7.org/fhir/encounter-definitions.html#Encounter.period
        :return: record mapping encounter to a fake ID
        """
        self.patient(mrn)
        if encounter_id:
            if encounter_id not in self.mrn[mrn]['encounter_id'].keys():
                self.mrn[mrn]['encounter_id'][encounter_id] = dict()
                self.mrn[mrn]['encounter_id'][encounter_id]['deid'] = str(uuid.uuid4())
                self.mrn[mrn]['encounter_id'][encounter_id]['period_start'] = period_start
                self.mrn[mrn]['encounter_id'][encounter_id]['period_end'] = period_end
                self.mrn[mrn]['encounter_id'][encounter_id]['docref'] = dict()

            return self.mrn[mrn]['encounter_id'][encounter_id]

    def docref(self, mrn, encounter_id, md5sum):
        """
        FHIR DocumentReference  
        :param mrn: Medical Record Number https://www.hl7.org/fhir/patient-definitions.html#Patient.identifier 
        :param encounter_id: https://hl7.org/fhir/encounter-definitions.html#Encounter.identifier
        :param md5sum: https://www.hl7.org/fhir/documentreference-definitions.html#DocumentReference.identifier
        :return: record mapping docref to a fake ID
        """
        self.encounter(mrn, encounter_id)
        if md5sum:
            if md5sum not in self.mrn[mrn]['encounter_id'][encounter_id]['docref'].keys():
                self.mrn[mrn]['encounter_id'][encounter_id]['docref'][md5sum] = dict()

            return self.mrn[mrn]['encounter_id'][encounter_id]['docref'][md5sum]

    def _load_saved(self, saved:dict):
        """        
        :param saved: dictionary containing structure [mrn][encounter_id][docref]
        :return: 
        """
        for mrn in saved['mrn'].keys():
            self.patient(mrn)['deid'] = saved['mrn'][mrn]['deid']

            for enc in saved['mrn'][mrn]['encounter_id'].keys():
                self.encounter(mrn, enc)['deid'] = saved['mrn'][mrn]['encounter_id'][enc]['deid']

                for md5sum in saved['mrn'][mrn]['encounter_id'][enc]['docref']:
                    self.docref(mrn, enc, md5sum)

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
    out.encounter_id = str(deid_link())

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