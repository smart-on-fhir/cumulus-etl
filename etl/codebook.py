import logging
import uuid
import hashlib
import requests

from fhirclient.models.identifier import Identifier
from fhirclient.models.fhirreference import FHIRReference
from fhirclient.models.fhirdate import FHIRDate
from fhirclient.models.meta import Meta
from fhirclient.models.period import Period
from fhirclient.models.duration import Duration
from fhirclient.models.coding import Coding
from fhirclient.models.extension import Extension
from fhirclient.models.patient import Patient
from fhirclient.models.encounter import Encounter
from fhirclient.models.condition import Condition
from fhirclient.models.observation import Observation
from fhirclient.models.documentreference import DocumentReference
from fhirclient.models.documentreference import DocumentReferenceContext, DocumentReferenceContent
from fhirclient.models.attachment import Attachment
from fhirclient.models.codeableconcept import CodeableConcept

from etl import common
from etl.common import fake_id, hash_clinical_text

class Codebook:

    def __init__(self, db= None):
        """
        :param db: saved codebook or None (initialize empty)
        """
        self.db = db if db else CodebookDB()

    def fhir_patient(self, patient: Patient) -> Patient:
        mrn = patient.identifier[0].value
        deid = self.db.patient(mrn)['deid']

        patient.id = deid
        patient.identifier[0].value = deid

        return patient

    def fhir_encounter(self, encounter: Encounter) -> Encounter:
        mrn = encounter.subject.reference
        encounter.subject.reference = self.db.patient(mrn)['deid']

        deid = self.db.encounter(mrn, encounter.id)['deid']
        encounter.id = deid
        encounter.identifier[0].value = deid

        return encounter

    def fhir_condition(self, condition: Condition) -> Condition:
        mrn = condition.subject.reference
        condition.subject.reference = self.db.patient(mrn)['deid']

        condition.id = common.fake_id()
        condition.subject.reference = self.db.patient(mrn)['deid']
        condition.encounter.reference = self.db.encounter(mrn, condition.encounter.reference)['deid']

        return condition

    def fhir_observation(self, observation: Observation) -> Observation:
        mrn = observation.subject.reference
        observation.subject.reference = self.db.patient(mrn)['deid']

        observation.id = common.fake_id()
        observation.subject.reference = self.db.patient(mrn)['deid']
        observation.context.encounter.reference = self.db.encounter(mrn, observation.context.encounter.reference)['deid']

        return observation

    def fhir_documentreference(self, docref: DocumentReference) -> DocumentReference:
        return docref

#######################################################################################################################
#
# HTTP Client for CTAKES REST
#
#######################################################################################################################

class CodebookDB:
    def __init__(self, saved=None):
        """
        Preserve scientific accuracy of patient counting and linkage while preserving patient privacy.

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

    def patient(self, mrn) -> dict:
        """
        FHIR Patient
        :param mrn: Medical Record Number https://www.hl7.org/fhir/patient-definitions.html#Patient.identifier
        :return: record mapping MRN to a fake ID
        """
        if mrn:
            if mrn not in self.mrn.keys():
                self.mrn[mrn] = dict()
                self.mrn[mrn]['deid'] = common.fake_id()
                self.mrn[mrn]['encounter'] = dict()
            return self.mrn[mrn]

    def encounter(self, mrn, encounter_id, period_start=None, period_end=None) -> dict:
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
            if encounter_id not in self.mrn[mrn]['encounter'].keys():
                self.mrn[mrn]['encounter'][encounter_id] = dict()
                self.mrn[mrn]['encounter'][encounter_id]['deid'] = common.fake_id()
                self.mrn[mrn]['encounter'][encounter_id]['period_start'] = period_start
                self.mrn[mrn]['encounter'][encounter_id]['period_end'] = period_end
                self.mrn[mrn]['encounter'][encounter_id]['docref'] = dict()

            return self.mrn[mrn]['encounter'][encounter_id]

    def docref(self, mrn, encounter_id, md5sum) -> dict:
        """
        FHIR DocumentReference  
        :param mrn: Medical Record Number https://www.hl7.org/fhir/patient-definitions.html#Patient.identifier 
        :param encounter_id: https://hl7.org/fhir/encounter-definitions.html#Encounter.identifier
        :param md5sum: https://www.hl7.org/fhir/documentreference-definitions.html#DocumentReference.identifier
        :return: record mapping docref to a fake ID
        """
        self.encounter(mrn, encounter_id)
        if md5sum:
            if md5sum not in self.mrn[mrn]['encounter'][encounter_id]['docref'].keys():
                self.mrn[mrn]['encounter'][encounter_id]['docref'][md5sum] = dict()
                self.mrn[mrn]['encounter'][encounter_id]['docref'][md5sum]['deid'] = common.fake_id()

            return self.mrn[mrn]['encounter'][encounter_id]['docref'][md5sum]

    def _load_saved(self, saved: dict):
        """        
        :param saved: dictionary containing structure [patient][encounter][docref]
        :return:
        """
        for mrn in saved['mrn'].keys():
            self.patient(mrn)['deid'] = saved['mrn'][mrn]['deid']

            for enc in saved['mrn'][mrn]['encounter'].keys():
                self.encounter(mrn, enc)['deid'] = saved['mrn'][mrn]['encounter'][enc]['deid']
                self.encounter(mrn, enc)['period_start'] = saved['mrn'][mrn]['encounter'][enc]['period_start']
                self.encounter(mrn, enc)['period_end'] = saved['mrn'][mrn]['encounter'][enc]['period_end']

                for md5sum in saved['mrn'][mrn]['encounter'][enc]['docref']:
                    self.docref(mrn, enc, md5sum)['deid'] = saved['mrn'][mrn]['encounter'][enc]['docref'][md5sum]['deid']





