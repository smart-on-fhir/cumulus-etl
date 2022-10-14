"""Codebook to help de-identify records"""

import logging
from fhirclient.models.fhirdate import FHIRDate
from fhirclient.models.patient import Patient
from fhirclient.models.encounter import Encounter
from fhirclient.models.condition import Condition
from fhirclient.models.observation import Observation
from fhirclient.models.documentreference import DocumentReference
from fhirclient.models.fhirreference import FHIRReference

from cumulus import common, fhir_common, store


class Codebook:
    """
    Codebook links real IDs (like MRN medical record number) to UUIDs.
    Obtaining the UUID without the codebook is safe.
    Codebook is saved local to the hospital and NOT shared on public internet.
    """

    def __init__(self, saved=None):
        """
        :param saved: saved codebook or None (initialize empty)
        """
        try:
            self.db = CodebookDB(saved)
        except FileNotFoundError:
            self.db = CodebookDB()

    def fhir_patient(self, patient: Patient) -> Patient:
        mrn = patient.identifier[0].value
        deid = self.db.patient(mrn)['deid']

        patient.id = deid
        patient.identifier[0].value = deid

        return patient

    def fhir_encounter(self, encounter: Encounter) -> Encounter:
        mrn = self._clean_mrn(encounter.subject)

        deid = self.db.encounter(mrn, encounter.id, encounter.period.start,
                                 encounter.period.end)['deid']
        encounter.id = deid
        encounter.identifier[0].value = deid
        encounter.subject = fhir_common.ref_subject(self.db.patient(mrn)['deid'])

        return encounter

    def fhir_condition(self, condition: Condition) -> Condition:
        mrn = self._clean_mrn(condition.subject)
        encounter_id = self._clean_encounter_id(condition.encounter)

        condition.id = common.fake_id()
        condition.subject = fhir_common.ref_subject(self.db.patient(mrn)['deid'])
        condition.encounter = fhir_common.ref_encounter(self.db.encounter(mrn, encounter_id)['deid'])

        return condition

    def fhir_observation(self, observation: Observation) -> Observation:
        mrn = self._clean_mrn(observation.subject)
        encounter_id = self._clean_encounter_id(observation.encounter)

        observation.id = common.fake_id()
        observation.subject = fhir_common.ref_subject(self.db.patient(mrn)['deid'])
        observation.encounter = fhir_common.ref_encounter(self.db.encounter(mrn, encounter_id)['deid'])

        return observation

    def fhir_documentreference(self, docref: DocumentReference) -> DocumentReference:
        mrn = self._clean_mrn(docref.subject)

        docref.id = common.fake_id()
        docref.subject = fhir_common.ref_subject(self.db.patient(mrn)['deid'])
        docref.context.encounter = [
            fhir_common.ref_encounter(self.db.encounter(mrn, self._clean_encounter_id(encounter))['deid'])
            for encounter in docref.context.encounter
        ]

        return docref

    @staticmethod
    def _clean_id(ref: FHIRReference, id_type: str) -> str:
        """
        Strips a leading type marker, if any

        Examples with id_type=Patient:
        - ABC -> ABC
        - Patient/ABC -> ABC
        - Group/ABC -> Group/ABC
        """
        # TODO: what if ref is not simply a local reference like Patient/ABC, but has a type & identifier or url
        prefix = f'{id_type}/'
        # Once we depend on python3.9+, we can simply use identifier.removeprefix()
        if ref.reference.startswith(prefix):
            return ref.reference[len(prefix):]
        return ref.reference

    def _clean_mrn(self, ref: FHIRReference) -> str:
        return self._clean_id(ref, 'Patient')

    def _clean_encounter_id(self, ref: FHIRReference) -> str:
        return self._clean_id(ref, 'Encounter')


###############################################################################
#
# HTTP Client for CTAKES REST
#
###############################################################################


class CodebookDB:
    """Class to hold codebook data and read/write it to storage"""

    def __init__(self, saved=None):
        """
        Create a codebook database.

        Preserves scientific accuracy of patient counting and linkage while
        preserving patient privacy.

        Codebook replaces sensitive PHI identifiers with DEID linked
        identifiers.
        https://www.ncbi.nlm.nih.gov/pmc/articles/PMC2244902

        codebook::= (patient (encounter note))+
        mrn::= text
        encounter::= encounter_id period_start period_end
        note::= md5sum

        :param saved: load from file (optional)
        """
        self.mrn = {}
        if saved is not None:
            if isinstance(saved, str):
                self._load_saved(common.read_json(saved))
            if isinstance(saved, dict):
                self._load_saved(saved)
            if isinstance(saved, CodebookDB):
                self.mrn = saved.mrn

    def patient(self, mrn) -> dict:
        """
        FHIR Patient
        :param mrn: Medical Record Number
                    https://www.hl7.org/fhir/patient-definitions.html#Patient.identifier
        :return: record mapping MRN to a fake ID
        """
        if mrn:
            if mrn not in self.mrn.keys():
                self.mrn[mrn] = {}
                self.mrn[mrn]['deid'] = common.fake_id()
                self.mrn[mrn]['encounter'] = {}
            return self.mrn[mrn]

    def encounter(self,
                  mrn,
                  encounter_id,
                  period_start=None,
                  period_end=None) -> dict:
        """
        FHIR Encounter

        :param mrn: Medical Record Number
        :param encounter_id: encounter identifier
                             https://hl7.org/fhir/encounter-definitions.html#Encounter.identifier
        :param period_start: start of encounter
                             http://hl7.org/fhir/encounter-definitions.html#Encounter.period
        :param period_end: end of encounter
                           http://hl7.org/fhir/encounter-definitions.html#Encounter.period
        :return: record mapping encounter to a fake ID
        """
        self.patient(mrn)
        if encounter_id:
            if encounter_id not in self.mrn[mrn]['encounter'].keys():
                self.mrn[mrn]['encounter'][encounter_id] = {}
                self.mrn[mrn]['encounter'][encounter_id][
                    'deid'] = common.fake_id()
                self.mrn[mrn]['encounter'][encounter_id]['docref'] = {}

                if period_start:
                    if isinstance(period_start, FHIRDate):
                        period_start = period_start.isostring

                if period_end:
                    if isinstance(period_end, FHIRDate):
                        period_end = period_end.isostring

                self.mrn[mrn]['encounter'][encounter_id][
                    'period_start'] = period_start
                self.mrn[mrn]['encounter'][encounter_id][
                    'period_end'] = period_end

            return self.mrn[mrn]['encounter'][encounter_id]

    def docref(self, mrn, encounter_id, md5sum) -> dict:
        """
        FHIR DocumentReference
        :param mrn: Medical Record Number
                    https://www.hl7.org/fhir/patient-definitions.html#Patient.identifier
        :param encounter_id: encounter identifier
                             https://hl7.org/fhir/encounter-definitions.html#Encounter.identifier
        :param md5sum: md5 checksum
                       https://www.hl7.org/fhir/documentreference-definitions.html#DocumentReference.identifier
        :return: record mapping docref to a fake ID
        """
        self.encounter(mrn, encounter_id)
        if md5sum:
            if md5sum not in self.mrn[mrn]['encounter'][encounter_id][
                    'docref'].keys():
                self.mrn[mrn]['encounter'][encounter_id]['docref'][md5sum] = {}
                self.mrn[mrn]['encounter'][encounter_id]['docref'][md5sum][
                    'deid'] = common.fake_id()

            return self.mrn[mrn]['encounter'][encounter_id]['docref'][md5sum]

    def _load_saved(self, saved: dict):
        """
        :param saved: dictionary containing structure
                      [patient][encounter][docref]
        :return:
        """
        for mrn in saved['mrn'].keys():
            self.patient(mrn)['deid'] = saved['mrn'][mrn]['deid']

            for enc in saved['mrn'][mrn]['encounter'].keys():
                self.encounter(
                    mrn,
                    enc)['deid'] = saved['mrn'][mrn]['encounter'][enc]['deid']
                self.encounter(mrn, enc)['period_start'] = saved['mrn'][mrn][
                    'encounter'][enc]['period_start']
                self.encounter(mrn, enc)['period_end'] = saved['mrn'][mrn][
                    'encounter'][enc]['period_end']

                for md5sum in saved['mrn'][mrn]['encounter'][enc]['docref']:
                    self.docref(mrn, enc, md5sum)['deid'] = saved['mrn'][mrn][
                        'encounter'][enc]['docref'][md5sum]['deid']

    def save(self, path: str) -> None:
        """
        Save the CodebookDB database as JSON
        :param path: /path/to/codebook.json
        """
        logging.info('Saving codebook to: %s', path)
        common.write_json(path, self.__dict__)

    def delete(self, root: store.Root, path: str) -> None:
        """
        DELETE the CodebookDB database
        :param root: target filesystem
        :param path: /path/to/codebook.json
        """
        logging.warning('DELETE codebook from: %s', path)
        root.rm(path)
