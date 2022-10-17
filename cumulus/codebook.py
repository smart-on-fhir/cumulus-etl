"""Codebook to help de-identify records"""

import logging
from fhirclient.models.fhirdate import FHIRDate
from fhirclient.models.patient import Patient
from fhirclient.models.encounter import Encounter
from fhirclient.models.condition import Condition
from fhirclient.models.observation import Observation
from fhirclient.models.documentreference import DocumentReference
from fhirclient.models.fhirreference import FHIRReference

from cumulus import common, fhir_common, store, text2fhir


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

        self.docrefs = {}

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

        condition.id = common.fake_id('Condition')
        condition.subject = fhir_common.ref_subject(self.db.patient(mrn)['deid'])
        condition.encounter = fhir_common.ref_encounter(self.db.encounter(mrn, encounter_id)['deid'])

        return condition

    def fhir_observation(self, observation: Observation) -> Observation:
        mrn = self._clean_mrn(observation.subject)
        encounter_id = self._clean_encounter_id(observation.encounter)

        observation.id = common.fake_id('Observation')
        observation.subject = fhir_common.ref_subject(self.db.patient(mrn)['deid'])
        observation.encounter = fhir_common.ref_encounter(self.db.encounter(mrn, encounter_id)['deid'])

        # Does the observation have an NLP source extension? If so, de-identify its docref
        for extension in (observation.extension or []):
            if extension.url == text2fhir.FHIR_DERIVATION_REF_URL:
                for values in extension.extension:
                    if values.url == 'reference':
                        cleaned_docref_id = self._clean_id(values.valueReference, 'DocumentReference')
                        deid_docref_id = self._docref_deid(cleaned_docref_id)
                        values.valueReference = fhir_common.ref_document(deid_docref_id)

        return observation

    def fhir_documentreference(self, docref: DocumentReference) -> DocumentReference:
        mrn = self._clean_mrn(docref.subject)
        original_id = docref.id

        docref.id = common.fake_id('DocumentReference')
        docref.subject = fhir_common.ref_subject(self.db.patient(mrn)['deid'])
        docref.context.encounter = [
            fhir_common.ref_encounter(self.db.encounter(mrn, self._clean_encounter_id(encounter))['deid'])
            for encounter in docref.context.encounter
        ]

        # Record the mapping for this document, so that later observations (like NLP results) can reference it.
        # We don't bother saving this in the codebook database though, since we don't care about persisting run-to-run.
        self.docrefs[original_id] = docref.id

        return docref

    def _docref_deid(self, docref_id: str) -> str:
        """
        Looks up the mapping from original to deid docref ID for this cumulus run

        This should only be used after all docrefs have been scanned, so that we already have the mappings.
        """
        deid_docref_id = self.docrefs.get(docref_id)

        if deid_docref_id is None:
            # Should not happen unless we have broken links -- all documents will have been scanned by this point.
            deid_docref_id = common.fake_id('DocumentReference')
            self.docrefs[docref_id] = deid_docref_id
            logging.error('Could not find existing docref to de-identify, inventing new ref "%s"', deid_docref_id)

        return deid_docref_id

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
                self.mrn[mrn]['deid'] = common.fake_id('Patient')
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
                self.mrn[mrn]['encounter'][encounter_id]['deid'] = common.fake_id('Encounter')

                if period_start and isinstance(period_start, FHIRDate):
                    period_start = period_start.isostring

                if period_end and isinstance(period_end, FHIRDate):
                    period_end = period_end.isostring

                self.mrn[mrn]['encounter'][encounter_id]['period_start'] = period_start
                self.mrn[mrn]['encounter'][encounter_id]['period_end'] = period_end

            return self.mrn[mrn]['encounter'][encounter_id]

    def _load_saved(self, saved: dict):
        """
        :param saved: dictionary containing structure
                      [patient][encounter]
        :return:
        """
        for mrn, patient_data in saved['mrn'].items():
            self.patient(mrn)['deid'] = patient_data['deid']

            for enc, enc_data in patient_data.get('encounter', {}).items():
                self.encounter(mrn, enc)['deid'] = enc_data['deid']
                self.encounter(mrn, enc)['period_start'] = enc_data['period_start']
                self.encounter(mrn, enc)['period_end'] = enc_data['period_end']

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
