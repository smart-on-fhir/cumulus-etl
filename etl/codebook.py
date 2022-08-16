from etl import deid

class Codebook:
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

    def patient(self, mrn):
        """
        FHIR Patient
        :param mrn: Medical Record Number https://www.hl7.org/fhir/patient-definitions.html#Patient.identifier
        :return: record mapping MRN to a fake ID
        """
        if mrn:
            if mrn not in self.mrn.keys():
                self.mrn[mrn] = dict()
                self.mrn[mrn]['deid'] = deid.fake_id()
                self.mrn[mrn]['encounter'] = dict()
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
            if encounter_id not in self.mrn[mrn]['encounter'].keys():
                self.mrn[mrn]['encounter'][encounter_id] = dict()
                self.mrn[mrn]['encounter'][encounter_id]['deid'] = deid.fake_id()
                self.mrn[mrn]['encounter'][encounter_id]['period_start'] = period_start
                self.mrn[mrn]['encounter'][encounter_id]['period_end'] = period_end
                self.mrn[mrn]['encounter'][encounter_id]['docref'] = dict()

            return self.mrn[mrn]['encounter'][encounter_id]

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
            if md5sum not in self.mrn[mrn]['encounter'][encounter_id]['docref'].keys():
                self.mrn[mrn]['encounter'][encounter_id]['docref'][md5sum] = dict()
                self.mrn[mrn]['encounter'][encounter_id]['docref'][md5sum]['deid'] = deid.fake_id()

            return self.mrn[mrn]['encounter'][encounter_id]['docref'][md5sum]

    def _load_saved(self, saved:dict):
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

