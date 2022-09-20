"""Schemas for i2b2 csv files"""


class ObservationFact:
    """
    i2b2 ObservationFact
    https://www.i2b2.org/software/files/PDF/current/CRC_Design.pdf
    https://www.i2b2.org/software/projects/datarepo/CRC_Design_Doc_13.pdf

    ENCOUNTER_NUM   -> FHIR Encounter
    PATIENT_NUM     -> FHIR Patient
    CONCEPT_CD      -> FHIR CodeableConcept
    PROVIDER_ID
    START_DATE      -> date
    MODIFIER_CD
    INSTANCE_NUM
    VALTYPE_CD      ->
    TVAL_CHAR       -> Labs "Negative" or "Positive"
    NVAL_NUM
    VALUEFLAG_CD
    QUANTITY_NUM
    UNITS_CD        -> Lab Units (?)
    END_DATE
    LOCATION_CD
    OBSERVATION_BLOB -> Physician Notes (clinical text / NLP)
    CONFIDENCE_NUM
    UPDATE_DATE
    DOWNLOAD_DATE
    IMPORT_DATE
    SOURCESYSTEM_CD
    UPLOAD_ID
    TEXT_SEARCH_INDEX
    """

    def __init__(self, row: dict):
        self.patient_num = row.get('PATIENT_NUM')
        self.encounter_num = row.get('ENCOUNTER_NUM')
        self.concept_cd = row.get('CONCEPT_CD')
        self.start_date = row.get('START_DATE')
        self.end_date = row.get('END_DATE')
        self.observation_blob = row.get('OBSERVATION_BLOB')
        #
        self.valtype_cd = row.get('VALTYPE_CD')
        self.tval_char = row.get('TVAL_CHAR')
        self.nval_num = row.get('NVAL_NUM')
        self.modifier_cd = row.get('MODIFIER_CD')

    def as_json(self):
        return self.__dict__


class PatientDimension:
    """
    PATIENT_NUM
    VITAL_STATUS_CD
    BIRTH_DATE
    DEATH_DATE
    SEX_CD
    AGE_IN_YEARS_NUM
    LANGUAGE_CD
    RACE_CD
    MARITAL_STATUS_CD
    RELIGION_CD
    ZIP_CD
    STATECITYZIP_PATH
    INCOME_CD
    PATIENT_BLOB
    UPDATE_DATE
    DOWNLOAD_DATE
    IMPORT_DATE
    SOURCESYSTEM_CD
    UPLOAD_ID
    PCP_PROVIDER_ID
    """

    def __init__(self, row: dict):
        self.patient_num = row.get('PATIENT_NUM')
        self.birth_date = row.get('BIRTH_DATE')
        self.death_date = row.get('DEATH_DATE')
        self.sex_cd = row.get('SEX_CD')
        self.race_cd = row.get('RACE_CD')
        self.zip_cd = row.get('ZIP_CD')

    def as_json(self):
        return self.__dict__


class VisitDimension:
    """
    ENCOUNTER_NUM
    PATIENT_NUM
    START_DATE
    END_DATE
    INOUT_CD
    LOCATION_CD
    LENGTH_OF_STAY
    VISIT_BLOB
    """

    def __init__(self, row: dict):
        self.patient_num = row.get('PATIENT_NUM')
        self.encounter_num = row.get('ENCOUNTER_NUM')
        self.start_date = row.get('START_DATE')
        self.end_date = row.get('END_DATE')
        self.inout_cd = row.get('INOUT_CD')
        self.length_of_stay = row.get('LENGTH_OF_STAY')

    def as_json(self):
        return self.__dict__
