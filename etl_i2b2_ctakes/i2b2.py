class ObservationFact:
    """
    i2b2 ObservationFact
    https://www.i2b2.org/software/projects/datarepo/CRC_Design_Doc_13.pdf

    ENCOUNTER_NUM   -> FHIR Encounter
    PATIENT_NUM     -> FHIR Patient
    CONCEPT_CD      -> FHIR CodeableConcept
    PROVIDER_ID
    START_DATE      -> date
    MODIFIER_CD
    INSTANCE_NUM
    VALTYPE_CD      ->
    TVAL_CHAR       -> Labs (?)
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
    def __init__(self, row:dict):
        self.patient_num = row['PATIENT_NUM']
        self.encounter_num = row['ENCOUNTER_NUM']
        self.concept_cd = row['CONCEPT_CD']
        self.start_date = row['START_DATE']
        self.end_date = row['END_DATE']
        self.observation_blob = row['OBSERVATION_BLOB']
        self.tval_char = row['TVAL_CHAR']
