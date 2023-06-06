"""Schemas for i2b2 csv files"""

from enum import Enum

# pylint: disable=invalid-name


class Table(Enum):
    """
    https://www.i2b2.org/software/files/PDF/current/CRC_Design.pdf

    select TABLE_NAME from all_tables
     where tablespace_name = 'I2B2_BLUE_TABLESPACE'
    """

    patient = "patient_dimension"
    patient_map = "patient_mapping"
    provider = "provider_dimension"
    visit = "visit_dimension"
    concept = "concept_dimension"
    modifier = "modifier_dimension"
    observation_fact = "observation_fact"
    i2b2 = "i2b2"  # metadata
    mrn_patient_uuid = "mrn_patuuid_patnum"
    mrn_patient_failed = "mrn_patuuid_patnum_failed"


class Dimension:
    """Base class for any i2b2 entry"""


###############################################################################
# i2b2 PatientDimension --> FHIR Patient
###############################################################################


class PatientDimension(Dimension):
    """
    desc patient_dimension

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

    def __init__(self, row=None):
        if row is None:
            row = {}

        self.patient_num = row.get("PATIENT_NUM")
        self.birth_date = row.get("BIRTH_DATE")
        self.death_date = row.get("DEATH_DATE")
        self.sex_cd = row.get("SEX_CD")
        self.race_cd = row.get("RACE_CD")
        self.zip_cd = row.get("ZIP_CD")

        self.table = Table.patient

    def as_json(self):
        return self.__dict__


###############################################################################
# i2b2 ProviderDimension --> FHIR Practitioner (Future)
###############################################################################
class ProviderDimension(Dimension):
    """
    desc provider_dimension

        NAME_CHAR
        PROVIDER_BLOB
        SOURCESYSTEM_CD
        UPLOAD_ID
        IMPORT_DATE
        UPDATE_DATE
        DOWNLOAD_DATE
    """

    def __init__(self, row=None):
        if row is None:
            row = {}

        self.provider_path = row.get("PROVIDER_PATH")
        self.provider_id = row.get("PROVIDER_ID")
        self.sourcesystem_cd = row.get("SOURCESYSTEM_CD")
        self.name_char = row.get("NAME_CHAR")
        self.provider_blob = row.get("PROVIDER_BLOB")
        self.import_date = row.get("IMPORT_DATE")

        self.table = Table.provider

    def as_json(self):
        return self.__dict__


###############################################################################
# I2b2 VisitDimension --> FHIR Encounter
###############################################################################


class VisitDimension(Dimension):
    """
    desc visit_dimension

        ENCOUNTER_NUM
        PATIENT_NUM
        START_DATE
        END_DATE
        INOUT_CD
        LOCATION_CD
        LENGTH_OF_STAY
        VISIT_BLOB
    """

    def __init__(self, row=None):
        if row is None:
            row = {}

        self.patient_num = row.get("PATIENT_NUM")
        self.encounter_num = row.get("ENCOUNTER_NUM")
        self.start_date = row.get("START_DATE")
        self.end_date = row.get("END_DATE")
        self.inout_cd = row.get("INOUT_CD")
        self.length_of_stay = row.get("LENGTH_OF_STAY")

        self.table = Table.visit

    def as_json(self):
        return self.__dict__


###############################################################################
#
# I2b2 ConceptDimension --> FHIR Condition, FHIR Observation,
#                           (future: FHIR Medication, ...)
#
###############################################################################
class ConceptDimension(Dimension):
    """
    desc concept_dimension

        CONCEPT_CD
        NAME_CHAR
        CONCEPT_BLOB
        UPDATE_DATE
        DOWNLOAD_DATE
        IMPORT_DATE
        SOURCESYSTEM_CD
        UPLOAD_ID
    """

    def __init__(self, row=None):
        if row is None:
            row = {}

        self.concept_cd = row.get("CONCEPT_CD")
        self.name_char = row.get("NAME_CHAR")
        self.concept_blob = row.get("CONCEPT_BLOB")
        self.download_date = row.get("DOWNLOAD_DATE")
        self.import_date = row.get("IMPORT_DATE")
        self.sourcesystem_cd = row.get("SOURCESYSTEM_CD")

        self.table = Table.concept


###############################################################################
#
# I2b2 ObservationFact --> FHIR Condition, FHIR Observation,
#                          FHIR DocumentReference
#
###############################################################################


class ValueType(Enum):
    """
    observation_fact.VALTYPE_CD

        @: No Value
        N: Numeric objects such as those found in lab tests
        T: Text objects such as labels, short message, enumerated values
        B: Raw text objects such as a doctor’s note, discharge summary, and
           radiology report NLP result xml objects
        NLP: NLP result xml objects
    """

    no_value = "@"
    numeric = "N"
    text = "T"
    clinical_notes = "B"
    NLP = "NLP"


class ValueEquality(Enum):
    """
    observation_fact.TVAL_CHAR

        Used in conjunction with VALTYPE_CD = “T” or
        “N” When the VALTYPE_CD = “T”
        Stores the text value

        When VALTYPE_CD = “N”
        E = Equals
        NE = Not equal
        L = Less than
        LE = Less than and Equal to
        G = Greater than
        GE = Greater than and Equal to
    """

    equal = "E"
    less = "L"
    less_equal = "LE"
    greater = "G"
    greater_equal = "GE"


class ValueFlagNormality(Enum):
    """
    observation_fact.VALUEFLAG_CD

        Used in conjunction with VALTYPE_CD = “B”, “NLP”, “N”, or “T”
        When VALTYPE_CD = “B” or “NLP” it is used to indicate whether or not
        the data in the blob column is encrypted.
        X = Encrypted text in the blob column
        When the VALTYPE_CD = “N” or “T” it is used to flag certain outlying
        or abnormal values
        H = High
        L = Low
        A = Abnormal
    """

    no_value = "@"
    abnormal = "A"
    high = "H"
    low = "L"
    encrypted = "X"


class ObservationFact(Dimension):
    """
    desc observation_fact

        PATIENT_NUM     -> ref i2b2.patient_dimension
        PROVIDER_ID     -> ref i2b2.provider_dimension
        ENCOUNTER_NUM   -> ref i2b2.visit_dimension
        START_DATE      -> ref i2b2.visit_dimension
        END_DATE        -> ref i2b2.visit_dimension
        LOCATION_CD     -> ref i2b2.visit_dimension
        CONCEPT_CD      -> ref i2b2.concept_dimension

        OBSERVATION_BLOB -> Clinical Notes (usually)
        INSTANCE_NUM    -> unique ID of observation
        VALTYPE_CD      -> @see ValueType(Enum)
        TVAL_CHAR       -> @see ValueEquality(Enum) Labs "Negative" or
                           "Positive"
        NVAL_NUM        -> "numerical value" (optional)
        QUANTITY_NUM    -> "quantity value" (optional)
        UNITS_CD        -> Lab Units (primary usage)
        VALUEFLAG_CD    -> @see ValueFlagCD(Enum)

        CONFIDENCE_NUM
        UPDATE_DATE
        DOWNLOAD_DATE
        IMPORT_DATE
        SOURCESYSTEM_CD
        UPLOAD_ID
        TEXT_SEARCH_INDEX
    """

    def __init__(self, row: dict):
        self.table = Table.observation_fact
        self.instance_num = row.get("INSTANCE_NUM")
        self.patient_num = row.get("PATIENT_NUM")
        self.encounter_num = row.get("ENCOUNTER_NUM")
        self.concept_cd = row.get("CONCEPT_CD")
        self.start_date = row.get("START_DATE")
        self.end_date = row.get("END_DATE")
        self.observation_blob = row.get("OBSERVATION_BLOB")
        #
        self.valtype_cd = row.get("VALTYPE_CD")
        self.valueflag_cd = row.get("VALUEFLAG_CD")
        self.tval_char = row.get("TVAL_CHAR")
        self.nval_num = row.get("NVAL_NUM")
        self.units_cd = row.get("UNITS_CD")
        self.modifier_cd = row.get("MODIFIER_CD")

    def get_value_type(self) -> ValueType:
        return ValueType(self.valtype_cd)

    def get_value_equality(self) -> ValueEquality:
        return ValueEquality(self.tval_char)

    def get_value_flag_abnormality(self) -> ValueFlagNormality:
        return ValueFlagNormality(self.valueflag_cd)

    def as_json(self):
        return self.__dict__
