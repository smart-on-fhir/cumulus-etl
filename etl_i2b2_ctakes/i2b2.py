from enum import Enum

class Column(Enum):
    """
    https://www.i2b2.org/software/projects/datarepo/CRC_Design_Doc_13.pdf
    """
    PATIENT_NUM = 'PATIENT_NUM'
    ENCOUNTER_NUM = 'ENCOUNTER_NUM'
    OBSERVATION_BLOB = 'OBSERVATION_BLOB'
    CONCEPT_CD = 'CONCEPT_CD'
    START_DATE = 'START_DATE'
    END_DATE = 'END_DATE'

class ObservationFact:
    """
    i2b2 ObservationFact
    init protects *immutability* in most cases.
    """
    def __init__(self, row:dict):
        self.patient_num = row[Column.PATIENT_NUM.value]
        self.encounter_num = row[Column.ENCOUNTER_NUM.value]
        self.concept_cd = row[Column.CONCEPT_CD.value]
        self.start_date = row[Column.START_DATE.value]
        self.end_date = row[Column.END_DATE.value]
        self.observation_blob = row[Column.OBSERVATION_BLOB.value]
