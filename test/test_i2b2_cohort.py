import unittest

from cumulus.i2b2.cohort import *
from cumulus.i2b2 import transform as T

class TestCohortSuicidality(unittest.TestCase):

    def test_cohort_selection_suididality(self):
        """
        Example Cohort Selection
        """
        any = dict()
        patient = PatientDimension(any)
        patient.birth_date = T.parse_fhir_period('1992-01-01', '2023-01-01')

        visit = VisitDimension(any)
        visit.start_date = T.parse_fhir_date('2014-01-01')
        visit.end_date = T.parse_fhir_date('2023-01-01')
        visit.length_of_stay = T.parse_fhir_range(0, 365)

        obs = ObservationFact(any)
        obs.start_date = T.parse_fhir_date('2014-01-01')
        obs.end_date = T.parse_fhir_date('2023-01-01')
        obs.patient_num = patient
        obs.encounter_num = visit
        obs.concept_cd = ['ICD10:R45.851',  # suicidal ideation
                          'ICD10:T14.91*',  # suicide attempt
                          'ICD10:Z75'       # waiting for admit facility
                          ]

        cohort = CohortSelection(patient, visit, obs)

        print(cohort.as_json())
