"""Tests for i2b2.cohort"""

import unittest

from cumulus import fhir_common
from cumulus.loaders.i2b2 import cohort


class TestCohortSuicidality(unittest.TestCase):
    """Test case for cohort suicidality"""

    def test_cohort_selection_suididality(self):
        """
        Example Cohort Selection
        """
        empty_row = {}
        patient = cohort.PatientDimension(empty_row)
        patient.birth_date = fhir_common.parse_fhir_period("1992-01-01", "2023-01-01")

        visit = cohort.VisitDimension(empty_row)
        visit.start_date = fhir_common.parse_fhir_date("2014-01-01")
        visit.end_date = fhir_common.parse_fhir_date("2023-01-01")
        visit.length_of_stay = fhir_common.parse_fhir_range(0, 365)

        obs = cohort.ObservationFact(empty_row)
        obs.start_date = fhir_common.parse_fhir_date("2014-01-01")
        obs.end_date = fhir_common.parse_fhir_date("2023-01-01")
        obs.patient_num = patient
        obs.encounter_num = visit
        obs.concept_cd = [
            "ICD10:R45.851",  # suicidal ideation
            "ICD10:T14.91*",  # suicide attempt
            "ICD10:Z75",  # waiting for admit facility
        ]

        selection = cohort.CohortSelection(patient, visit, obs)

        print(selection.as_json())
