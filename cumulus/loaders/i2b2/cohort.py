"""Support for cohorts"""

from fhirclient.models.period import Period

from cumulus.loaders.i2b2.schema import PatientDimension, VisitDimension, ObservationFact


class CohortSelection:
    """
    NOTE: This code is experimental/testing.

    QBE Query By Example style approach to cohort selection.
    This class is used to hold selection criteria for Patient, Visits, and
    Observations.

    [USAGE]

    Date values should be expressed as FHIR Range.

    Applies to:
    * PatientDimension.BIRTH_DATE
    * PatientDimension.DEATH_DATE
    * VisitDimension.START_DATE
    * VisitDimension.END_DATE

    Duration values should be expressed as Duration.

    Applies to:
    * VisitDimension.LENGTH_OF_STAY

    List values should be applied to any i2b2 Concept Dimension, such as ICD10
    codes.

    Simple Example:
    ObservationFact.CONCEPT_CD = ['u07.1', 'R05.9']
    """

    def __init__(self, patient: PatientDimension, visit: VisitDimension,
                 observation: ObservationFact):
        """
        :param patient:
        :param visit:
        :param observation:
        """
        self.patient = patient
        self.visit = visit
        self.observation = observation

    def as_json(self):
        out = {
            'patient': self.patient.as_json(),
            'visit': self.visit.as_json(),
            'observation': self.observation.as_json()
        }

        if self.patient.birth_date and isinstance(self.patient.birth_date,
                                                  Period):
            out['patient']['birth_date'] = self.patient.birth_date.as_json()

        if self.patient.death_date and isinstance(self.patient.death_date,
                                                  Period):
            out['patient']['death_date'] = self.patient.death_date.as_json()

        if self.visit.start_date and isinstance(self.visit.start_date, Period):
            out['visit']['start_date'] = self.visit.start_date.as_json()

        if self.visit.end_date and isinstance(self.visit.end_date, Period):
            out['visit']['end_date'] = self.visit.end_date.as_json()

        return out
