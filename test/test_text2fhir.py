import unittest
import os

from cumulus import common
from cumulus import text2fhir
from ctakesclient.typesystem import CtakesJSON, Polarity

def example_note(filename='synthea.txt') -> str:
    """
    :param filename: default is *NOT PHI* Synthea AI generated example.
    """
    return common.read_text(path(filename))

def example_ctakes(filename='synthea.json') -> CtakesJSON:
    return CtakesJSON(common.read_json(path(filename)))

def path(filename: str):
    """
    Physician Note examples is sourced from ctakes:

    Expose ctakesclient tests accessible to user #17
    https://github.com/Machine-Learning-for-Medical-Language/ctakes-client-py/issues/17
    :param filename:
    :return: /path/to/resources/filename
    """
    return os.path.join(os.path.dirname(__file__), '..', 'resources', filename)

def example_version() -> dict:
    """
    :return: real example of nlp-version
    """
    return {"url": "http://fhir-registry.smarthealthit.org/StructureDefinition/nlp-version",
            "valueCodeableConcept": {
                "text": "NLP Version",
                "coding": [{
                    "code": "1.0.3",
                    "display": "ctakesclient=1.0.3",
                    "system": "https://github.com/Machine-Learning-for-Medical-Language/ctakes-client-py/releases/tag/v1.0.3"
                }]}}

def example_derivation_reference() -> dict:
    """
    Jamie Jones proposed example
    """
    return {'extension': [{'extension': [{'url': 'reference',
                                          'valueReference': {'display': 'note',
                                                             'reference': 'DocumentReference/episode-summary'}},
                                         {'url': 'offset', 'valueInteger': 20},
                                         {'url': 'length', 'valueInteger': 5},
                                         {'url': 'algorithm', 'valueString': 'however specifically we want to log the fact that cTAKES was used'},
                                         {'url': 'version', 'valueString': 'whatever date or number is useful here'}],
                           'url': 'http://hl7.org/fhir/StructureDefinition/derivation-reference'}]}

###############################################################################
#
# Unit Test : assertions and lightweight checking reading/printing FHIR types
#
###############################################################################

class TestText2Fhir(unittest.TestCase):
    """
    Test Transformation from NLP results  to FHIR resources.
    Test serialization to/from JSON of the different types (NLP and FHIR).

    Most test methods do not test expected values yet, as the proposal for derivation reference is not finalized.
    see @self.example_derivation_reference()
    http://build.fhir.org/extension-derivation-reference.html
    """
    def test_nlp_version_client(self):
        self.assertDictEqual(example_version(),
                             text2fhir.nlp_version_client().as_json())

    def test_nlp_algorithm(self):
        """
        Test the FHIR Extension for "nlp-algorithm" is the proposed format.
        """
        ver = text2fhir.nlp_version_client()
        algo = text2fhir.nlp_algorithm(ver)

        common.print_fhir(algo)

    def test_fhir_concept(self):
        """
        Test construction of FHIR CodeableConcept the conventional way
        """
        vomiting1 = text2fhir.fhir_coding('http://snomed.info/sct', '249497008', 'Vomiting symptom (finding)')
        vomiting2 = text2fhir.fhir_coding('http://snomed.info/sct', '300359004', 'Finding of vomiting (finding)')

        # without extension
        as_fhir = text2fhir.fhir_concept('vomiting', [vomiting1, vomiting2])
        common.print_fhir(as_fhir)

        # with extension
        position = text2fhir.nlp_text_position(58, 66)
        as_fhir = text2fhir.fhir_concept('vomiting', [vomiting1, vomiting2], position)
        common.print_fhir(as_fhir)

    def test_nlp_concept(self):
        """
        Test construction of FHIR CodeableConcept the easy way using ctakesclient helper functions
        """
        ctakes_json = example_ctakes()

        for match in ctakes_json.list_match():
            concept = text2fhir.nlp_concept(match)
            common.print_fhir(concept)

    def test_observation_symptom(self):
        """
        Test conversion from NLP to FHIR (SignSymptomMention -> FHIR Observation).
        Test serialization to/from JSON.
        Does not text expected values.
        """
        ctakes_json = example_ctakes()

        subject_id = '1234'
        encounter_id = '5678'

        for match in ctakes_json.list_sign_symptom():
            symptom = text2fhir.nlp_observation(subject_id,
                                                encounter_id,
                                                match)
            common.print_fhir(symptom)

    def test_medication(self):
        """
        Test conversion from NLP to FHIR (MedicationMention -> FHIR MedicationStatement).
        Test serialization to/from JSON.
        Does not text expected values.
        """
        ctakes_json = example_ctakes()

        subject_id = '1234'
        encounter_id = '5678'

        for match in ctakes_json.list_medication():
            medication = text2fhir.nlp_medication(subject_id,
                                                  encounter_id,
                                                  match)
            common.print_fhir(medication)

    def test_nlp_fhir(self):
        """
        Test conversion from NLP (SignSymptom, DiseaseDisorder, Medication, Procedure) to
        FHIR (Observation, Condition, MedicationStatement, Procedure).

        Test serialization to/from JSON.
        Does not text expected values.
        """
        ctakes_json = example_ctakes()

        subject_id = '1234'
        encounter_id = '5678'

        for as_fhir in text2fhir.nlp_fhir(subject_id, encounter_id, ctakes_json):
            common.print_fhir(as_fhir)

    def test_nlp_bodysite(self):
        """
        Optional. Demonstrate use of FHIR BodySite - can be attached to different resources.
        This example also shows the flexibility of using nlp_concept to support additional FHIR resource types.

        https://www.hl7.org/fhir/procedure-definitions.html#Procedure.bodySite
        https://www.hl7.org/fhir/condition-definitions.html#Condition.bodySite
        https://www.hl7.org/fhir/extension-bodysite.html
        https://www.hl7.org/fhir/valueset-body-site.html
        """
        ctakes_json = example_ctakes()

        for match in ctakes_json.list_anatomical_site(Polarity.pos):
            bodysite = text2fhir.nlp_concept(match)

            common.print_fhir(bodysite)


if __name__ == '__main__':
    unittest.main()
