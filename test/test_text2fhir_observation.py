import unittest
import os

from fhirclient.models.fhirdate import FHIRDate

from cumulus import common
from cumulus import text2fhir
from ctakesclient.typesystem import CtakesJSON

def resource(filename:str):
    """
    Physician Note example is sourced from ctakes:

    Expose ctakesclient tests accessible to user #17
    https://github.com/Machine-Learning-for-Medical-Language/ctakes-client-py/issues/17
    :param filename:
    :return: /path/to/resources/filename
    """
    return os.path.join(os.path.dirname(__file__), '..', 'resources', filename)

def example_nlp_response() -> CtakesJSON:
    """
    :return: real example of response from ctakes for a physician note
    """
    return CtakesJSON(common.read_json(resource('test_physician_note.json')))

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

class TestText2Fhir(unittest.TestCase):

    def test_nlp_version_client(self):
        self.assertDictEqual(example_version(),
                             text2fhir.nlp_version_client().as_json())

    def test_nlp_algorithm(self):
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
        ctakes_json = example_nlp_response()
        for match in ctakes_json.list_sign_symptom():
            concept = text2fhir.nlp_concept(match)
            common.print_fhir(concept)

    def test_symptom(self):

        ctakes_json = CtakesJSON(common.read_json(resource('test_physician_note.json')))

        effective_date = FHIRDate('2021-09-27')
        subject_id = '1234'
        encounter_id = '5678'

        for match in ctakes_json.list_sign_symptom():
            symptom = text2fhir.to_fhir_observation_symptom(subject_id,
                                                            encounter_id,
                                                            effective_date,
                                                            match)
            common.print_fhir(symptom)



if __name__ == '__main__':
    unittest.main()
