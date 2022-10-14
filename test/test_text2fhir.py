"""Tests for the text2fhir NLP part of etl.py"""

import datetime
import os
import unittest

from cumulus import common
from cumulus import text2fhir

import ctakesclient
from ctakesclient.typesystem import CtakesJSON, Polarity

def path(filename: str):
    """
    Physician Note examples is sourced from ctakes:

    Expose ctakesclient tests accessible to user #17
    https://github.com/Machine-Learning-for-Medical-Language/ctakes-client-py/issues/17
    :param filename:
    :return: /path/to/resources/filename
    """
    return os.path.join(os.path.dirname(__file__), 'data', filename)

def example_note(filename='synthea.txt') -> str:
    """
    :param filename: default is *NOT PHI* Synthea AI generated example.
    """
    return common.read_text(path(filename))


def example_ctakes(filename='synthea.json') -> CtakesJSON:
    return CtakesJSON(common.read_json(path(filename)))


def example_nlp_source() -> dict:
    ver = ctakesclient.__version__
    url = f'https://github.com/Machine-Learning-for-Medical-Language/ctakes-client-py/releases/tag/v{ver}'

    return {"url": "http://fhir-registry.smarthealthit.org/StructureDefinition/nlp-source",
            "extension": [
                {"url": "algorithm", "valueString": "ctakesclient"},
                {"url": "version", "valueString": f"{url}"}]}


def example_derivation_reference() -> dict:
    return {'url': 'http://hl7.org/fhir/StructureDefinition/derivation-reference',
            'extension': [{'url': 'reference', 'valueReference': {'reference': 'DocumentReference/ABCD'}},
                            {'url': 'offset', 'valueInteger': 20},
                            {'url': 'length', 'valueInteger': 5}]}


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
    def setUp(self):
        self.maxDiff = None

    def test_nlp_source(self):
        expected = example_nlp_source()
        actual = text2fhir.nlp_source()

        # common.print_json(actual)
        self.assertDictEqual(expected, actual.as_json())

    def test_nlp_derivation_reference(self):

        actual = text2fhir.nlp_derivation(docref_id='ABCD', offset=20, length=5)

        # common.print_json(actual)
        self.assertDictEqual(example_derivation_reference(), actual.as_json())

    def test_fhir_concept(self):
        """
        Test construction of FHIR CodeableConcept the conventional way
        """
        vomiting1 = text2fhir.fhir_coding('http://snomed.info/sct', '249497008', 'Vomiting symptom (finding)')
        vomiting2 = text2fhir.fhir_coding('http://snomed.info/sct', '300359004', 'Finding of vomiting (finding)')

        as_fhir = text2fhir.fhir_concept('vomiting', [vomiting1, vomiting2])
        # common.print_json(as_fhir)
        self.assertDictEqual({
            'coding': [
                {
                    'code': '249497008',
                    'display': 'Vomiting symptom (finding)',
                    'system': 'http://snomed.info/sct'
                },
                {
                    'code': '300359004',
                    'display': 'Finding of vomiting (finding)',
                    'system': 'http://snomed.info/sct'
                },
            ],
            'text': 'vomiting',
        }, as_fhir.as_json())

    def test_nlp_concept(self):
        """
        Test construction of FHIR CodeableConcept the easy way using ctakesclient helper functions
        """
        ctakes_json = example_ctakes()

        concepts = [text2fhir.nlp_concept(match).as_json() for match in ctakes_json.list_match()]
        # common.print_json(concepts)
        self.assertEqual(120, len(concepts))

        # Just spot check the first one
        self.assertDictEqual({
            'coding': [
                {
                    'code': '66076007',
                    'system': 'SNOMEDCT_US',
                },
                {
                    'code': 'C0304290',
                    'system': 'http://terminology.hl7.org/CodeSystem/umls',
                },
                {
                    'code': '91058',
                    'system': 'RXNORM',
                },
                {
                    'code': 'C0304290',
                    'system': 'http://terminology.hl7.org/CodeSystem/umls',
                },
            ],
            'text': 'chewable tablet',
        }, concepts[0])

    def test_observation_symptom(self):
        """
        Test conversion from NLP to FHIR (SignSymptomMention -> FHIR Observation).
        Test serialization to/from JSON.
        Does not text expected values.
        """
        ctakes_json = example_ctakes()

        subject_id = '1234'
        encounter_id = '5678'
        docref_id = 'ABCD'

        symptoms = [
            text2fhir.nlp_observation(subject_id, encounter_id, docref_id, symptom).as_json()
            for symptom in ctakes_json.list_sign_symptom()
        ]
        # common.print_json(symptoms)
        self.assertEqual(38, len(symptoms))

        # Spot check first symptom (but first drop id as it is randomly generated)
        del symptoms[0]['id']
        self.assertDictEqual({
            'code': {
                'coding': [
                    {
                        'code': '33962009',
                        'system': 'SNOMEDCT_US',
                    },
                    {
                        'code': 'C0277786',
                        'system': 'http://terminology.hl7.org/CodeSystem/umls',
                    },
                    {
                        'code': '409586006',
                        'system': 'SNOMEDCT_US',
                    },
                    {
                        'code': 'C0277786',
                        'system': 'http://terminology.hl7.org/CodeSystem/umls',
                    },
                ],
                'text': 'Complaint',
            },
            'encounter': {
                'reference': 'Encounter/5678',
            },
            'extension': [
                {
                    'extension': [
                        {'url': 'reference', 'valueReference': {'reference': 'DocumentReference/ABCD'}},
                        {'url': 'offset', 'valueInteger': 21},
                        {'url': 'length', 'valueInteger': 9},
                    ],
                    'url': 'http://hl7.org/fhir/StructureDefinition/derivation-reference',
                },
            ],
            'modifierExtension': [
                example_nlp_source(),
                {
                    'url': 'http://fhir-registry.smarthealthit.org/StructureDefinition/nlp-polarity',
                    'valueBoolean': False,
                },
            ],
            'resourceType': 'Observation',
            'status': 'preliminary',
            'subject': {
                'reference': 'Patient/1234',
            },
        }, symptoms[0])

    def test_medication(self):
        """
        Test conversion from NLP to FHIR (MedicationMention -> FHIR MedicationStatement).
        Test serialization to/from JSON.
        Does not text expected values.
        """
        ctakes_json = example_ctakes()

        subject_id = '1234'
        encounter_id = '5678'
        docref_id = 'ABCD'

        medications = [
            text2fhir.nlp_medication(subject_id, encounter_id, docref_id, medication).as_json()
            for medication in ctakes_json.list_medication()
        ]
        # common.print_json(medications)
        self.assertEqual(5, len(medications))

        # Spot check first medication (but first drop id as it is randomly generated)
        del medications[0]['id']
        self.assertDictEqual({
            'context': {
                'reference': 'Encounter/5678',
            },
            'medicationCodeableConcept': {
                'coding': [
                    {
                        'code': '66076007',
                        'system': 'SNOMEDCT_US',
                    },
                    {
                        'code': 'C0304290',
                        'system': 'http://terminology.hl7.org/CodeSystem/umls',
                    },
                    {
                        'code': '91058',
                        'system': 'RXNORM',
                    },
                    {
                        'code': 'C0304290',
                        'system': 'http://terminology.hl7.org/CodeSystem/umls',
                    },
                ],
                'text': 'chewable tablet',
            },
            'extension': [
                {
                    'extension': [
                        {'url': 'reference', 'valueReference': {'reference': 'DocumentReference/ABCD'}},
                        {'url': 'offset', 'valueInteger': 442},
                        {'url': 'length', 'valueInteger': 15},
                    ],
                    'url': 'http://hl7.org/fhir/StructureDefinition/derivation-reference',
                },
            ],
            'modifierExtension': [
                example_nlp_source(),
                {
                    'url': 'http://fhir-registry.smarthealthit.org/StructureDefinition/nlp-polarity',
                    'valueBoolean': True,
                },
            ],
            'resourceType': 'MedicationStatement',
            'status': 'unknown',
            'subject': {
                'reference': 'Patient/1234',
            },
        }, medications[0])

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
        docref_id = 'ABCD'

        all_fhir = [as_fhir.as_json() for as_fhir in text2fhir.nlp_fhir(subject_id, encounter_id, docref_id, ctakes_json)]
        # common.print_json(all_fhir)
        self.assertEqual(68, len(all_fhir))

        resource_types = {x['resourceType'] for x in all_fhir}
        self.assertSetEqual({'Condition', 'MedicationStatement', 'Observation', 'Procedure'}, resource_types)

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

        bodysites = [
            text2fhir.nlp_concept(bodysite).as_json()
            for bodysite in ctakes_json.list_anatomical_site(Polarity.pos)
        ]
        # common.print_json(bodysites)
        self.assertEqual(5, len(bodysites))

        # Spot check first bodysite
        self.assertDictEqual({
            'coding': [
                {
                    'code': '8205005',
                    'system': 'SNOMEDCT_US',
                },
                {
                    'code': 'C0043262',
                    'system': 'http://terminology.hl7.org/CodeSystem/umls',
                },
            ],
            'text': 'wrist',
        }, bodysites[0])


if __name__ == '__main__':
    unittest.main()
