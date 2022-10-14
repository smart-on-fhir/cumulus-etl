"""Tests for the text2fhir NLP part of etl.py"""

import datetime
import os
import unittest

from cumulus import common
from cumulus import text2fhir

import ctakesclient
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
    return os.path.join(os.path.dirname(__file__), 'data', filename)


def expected_version() -> dict:
    """
    :return: real example of nlp-version
    """
    ver = ctakesclient.__version__
    system_url = f'https://github.com/Machine-Learning-for-Medical-Language/ctakes-client-py/releases/tag/v{ver}'
    return {
        'url': 'http://fhir-registry.smarthealthit.org/StructureDefinition/nlp-version',
        'valueCodeableConcept': {
            'text': 'NLP Version',
            'coding': [{
                'code': ver,
                'display': f'ctakesclient=={ver}',
                'system': system_url,
            }]
        }
    }


def expected_algorithm() -> dict:
    return {
        'extension': [
            expected_version(),
            {
                'url': 'http://fhir-registry.smarthealthit.org/StructureDefinition/nlp-date-processed',
                'valueDate': datetime.datetime.now(datetime.timezone.utc).date().isoformat(),
            },
        ],
        'url': 'http://fhir-registry.smarthealthit.org/StructureDefinition/nlp-algorithm',
    }


def example_derivation_reference() -> dict:
    """
    Jamie Jones proposed example
    """
    return {'extension': [{'extension': [{'url': 'reference',
                                          'valueReference': {'display': 'note',
                                                             'reference': 'DocumentReference/episode-summary'}},
                                         {'url': 'offset', 'valueInteger': 20},
                                         {'url': 'length', 'valueInteger': 5},
                                         {'url': 'algorithm', 'valueString': 'however specifically we want to log the '
                                                                             'fact that cTAKES was used'},
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
        self.assertDictEqual(expected_version(),
                             text2fhir.nlp_version_client().as_json())

    def test_nlp_algorithm(self):
        """
        Test the FHIR Extension for "nlp-algorithm" is the proposed format.
        """
        ver = text2fhir.nlp_version_client()
        algo = text2fhir.nlp_algorithm(ver)

        self.assertDictEqual(expected_algorithm(), algo.as_json())

    def test_fhir_concept(self):
        """
        Test construction of FHIR CodeableConcept the conventional way
        """
        vomiting1 = text2fhir.fhir_coding('http://snomed.info/sct', '249497008', 'Vomiting symptom (finding)')
        vomiting2 = text2fhir.fhir_coding('http://snomed.info/sct', '300359004', 'Finding of vomiting (finding)')

        # without extension
        as_fhir = text2fhir.fhir_concept('vomiting', [vomiting1, vomiting2])
        expected_concept = {
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
        }
        self.assertDictEqual(expected_concept, as_fhir.as_json())

        # with extension
        position = text2fhir.nlp_text_position(58, 66)
        as_fhir = text2fhir.fhir_concept('vomiting', [vomiting1, vomiting2], position)
        expected_concept.update({
            'extension': [
                {
                    'extension': [
                        {
                            'url': 'begin',
                            'valueInteger': 58
                        },
                        {
                            'url': 'end',
                            'valueInteger': 66
                        },
                    ],
                    'url': 'http://fhir-registry.smarthealthit.org/StructureDefinition/nlp-text-position',
                },
            ],
        })
        self.assertDictEqual(expected_concept, as_fhir.as_json())

    def test_nlp_concept(self):
        """
        Test construction of FHIR CodeableConcept the easy way using ctakesclient helper functions
        """
        ctakes_json = example_ctakes()

        concepts = [text2fhir.nlp_concept(match).as_json() for match in ctakes_json.list_match()]
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
            'extension': [
                {
                    'extension': [
                        {
                            'url': 'begin',
                            'valueInteger': 442,
                        },
                        {
                            'url': 'end',
                            'valueInteger': 457,
                        },
                    ],
                    'url': 'http://fhir-registry.smarthealthit.org/StructureDefinition/nlp-text-position',
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

        symptoms = [
            text2fhir.nlp_observation(subject_id, encounter_id, symptom).as_json()
            for symptom in ctakes_json.list_sign_symptom()
        ]
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
                'extension': [
                    {
                        'extension': [
                            {
                                'url': 'begin',
                                'valueInteger': 21,
                            },
                            {
                                'url': 'end',
                                'valueInteger': 30,
                            },
                        ],
                        'url': 'http://fhir-registry.smarthealthit.org/StructureDefinition/nlp-text-position',
                    },
                ],
                'text': 'Complaint',
            },
            'encounter': {
                'reference': 'Encounter/5678',
            },
            'modifierExtension': [
                expected_algorithm(),
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

        medications = [
            text2fhir.nlp_medication(subject_id, encounter_id, medication).as_json()
            for medication in ctakes_json.list_medication()
        ]
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
                'extension': [
                    {
                        'extension': [
                            {
                                'url': 'begin',
                                'valueInteger': 442,
                            },
                            {
                                'url': 'end',
                                'valueInteger': 457,
                            },
                        ],
                        'url': 'http://fhir-registry.smarthealthit.org/StructureDefinition/nlp-text-position',
                    },
                ],
                'text': 'chewable tablet',
            },
            'modifierExtension': [
                expected_algorithm(),
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

        all_fhir = [as_fhir.as_json() for as_fhir in text2fhir.nlp_fhir(subject_id, encounter_id, ctakes_json)]
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
            'extension': [
                {
                    'extension': [
                        {
                            'url': 'begin',
                            'valueInteger': 198,
                        },
                        {
                            'url': 'end',
                            'valueInteger': 203,
                        },
                    ],
                    'url': 'http://fhir-registry.smarthealthit.org/StructureDefinition/nlp-text-position',
                },
            ],
            'text': 'wrist',
        }, bodysites[0])


if __name__ == '__main__':
    unittest.main()
