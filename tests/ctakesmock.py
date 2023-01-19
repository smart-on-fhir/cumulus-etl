"""Holds mock methods for ctakesclient.client"""

import unittest
from typing import List
from unittest import mock

from ctakesclient import typesystem


class CtakesMixin(unittest.TestCase):
    """
    Add this mixin to your test class to properly mock out calls to the NLP server

    See the docstring for fake_ctakes_extract() for guidance on the fake results this generates.
    """

    def setUp(self):
        super().setUp()

        version_patcher = mock.patch("ctakesclient.__version__", new="1.2.0")  # just freeze this in place
        self.addCleanup(version_patcher.stop)
        version_patcher.start()

        nlp_patcher = mock.patch("cumulus.ctakes.ctakesclient.client.extract", side_effect=fake_ctakes_extract)
        self.addCleanup(nlp_patcher.stop)
        self.nlp_mock = nlp_patcher.start()

        cnlp_patcher = mock.patch(
            "cumulus.ctakes.ctakesclient.transformer.list_polarity", side_effect=fake_transformer_list_polarity
        )
        self.addCleanup(cnlp_patcher.stop)
        self.cnlp_mock = cnlp_patcher.start()


def fake_ctakes_extract(sentence: str) -> typesystem.CtakesJSON:
    """
    Simple fake response from cTAKES

    The output is fairly static:
    - The 1st word is marked as a 'patient'
    - The 2nd word is marked as a 'fever'
    - The 3rd word is marked as a 'itch'
    - The rest are ignored
    """
    words = sentence.split()

    if len(words) < 3:
        return typesystem.CtakesJSON()

    patient_word = words[0]
    patient_begin = 0
    patient_end = len(patient_word)

    fever_word = words[1]
    fever_begin = patient_end + 1
    fever_end = fever_begin + len(fever_word)

    itch_word = words[2]
    itch_begin = fever_end + 1
    itch_end = itch_begin + len(itch_word)

    # Response template inspired by response to "Patient has a fever and an itch"
    response = {
        "SignSymptomMention": [
            # A first normal symptom match on fever
            {
                "begin": fever_begin,
                "end": fever_end,
                "text": fever_word,
                "polarity": 0,
                "conceptAttributes": [
                    {"code": "386661006", "cui": "C0015967", "codingScheme": "SNOMEDCT_US", "tui": "T184"},
                    {"code": "50177009", "cui": "C0015967", "codingScheme": "SNOMEDCT_US", "tui": "T184"},
                ],
                "type": "SignSymptomMention",
            },
            # A second covid symptom match on nausea (for the same word as above), just for more matches during testing
            {
                "begin": fever_begin,
                "end": fever_end,
                "text": fever_word,
                "polarity": 0,
                "conceptAttributes": [
                    {"code": "422587007", "cui": "C0027497", "codingScheme": "SNOMEDCT_US", "tui": "T184"},
                ],
                "type": "SignSymptomMention",
            },
            # This itch word will be stripped from symptom list after our ETL code receives it, since it is non-covid
            {
                "begin": itch_begin,
                "end": itch_end,
                "text": itch_word,
                "polarity": 0,
                "conceptAttributes": [
                    {"code": "418290006", "cui": "C0033774", "codingScheme": "SNOMEDCT_US", "tui": "T184"},
                    {"code": "279333002", "cui": "C0033774", "codingScheme": "SNOMEDCT_US", "tui": "T184"},
                    {"code": "424492005", "cui": "C0033774", "codingScheme": "SNOMEDCT_US", "tui": "T184"},
                    {"code": "418363000", "cui": "C0033774", "codingScheme": "SNOMEDCT_US", "tui": "T184"},
                ],
                "type": "SignSymptomMention",
            },
        ],
        "IdentifiedAnnotation": [
            {
                "begin": patient_begin,
                "end": patient_end,
                "text": patient_word,
                "polarity": 0,
                "conceptAttributes": [
                    {"code": "n/a", "cui": "CE_64", "codingScheme": "custom", "tui": "T0NA"},
                ],
                "type": "IdentifiedAnnotation",
            },
            {
                "begin": fever_begin,
                "end": fever_end,
                "text": fever_word,
                "polarity": 0,
                "conceptAttributes": [
                    {"code": "n/a", "cui": "a0_27", "codingScheme": "custom", "tui": "T0NA"},
                    {"code": "n/a", "cui": "DIS_31", "codingScheme": "custom", "tui": "T0NA"},
                    {"code": "n/a", "cui": "a0_36", "codingScheme": "custom", "tui": "T0NA"},
                ],
                "type": "IdentifiedAnnotation",
            },
        ],
    }

    return typesystem.CtakesJSON(response)


def fake_transformer_list_polarity(sentence: str, spans: List[tuple]) -> List[typesystem.Polarity]:
    """
    Simple fake response from cNLP

    The output is quite static, and matches the above fake cTAKES results. By default, we're neg, pos, pos.
    """
    del sentence
    return [typesystem.Polarity.pos] * len(spans)
