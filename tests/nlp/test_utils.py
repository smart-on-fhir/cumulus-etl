from cumulus_etl import nlp
from tests.utils import AsyncTestCase


class TestNlpUtils(AsyncTestCase):
    def test_get_note_subject_ref(self):
        # Simple happy path
        self.assertEqual(
            nlp.get_note_subject_ref({"subject": {"reference": "Patient/xyz"}}),
            "Patient/xyz",
        )

        # Bad/non-present reference
        self.assertIsNone(nlp.get_note_subject_ref({}))

        # Contained ref
        self.assertIsNone(nlp.get_note_subject_ref({"subject": {"reference": "#xyz"}}))

    def test_get_note_info(self):
        base = {"resourceType": "DocumentReference", "id": "d"}

        # Simple happy path
        everything = base | {
            "context": {"encounter": [{"reference": "Encounter/e"}]},
            "subject": {"reference": "Patient/p"},
        }
        self.assertEqual(nlp.get_note_info(everything), ("DocumentReference/d", "e", "Patient/p"))

        # No encounter
        just_subject = base | {"subject": {"reference": "Patient/p"}}
        with self.assertRaisesRegex(KeyError, "No encounters for note 'DocumentReference/d'"):
            nlp.get_note_info(just_subject)

        # No subject
        just_enc = base | {"context": {"encounter": [{"reference": "Encounter/e"}]}}
        with self.assertRaisesRegex(KeyError, "No subject for note 'DocumentReference/d'"):
            nlp.get_note_info(just_enc)
