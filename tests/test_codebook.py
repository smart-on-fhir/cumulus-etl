import json
import unittest
import i2b2
from etl_i2b2_ctakes import codebook

class TestCodebook(unittest.TestCase):

    def test_codebook_entry(self):
        note1 = codebook.hash_clinical_text('chief complaint: patient complains of fever and chills but denies cough')
        note2 = codebook.hash_clinical_text('discharge diagnosis: U07.1 COVID-19')

        patientA = '000111'
        encounter1 = 'ABCDEFG'
        encounter2 = 'HIJKLMN'

        patientB = '222333'
        encounter3 = 'OPQRST'
        encounter4 = 'UVWXYZ'

        cb = codebook.Codebook()
        #
        cb.note(patientA, encounter1, note1)
        cb.note(patientA, encounter1, note2)
        cb.encounter(patientA, encounter2)

        # suppress duplicates
        cb.note(patientA, encounter1, note1)
        cb.note(patientA, encounter1, note1)
        cb.note(patientA, encounter1, note1)

        # other patient
        cb.encounter(patientB, encounter3)
        cb.encounter(patientB, encounter4)

        print('######## to_json ')
        print(json.dumps(cb.__dict__, indent=4))

        to_json = cb.__dict__
        from_json = codebook.Codebook(to_json)

        print('######## from_json ')
        print(json.dumps(from_json.__dict__, indent=4))

        self.assertEqual(to_json, from_json.__dict__, 'Dictionary does not match')





    def test_deid_link(self):
        links = [codebook.deid_link() for i in range(0,100)]
        self.assertEqual(100, len(set(links)), '100 generated links should all be unique')

    def test_deid_i2b2(self):
        # https://www.md5hashgenerator.com/
        expected = '991ca427187c51e9b5850204cfda71a4'

        example = {
            'PATIENT_NUM': 'MRN_12345',
            'ENCOUNTER_NUM': 'Visit_6789',
            'OBSERVATION_BLOB': 'Chief Complaint: patient c/o difficulty breathing, fever, and swelling. Denies cough.',
            'CONCEPT_CD': None,
            'START_DATE': None,
            'END_DATE': None}

        actual = codebook.deid_i2b2(i2b2.ObservationFact(example))

        self.assertEqual(expected, actual, f'MD5 hash did not match, expected {expected} actual {actual}')


if __name__ == '__main__':
    unittest.main()
