import unittest
import i2b2
from etl_i2b2_ctakes import codebook

class TestCodebook(unittest.TestCase):

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
