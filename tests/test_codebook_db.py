import json
import logging
import unittest
from etl import codebook
from etl import common

class TestCodebookDB(unittest.TestCase):

    def test_hash_clinical_text(self):
        actual = common.hash_clinical_text(
            'Chief Complaint: patient c/o difficulty breathing, fever, and swelling. Denies cough.')

        # https://www.md5hashgenerator.com
        expected = '991ca427187c51e9b5850204cfda71a4'

        self.assertEqual(expected, actual, f'MD5 hash did not match, expected {expected} actual {actual}')

    def test_codebook_entry(self):
        note1 = common.hash_clinical_text('chief complaint: patient complains of fever and chills but denies cough')
        note2 = common.hash_clinical_text('discharge diagnosis: U07.1 COVID-19')

        patientA = '000111'
        encounter1 = 'ABCDEFG'
        encounter2 = 'HIJKLMN'

        patientB = '222333'
        encounter3 = 'OPQRST'
        encounter4 = 'UVWXYZ'

        cb = codebook.CodebookDB()
        #
        cb.docref(patientA, encounter1, note1)
        cb.docref(patientA, encounter1, note2)
        cb.encounter(patientA, encounter2)

        # suppress duplicates
        cb.docref(patientA, encounter1, note1)
        cb.docref(patientA, encounter1, note1)
        cb.docref(patientA, encounter1, note1)

        # other patient
        cb.encounter(patientB, encounter3)
        cb.encounter(patientB, encounter4)

        logging.debug('######## to_json ')
        logging.debug(json.dumps(cb.__dict__, indent=4))

        to_json = cb.__dict__
        from_json = codebook.CodebookDB(to_json)

        logging.debug('######## from_json ')
        logging.debug(json.dumps(from_json.__dict__, indent=4))

        self.assertEqual(to_json, from_json.__dict__, 'Codebooks do not match before/after reloading.')