"""Tests for the internal CodebookDB class"""

import json
import logging
import unittest
from cumulus import codebook


class TestCodebookDB(unittest.TestCase):
    """Test case for the CodebookDB class"""

    def test_codebook_entry(self):
        patient1 = '000111'
        encounter1 = 'HIJKLMN'

        patient2 = '222333'
        encounter2 = 'OPQRST'
        encounter3 = 'UVWXYZ'

        cb = codebook.CodebookDB()
        #
        cb.encounter(patient1, encounter1)

        # other patient
        cb.encounter(patient2, encounter2)
        cb.encounter(patient2, encounter3)

        logging.debug('######## to_json ')
        logging.debug(json.dumps(cb.__dict__, indent=4))

        to_json = cb.__dict__
        from_json = codebook.CodebookDB(to_json)

        logging.debug('######## from_json ')
        logging.debug(json.dumps(from_json.__dict__, indent=4))

        self.assertEqual(to_json, from_json.__dict__,
                         'Codebooks do not match before/after reloading.')
