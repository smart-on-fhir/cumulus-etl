"""Tests for the internal CodebookDB class"""

import os
import tempfile
import unittest
from unittest import mock

import ddt

from cumulus.deid.codebook import Codebook, CodebookDB


def assert_empty_db(db: CodebookDB):
    assert {
        'version': 1,
        'Encounter': {},
        'Patient': {},
    } == db.mapping


@ddt.ddt
@mock.patch('cumulus.deid.codebook.secrets.token_hex', new=lambda x: '31323334')
class TestCodebook(unittest.TestCase):
    """Test case for the Codebook class"""

    @ddt.data('Patient', 'Encounter')
    def test_reversible_type(self, resource_type):
        cb = Codebook()
        fake_id = cb.fake_id(resource_type, '1')
        self.assertEqual(fake_id, cb.fake_id(resource_type, '1'))
        self.assertNotEqual(fake_id, cb.fake_id(resource_type, '2'))
        self.assertNotEqual(fake_id, cb.fake_id('Observation', '1'))
        self.assertEqual(fake_id, cb.db.mapping[resource_type]['1'])

    def test_hashed_type(self):
        cb = Codebook()
        fake_id = cb.fake_id('Condition', '1')
        self.assertEqual(fake_id, cb.fake_id('Condition', '1'))
        self.assertNotEqual(fake_id, cb.fake_id('Condition', '2'))
        self.assertEqual(fake_id, cb.fake_id('Observation', '1'))  # '1' hashes the same across types
        self.assertEqual('ee1b8555df1476e7512bc31940148a7821edae6e152e92037e6e8d7e948800a4', fake_id)
        self.assertEqual('31323334', cb.db.mapping.get('id_salt'))

    def test_missing_db_file(self):
        """Ensure we gracefully handle a saved db file that doesn't exist yet"""
        cb = Codebook('/missing-codebook-file.json')
        assert_empty_db(cb.db)


@ddt.ddt
class TestCodebookDB(unittest.TestCase):
    """Test case for the CodebookDB class"""

    def test_empty(self):
        assert_empty_db(CodebookDB())

    @ddt.data('patient', 'encounter')
    def test_basic_ids(self, method):
        db = CodebookDB()
        call = getattr(db, method)
        v1 = call('1')
        self.assertIsNotNone(v1)
        self.assertGreater(len(v1), 0)
        self.assertEqual(v1, call('1'))
        self.assertNotEqual(v1, call('2'))

    def test_save_and_load(self):
        db = CodebookDB()
        p1 = db.patient('1')
        p2 = db.patient('2')
        e1 = db.encounter('1')
        e2 = db.encounter('2')

        with tempfile.TemporaryDirectory() as tmpdir:
            path = os.path.join(tmpdir, 'cb.json')
            db.save(path)
            db2 = CodebookDB(path)

        self.assertEqual(p1, db2.patient('1'))
        self.assertEqual(p2, db2.patient('2'))
        self.assertEqual(e1, db2.encounter('1'))
        self.assertEqual(e2, db2.encounter('2'))

    def test_version0(self):
        script_dir = os.path.dirname(__file__)
        db_path = os.path.join(script_dir, 'data', 'codebook0.json')
        db = CodebookDB(db_path)

        # Patients
        self.assertEqual('1de9ea66-70d3-da1f-c735-df5ef7697fb9', db.patient('323456'))
        self.assertEqual('c07666e7-9ef6-62f8-8dee-ba7ec0ea7563', db.patient('3123456'))
        self.assertEqual('861abd5d-c0ae-6995-27aa-e362c6c0ac72', db.patient('3234567'))

        # Encounters
        self.assertEqual('458c3cdb-2d66-5a7b-0a4a-db41ce779a93', db.encounter('21'))
        self.assertEqual('175e9941-2607-ad5f-76ab-14759da618fd', db.encounter('22'))
        self.assertEqual('d30aad4b-4503-8e22-0bc4-621b94398520', db.encounter('23'))
        self.assertEqual('08f0ebd4-950c-ddd9-ce97-b5bdf073eed1', db.encounter('24'))
        self.assertEqual('af1e6186-3f9a-1fa9-3c73-cfa56c84a056', db.encounter('25'))
        self.assertEqual('4e9e5e14-a289-0d0d-81ee-8062b8b984c3', db.encounter('212'))

        # But we are now version 1 going forward
        self.assertEqual(1, db.mapping['version'])
