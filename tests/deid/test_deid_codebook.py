"""Tests for the internal CodebookDB class"""

import os
import tempfile
from unittest import mock

import ddt

from cumulus_etl import common
from cumulus_etl.deid.codebook import Codebook, CodebookDB
from tests import utils


def assert_empty_db(db: CodebookDB):
    assert {
        "version": 1,
        "id_salt": "31323334",
    } == db.settings


@ddt.ddt
@mock.patch("cumulus_etl.deid.codebook.secrets.token_hex", new=lambda x: "31323334")
class TestCodebook(utils.AsyncTestCase):
    """Test case for the Codebook class"""

    @ddt.data("Patient", "Encounter")
    def test_reversible_type(self, resource_type):
        cb = Codebook()
        fake_id = cb.fake_id(resource_type, "1")
        self.assertEqual(fake_id, cb.fake_id(resource_type, "1"))
        self.assertNotEqual(fake_id, cb.fake_id(resource_type, "2"))
        self.assertNotEqual(fake_id, cb.fake_id("Observation", "2"))
        self.assertEqual(fake_id, cb.db.cached_mapping[resource_type]["1"])

    def test_hashed_type(self):
        cb = Codebook()
        fake_id = cb.fake_id("Condition", "1")
        self.assertEqual(fake_id, cb.fake_id("Condition", "1"))
        self.assertNotEqual(fake_id, cb.fake_id("Condition", "2"))
        # '1' hashes the same across types
        self.assertEqual(fake_id, cb.fake_id("Observation", "1"))
        self.assertEqual(
            "ee1b8555df1476e7512bc31940148a7821edae6e152e92037e6e8d7e948800a4", fake_id
        )
        self.assertEqual("31323334", cb.db.settings.get("id_salt"))

    def test_missing_db_file(self):
        """Ensure we gracefully handle a saved db file that doesn't exist yet"""
        cb = Codebook("/")
        assert_empty_db(cb.db)

    def test_context_manager(self):
        tmpdir = self.make_tempdir()
        with Codebook(tmpdir):
            self.assertFalse(os.path.exists(f"{tmpdir}/codebook.json"))
        self.assertTrue(os.path.exists(f"{tmpdir}/codebook.json"))


@ddt.ddt
@mock.patch("cumulus_etl.deid.codebook.secrets.token_hex", new=lambda x: "31323334")
class TestCodebookDB(utils.AsyncTestCase):
    """Test case for the CodebookDB class"""

    def test_empty(self):
        assert_empty_db(CodebookDB())

    @ddt.data("patient", "encounter")
    def test_basic_ids(self, method):
        db = CodebookDB()
        call = getattr(db, method)
        v1 = call("1")
        self.assertIsNotNone(v1)
        self.assertGreater(len(v1), 0)
        self.assertEqual(v1, call("1"))
        self.assertNotEqual(v1, call("2"))

    def test_use_legacy_random_mappings(self):
        """Verify that we keep and use any old patient/encounter mappings that were wholly random and not hash based"""
        with tempfile.TemporaryDirectory() as tmpdir:
            common.write_json(
                os.path.join(tmpdir, "codebook.json"),
                {
                    "version": 1,
                    "Encounter": {
                        "42": "yup",
                    },
                    "Patient": {
                        "abc": "xyz",
                    },
                },
            )

            db = CodebookDB(tmpdir)
            self.assertEqual(db.encounter("42"), "yup")
            self.assertEqual(db.patient("abc"), "xyz")

            # confirm we don't cache legacy mappings
            self.assertNotIn("42", db.cached_mapping["Encounter"])
            self.assertNotIn("abc", db.cached_mapping["Patient"])

            # Confirm we do inject them into the reverse mapping
            self.assertEqual(db.get_reverse_mapping("Encounter"), {"yup": "42"})

    def test_save_and_load(self):
        tmpdir = self.make_tempdir()
        db = CodebookDB(tmpdir)
        p1 = db.patient("1")
        p2 = db.patient("2")
        e1 = db.encounter("1")
        e2 = db.encounter("2")

        expected_mapping = {
            "Encounter": {
                "1": e1,
                "2": e2,
            },
            "Patient": {
                "1": p1,
                "2": p2,
            },
        }

        db.save()

        # Verify that we saved the cached mapping to disk too
        self.assertEqual(
            expected_mapping,
            common.read_json(os.path.join(tmpdir, "codebook-cached-mappings.json")),
        )

        db2 = CodebookDB(tmpdir)

        # And that we loaded the cached mapping
        self.assertEqual(expected_mapping, db2.cached_mapping)

        self.assertEqual(p1, db2.patient("1"))
        self.assertEqual(p2, db2.patient("2"))
        self.assertEqual(e1, db2.encounter("1"))
        self.assertEqual(e2, db2.encounter("2"))

    def test_does_not_save_if_not_modified(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            # Confirm that an empty book starts modified
            db = CodebookDB(tmpdir)
            self.assertTrue(db.save())
            self.assertTrue(os.path.exists(f"{tmpdir}/codebook.json"))
            self.assertFalse(os.path.exists(f"{tmpdir}/codebook-cached-mappings.json"))
            codebook_mtime = os.path.getmtime(f"{tmpdir}/codebook.json")

            # But after a save, we are no longer modified
            self.assertFalse(db.save())

            # Resource hashes don't cause modification
            db.resource_hash("1")
            self.assertFalse(db.save())

            # Add a new patient, and we can save (because cached mapping changed)
            db.patient("1")
            self.assertTrue(db.save())
            self.assertTrue(os.path.exists(f"{tmpdir}/codebook-cached-mappings.json"))
            # main codebook shouldn't be written for just mappings changes
            self.assertEqual(codebook_mtime, os.path.getmtime(f"{tmpdir}/codebook.json"))

            # But if we make a call that doesn't modify the db, don't save
            db.patient("1")
            self.assertFalse(db.save())

            # And encounters
            db.encounter("1")
            self.assertTrue(db.save())

    def test_version0(self):
        db_path = os.path.join(self.datadir, "codebook0")
        db = CodebookDB(db_path)

        # Patients
        self.assertEqual("1de9ea66-70d3-da1f-c735-df5ef7697fb9", db.patient("323456"))
        self.assertEqual("c07666e7-9ef6-62f8-8dee-ba7ec0ea7563", db.patient("3123456"))
        self.assertEqual("861abd5d-c0ae-6995-27aa-e362c6c0ac72", db.patient("3234567"))

        # Encounters
        self.assertEqual("458c3cdb-2d66-5a7b-0a4a-db41ce779a93", db.encounter("21"))
        self.assertEqual("175e9941-2607-ad5f-76ab-14759da618fd", db.encounter("22"))
        self.assertEqual("d30aad4b-4503-8e22-0bc4-621b94398520", db.encounter("23"))
        self.assertEqual("08f0ebd4-950c-ddd9-ce97-b5bdf073eed1", db.encounter("24"))
        self.assertEqual("af1e6186-3f9a-1fa9-3c73-cfa56c84a056", db.encounter("25"))
        self.assertEqual("4e9e5e14-a289-0d0d-81ee-8062b8b984c3", db.encounter("212"))

        # But we are now version 1 going forward
        self.assertEqual(1, db.settings["version"])
