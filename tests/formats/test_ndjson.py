"""Tests for ndjson output format support"""

import os

import ddt

from cumulus_etl import common, store
from cumulus_etl.formats.ndjson import NdjsonFormat
from tests import utils


@ddt.ddt
class TestNdjsonFormat(utils.AsyncTestCase):
    """
    Test case for the ndjson format writer.

    i.e. tests for ndjson.py

    Note that a lot of the basics of that formatter gets tested in other unit tests.
    This class is mostly just for the less typical edge cases.
    """

    def setUp(self):
        super().setUp()
        self.output_tempdir = self.make_tempdir()
        self.root = store.Root(self.output_tempdir)

    @ddt.data(
        (None, True),
        ([], True),
        (["condition/condition.000.ndjson", "condition/condition.111.ndjson"], False),
        (["condition/readme.txt"], False),
        (["my-novel.txt"], False),
    )
    @ddt.unpack
    def test_disallows_existing_files(self, files: None | list[str], is_ok: bool):
        """Verify that we bail out if any files already exist in the output"""
        if files is None:
            # This means we don't want any folder at all for the test
            os.rmdir(self.root.path)
        else:
            for file in files:
                pieces = file.split("/")
                if len(pieces) > 1:
                    os.makedirs(self.root.joinpath(pieces[0]), exist_ok=True)
                # write any old content in there, we just want to create the file.
                common.write_text(self.root.joinpath(file), "Hello!")

        if is_ok:
            NdjsonFormat.initialize_class(self.root)
            # Test that we didn't adjust/remove/create any of the files on disk
            if files is None:
                self.assertFalse(os.path.exists(self.root.path))
            else:
                self.assertEqual(files or [], os.listdir(self.root.path))
        else:
            with self.assertRaises(SystemExit):
                NdjsonFormat.initialize_class(self.root)

    def test_writes_deleted_ids(self):
        """Verify that we write a table metadata file with deleted IDs"""
        meta_path = f"{self.root.joinpath('condition')}/condition.meta"

        # Test with a fresh directory
        formatter = NdjsonFormat(self.root, "condition")
        formatter.delete_records({"b", "a"})
        metadata = common.read_json(meta_path)
        self.assertEqual(metadata, {"deleted": ["a", "b"]})

        # Confirm we append to existing metadata, should we ever need to
        metadata["extra"] = "bonus metadata!"
        common.write_json(meta_path, metadata)
        formatter.delete_records({"c"})
        metadata = common.read_json(meta_path)
        self.assertEqual(metadata, {"deleted": ["a", "b", "c"], "extra": "bonus metadata!"})
