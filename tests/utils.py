"""Various test helper methods"""

import json
import os
import unittest


class TreeCompareMixin(unittest.TestCase):
    """Mixin that provides a simple way to diff two trees of files"""

    def setUp(self):
        super().setUp()

        # you'll always want this when debugging
        self.maxDiff = None  # pylint: disable=invalid-name

    def assert_file_tree_equal(self, dircmp):
        """
        Compare a tree of file content.

        filecmp.dircmp by itself likes to only do shallow comparisons that
        notice changes like timestamps. But we want the contents themselves.
        """
        self.assertEqual([], dircmp.left_only, dircmp.left)
        self.assertEqual([], dircmp.right_only, dircmp.right)

        for filename in dircmp.common_files:
            left_path = os.path.join(dircmp.left, filename)
            right_path = os.path.join(dircmp.right, filename)

            with open(left_path, "rb") as f:
                left_contents = f.read()
            with open(right_path, "rb") as f:
                right_contents = f.read()

            # Try to avoid comparing json files byte-for-byte. We may reasonably
            # change formatting, or even want the test files in an
            # easier-to-read format than the actual output files. In theory all
            # json files are equal once parsed.
            if filename.endswith(".json"):
                left_json = json.loads(left_contents.decode("utf8"))
                right_json = json.loads(right_contents.decode("utf8"))
                self.assertEqual(left_json, right_json, f"{right_path} vs {left_path}")
            elif filename.endswith(".ndjson"):
                left_split = left_contents.decode("utf8").splitlines()
                right_split = right_contents.decode("utf8").splitlines()
                left_rows = list(map(json.loads, left_split))
                right_rows = list(map(json.loads, right_split))
                self.assertEqual(left_rows, right_rows, f"{right_path} vs {left_path}")
            else:
                self.assertEqual(left_contents, right_contents, f"{right_path} vs {left_path}")

        for subdircmp in dircmp.subdirs.values():
            self.assert_file_tree_equal(subdircmp)
