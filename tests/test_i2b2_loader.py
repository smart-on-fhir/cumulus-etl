"""Tests for loading i2b2 data"""

import os
import shutil
import tempfile
import unittest

from cumulus import store
from cumulus.loaders.i2b2 import loader


class TestI2b2Loader(unittest.TestCase):
    """Test case for loading i2b2 data"""

    async def test_missing_files(self):
        """Verify that we don't error out if files are missing, we just ignore the ones that are"""
        with tempfile.TemporaryDirectory() as tmpdir:
            root = store.Root(tmpdir)
            i2b2_loader = loader.I2b2Loader(root, 5)

            # Write one file, but not others, just to confirm we do a partial read if possible.
            vitals = f"{os.path.dirname(__file__)}/data/simple/i2b2-input/observation_fact_vitals.csv"
            shutil.copy(vitals, tmpdir)

            loaded_dir = await i2b2_loader.load_all(["Observation", "Patient"])

            self.assertEqual(["Observation.ndjson"], os.listdir(loaded_dir.name))
