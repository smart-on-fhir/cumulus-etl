"""Tests for loading i2b2 data"""

import os
import shutil
import tempfile

from cumulus_etl import store
from cumulus_etl.loaders.i2b2 import loader
from tests.utils import AsyncTestCase


class TestI2b2Loader(AsyncTestCase):
    """Test case for loading i2b2 data"""

    async def test_missing_files(self):
        """Verify that we don't error out if files are missing, we just ignore the ones that are"""
        with tempfile.TemporaryDirectory() as tmpdir:
            root = store.Root(tmpdir)
            i2b2_loader = loader.I2b2Loader(root)

            # Write one file, but not others, just to confirm we do a partial read if possible.
            vitals = f"{self.datadir}/i2b2/input/observation_fact_vitals.csv"
            shutil.copy(vitals, tmpdir)

            loaded_dir = await i2b2_loader.load_all(["Observation", "Patient"])

            self.assertEqual(["Observation.1.ndjson"], os.listdir(loaded_dir.name))
