"""Tests for loading i2b2 data"""

import os
import shutil
import tempfile

from cumulus_etl import common, store
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

            results = await i2b2_loader.load_resources({"Observation", "Patient"})

            self.assertEqual(["Observation.1.ndjson"], os.listdir(results.path))

    async def test_duplicate_ids(self):
        """Verify that we ignore duplicate IDs"""
        with tempfile.TemporaryDirectory() as tmpdir:
            root = store.Root(tmpdir)
            i2b2_loader = loader.I2b2Loader(root)

            common.write_text(
                f"{tmpdir}/patient_dimension.csv",
                "PATIENT_NUM,BIRTH_DATE\n123,1982-10-16\n123,1983-11-17\n456,2000-01-13\n",
            )

            results = await i2b2_loader.load_resources({"Patient"})
            rows = common.read_resource_ndjson(store.Root(results.path), "Patient")
            values = [(r["id"], r["birthDate"]) for r in rows]
            self.assertEqual(values, [("123", "1982-10-16"), ("456", "2000-01-13")])

    async def test_detect_resources(self):
        """Verify we can inspect a folder and find all resources."""
        with tempfile.TemporaryDirectory() as tmpdir:
            common.write_text(f"{tmpdir}/visit_dimension.csv", "")
            common.write_text(f"{tmpdir}/unrelated.csv", "")
            common.write_text(f"{tmpdir}/observation_fact_lab_views.csv", "")

            i2b2_loader = loader.I2b2Loader(store.Root(tmpdir))
            resources = await i2b2_loader.detect_resources()

        self.assertEqual(resources, {"Encounter", "Observation"})

    async def test_detect_resources_tcp(self):
        """Verify we skip trying to detect resources before exporting from oracle."""
        i2b2_loader = loader.I2b2Loader(store.Root("tcp://localhost"))
        self.assertIsNone(await i2b2_loader.detect_resources())
