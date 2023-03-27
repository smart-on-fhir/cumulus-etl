"""Tests for chart_review/cli.py"""

import filecmp
import json
import os
import shutil
import tempfile
from typing import Iterable, List, Set, Tuple

import respx

from cumulus import cli, common, errors
from tests.utils import AsyncTestCase

# These are pre-computed fake IDs using the salt "1234"
ANON_P1 = "70339b189b5646dab20b2e2e46f8cef66da257e992e04862d2e88e1e1f1b1bd4"
ANON_P2 = "7a3a3360942dbbf6f0f119797db6a5d38cc57dfe8e727fabd5912a3c478b6cc0"
ANON_D1 = "d2f6df5b3d1bde3d6a8c3fde68ef45fb46991b315ac3f5065e4a7aae42f17819"
ANON_D2 = "4b5351bd9334232e8bf6d5dd0bfe65ba519cf68a54da85d289c2009b9b6dc668"
ANON_D3 = "bf1f931f34052b61e3c0ddd290e483b980672db9d6d7705544c0bb130fa7e577"


class TestChartReview(AsyncTestCase):
    """Tests for high-level chart review support."""

    def setUp(self):
        super().setUp()

        tmpdir = tempfile.mkdtemp()
        # Comment out this next line when debugging, to persist directory
        self.addCleanup(shutil.rmtree, tmpdir)

        script_dir = os.path.dirname(__file__)
        self.data_dir = os.path.join(script_dir, "data/simple")
        self.input_path = os.path.join(self.data_dir, "ndjson-input")
        self.phi_path = os.path.join(tmpdir, "phi")
        self.export_path = os.path.join(tmpdir, "export")

        # Write some initial cached patient mappings, so we can reverse-engineer them
        os.makedirs(self.phi_path)
        common.write_json(
            f"{self.phi_path}/codebook.json",
            {
                "version": 1,
                "id_salt": "1234",
            },
        )
        common.write_json(
            f"{self.phi_path}/codebook-cached-mappings.json",
            {
                "Patient": {
                    "P1": ANON_P1,
                    "P2": ANON_P2,
                }
            },
        )

        filecmp.clear_cache()

    async def run_chart_review(
        self,
        input_path=None,
        phi_path=None,
        anon_docrefs=None,
        docrefs=None,
    ) -> None:
        args = [
            "chart-review",
            input_path or self.input_path,
            phi_path or self.phi_path,
            f"--export-to={self.export_path}",
        ]
        if anon_docrefs:
            args += ["--anon-docrefs", anon_docrefs]
        if docrefs:
            args += ["--docrefs", docrefs]
        await cli.main(args)

    @staticmethod
    def mock_search_url(patient: str, doc_ids: Iterable[str]) -> None:
        bundle = {
            "resourceType": "Bundle",
            "entry": [
                {
                    "resource": {
                        "resourceType": "DocumentReference",
                        "id": doc_id,
                    }
                }
                for doc_id in doc_ids
            ],
        }

        respx.get(f"https://localhost/DocumentReference?patient={patient}&_elements=content").respond(json=bundle)

    @staticmethod
    def mock_read_url(doc_id: str, code: int = 200) -> None:
        docref = {
            "resourceType": "DocumentReference",
            "id": doc_id,
        }
        respx.get(f"https://localhost/DocumentReference/{doc_id}").respond(status_code=code, json=docref)

    @staticmethod
    def write_anon_docrefs(path: str, ids: List[Tuple[str, str]]) -> None:
        """Fills a file with the provided docref ids of (docref_id, patient_id) tuples"""
        lines = ["docref_id,patient_id"] + [f"{x[0]},{x[1]}" for x in ids]
        with open(path, "w", encoding="utf8") as f:
            f.write("\n".join(lines))

    @staticmethod
    def write_real_docrefs(path: str, ids: List[str]) -> None:
        """Fills a file with the provided docref ids"""
        lines = ["docref_id"] + ids
        with open(path, "w", encoding="utf8") as f:
            f.write("\n".join(lines))

    def get_exported_ids(self) -> Set[str]:
        with common.open_file(os.path.join(self.export_path, "DocumentReference.ndjson"), "r") as f:
            return {json.loads(line)["id"] for line in f}

    async def test_real_and_fake_docrefs_conflict(self):
        """Verify that you can't pass in both real and fake docrefs"""
        with self.assertRaises(SystemExit) as cm:
            await self.run_chart_review(anon_docrefs="foo", docrefs="bar")
        self.assertEqual(errors.ARGS_CONFLICT, cm.exception.code)

    @respx.mock
    async def test_anon_docrefs(self):
        self.mock_search_url("P1", ["NotMe", "D1", "NotThis", "D3"])
        self.mock_search_url("P2", ["D2"])

        with tempfile.NamedTemporaryFile() as file:
            self.write_anon_docrefs(
                file.name,
                [
                    (ANON_D1, ANON_P1),
                    (ANON_D2, ANON_P2),
                    (ANON_D3, ANON_P1),
                    ("unknown-doc", "unknown-patient"),  # gracefully ignored
                ],
            )
            await self.run_chart_review(input_path="https://localhost", anon_docrefs=file.name)

        self.assertEqual({"D1", "D2", "D3"}, self.get_exported_ids())

    @respx.mock
    async def test_real_docrefs(self):
        self.mock_read_url("D1")
        self.mock_read_url("D2")
        self.mock_read_url("D3")
        self.mock_read_url("unknown-doc", code=404)

        with tempfile.NamedTemporaryFile() as file:
            self.write_real_docrefs(file.name, ["D1", "D2", "D3", "unknown-doc"])
            await self.run_chart_review(input_path="https://localhost", docrefs=file.name)

        self.assertEqual({"D1", "D2", "D3"}, self.get_exported_ids())
