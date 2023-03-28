"""Tests for chart_review/cli.py"""

import base64
import json
import os
import shutil
import tempfile
from typing import Iterable, List, Set, Tuple
from unittest import mock

import ddt
import respx

from cumulus import cli, common, errors, loaders
from cumulus.chart_review.labelstudio import LabelStudioNote

from tests.ctakesmock import CtakesMixin
from tests.utils import AsyncTestCase

# These are pre-computed fake IDs using the salt "1234"
ANON_P1 = "70339b189b5646dab20b2e2e46f8cef66da257e992e04862d2e88e1e1f1b1bd4"
ANON_P2 = "7a3a3360942dbbf6f0f119797db6a5d38cc57dfe8e727fabd5912a3c478b6cc0"
ANON_D1 = "d2f6df5b3d1bde3d6a8c3fde68ef45fb46991b315ac3f5065e4a7aae42f17819"
ANON_D2 = "4b5351bd9334232e8bf6d5dd0bfe65ba519cf68a54da85d289c2009b9b6dc668"
ANON_D3 = "bf1f931f34052b61e3c0ddd290e483b980672db9d6d7705544c0bb130fa7e577"
ANON_43 = "ee448bae9c010410080183e7914151097dd6b927dbd8a58efcf0859fc32907a6"
ANON_44 = "7a1f76190039b872a3016843e1712048cef3787931c000f0ea66b15962ccf65d"


@ddt.ddt
class TestChartReview(CtakesMixin, AsyncTestCase):
    """Tests for high-level chart review support."""

    def setUp(self):
        super().setUp()

        tmpdir = tempfile.mkdtemp()
        # Comment out this next line when debugging, to persist directory
        self.addCleanup(shutil.rmtree, tmpdir)

        script_dir = os.path.dirname(__file__)
        self.input_path = os.path.join(script_dir, "data/simple/input")
        self.phi_path = os.path.join(tmpdir, "phi")
        self.export_path = os.path.join(tmpdir, "export")

        self.bsv_path = os.path.join(tmpdir, "ctakes.bsv")
        common.write_text(self.bsv_path, "C0028081|T184|42984000|SNOMEDCT_US|night sweats|Sweats")

        self.token_path = os.path.join(tmpdir, "ls-token.txt")
        common.write_text(self.token_path, "abc123")

        self.ls_client_mock = self.patch("cumulus.chart_review.cli.LabelStudioClient")
        self.ls_client = self.ls_client_mock.return_value

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

    async def run_chart_review(
        self,
        input_path=None,
        phi_path=None,
        anon_docrefs=None,
        docrefs=None,
        nlp=True,
        philter=True,
        overwrite=False,
    ) -> None:
        args = [
            "chart-review",
            input_path or self.input_path,
            "https://localhost/labelstudio",
            phi_path or self.phi_path,
            f"--ctakes-overrides={self.ctakes_overrides.name}",
            f"--symptoms-bsv={self.bsv_path}",
            f"--export-to={self.export_path}",
            "--ls-project=21",
            f"--ls-token={self.token_path}",
            "--skip-init-checks",
        ]
        if anon_docrefs:
            args += ["--anon-docrefs", anon_docrefs]
        if docrefs:
            args += ["--docrefs", docrefs]
        if not nlp:
            args += ["--no-nlp"]
        if not philter:
            args += ["--no-philter"]
        if overwrite:
            args += ["--overwrite"]
        await cli.main(args)

    @staticmethod
    def make_docref(doc_id: str, text: str = None, content: List[dict] = None) -> dict:
        if content is None:
            text = text or "What's up doc?"
            content = [
                {
                    "attachment": {
                        "contentType": "text/plain",
                        "data": base64.standard_b64encode(text.encode("utf8")).decode("ascii"),
                    },
                }
            ]

        return {
            "resourceType": "DocumentReference",
            "id": doc_id,
            "content": content,
        }

    @staticmethod
    def mock_search_url(patient: str, doc_ids: Iterable[str]) -> None:
        bundle = {
            "resourceType": "Bundle",
            "entry": [
                {
                    "resource": TestChartReview.make_docref(doc_id),
                }
                for doc_id in doc_ids
            ],
        }

        respx.get(f"https://localhost/DocumentReference?patient={patient}&_elements=content").respond(json=bundle)

    @staticmethod
    def mock_read_url(doc_id: str, code: int = 200, **kwargs) -> None:
        docref = TestChartReview.make_docref(doc_id, **kwargs)
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

    def get_pushed_ids(self) -> Set[str]:
        notes = self.ls_client.push_tasks.call_args[0][0]
        return {n.ref_id for n in notes}

    async def test_real_and_fake_docrefs_conflict(self):
        """Verify that you can't pass in both real and fake docrefs"""
        with self.assertRaises(SystemExit) as cm:
            await self.run_chart_review(anon_docrefs="foo", docrefs="bar")
        self.assertEqual(errors.ARGS_CONFLICT, cm.exception.code)

    @respx.mock
    async def test_gather_anon_docrefs_from_server(self):
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
        self.assertEqual({"D1", "D2", "D3"}, self.get_pushed_ids())

    @respx.mock
    async def test_gather_real_docrefs_from_server(self):
        self.mock_read_url("D1")
        self.mock_read_url("D2")
        self.mock_read_url("D3")
        self.mock_read_url("unknown-doc", code=404)

        with tempfile.NamedTemporaryFile() as file:
            self.write_real_docrefs(file.name, ["D1", "D2", "D3", "unknown-doc"])
            await self.run_chart_review(input_path="https://localhost", docrefs=file.name)

        self.assertEqual({"D1", "D2", "D3"}, self.get_exported_ids())
        self.assertEqual({"D1", "D2", "D3"}, self.get_pushed_ids())

    @mock.patch("cumulus.chart_review.downloader.loaders.FhirNdjsonLoader")
    async def test_gather_all_docrefs_from_server(self, mock_loader):
        # Mock out the bulk export loading, as that's well tested elsewhere
        async def load_all(*args):
            del args
            return loaders.RealDirectory(self.input_path)

        load_all_mock = mock_loader.return_value.load_all
        load_all_mock.side_effect = load_all

        # Do the actual chart review push
        await self.run_chart_review(input_path="https://localhost")

        # Make sure we drive the bulk export correctly
        self.assertEqual(1, mock_loader.call_count)
        self.assertEqual("https://localhost", mock_loader.call_args[0][0].path)
        self.assertEqual(self.export_path, mock_loader.call_args[1]["export_to"])
        self.assertEqual([mock.call(["DocumentReference"])], load_all_mock.call_args_list)

        # Make sure we do read the result and push the docrefs out
        self.assertEqual({"43", "44"}, self.get_pushed_ids())

    async def test_gather_anon_docrefs_from_folder(self):
        with tempfile.NamedTemporaryFile() as file:
            self.write_anon_docrefs(
                file.name,
                [
                    (ANON_43, "patient"),  # patient doesn't matter
                    ("unknown-doc", "unknown-patient"),  # gracefully ignored
                ],
            )
            await self.run_chart_review(anon_docrefs=file.name)

        self.assertEqual({"43"}, self.get_exported_ids())
        self.assertEqual({"43"}, self.get_pushed_ids())

    async def test_gather_real_docrefs_from_folder(self):
        with tempfile.NamedTemporaryFile() as file:
            self.write_real_docrefs(file.name, ["44", "unknown-doc"])
            await self.run_chart_review(docrefs=file.name)

        self.assertEqual({"44"}, self.get_exported_ids())
        self.assertEqual({"44"}, self.get_pushed_ids())

    async def test_gather_all_docrefs_from_folder(self):
        await self.run_chart_review()
        self.assertEqual({"43", "44"}, self.get_exported_ids())
        self.assertEqual({"43", "44"}, self.get_pushed_ids())

    async def test_successful_push_to_label_studio(self):
        await self.run_chart_review()

        # Confirm we passed LS args down to the Label Studio client
        self.assertEqual(
            [mock.call("https://localhost/labelstudio", "abc123", 21, {"C0028081": "Sweats"})],
            self.ls_client_mock.call_args_list,
        )

        # Confirm we imported tasks formatted with NLP correctly
        tasks = self.ls_client.push_tasks.call_args[0][0]
        self.assertEqual(["Notes for fever", "Notes! for fever"], [t.text for t in tasks])
        self.assertEqual(["43", "44"], [t.ref_id for t in tasks])
        self.assertEqual(
            {
                "begin": 6,
                "end": 9,
                "text": "for",
                "polarity": 0,
                "conceptAttributes": [
                    {"code": "386661006", "cui": "C0015967", "codingScheme": "SNOMEDCT_US", "tui": "T184"},
                    {"code": "50177009", "cui": "C0015967", "codingScheme": "SNOMEDCT_US", "tui": "T184"},
                ],
                "type": "SignSymptomMention",
            },
            tasks[0].matches[0].as_json(),
        )

    @ddt.data(True, False)
    async def test_overwrite(self, overwrite):
        """Verify we pass down --overwrite correctly"""
        await self.run_chart_review(overwrite=overwrite)
        self.assertEqual(overwrite, self.ls_client.push_tasks.call_args[1]["overwrite"])

    async def test_disabled_nlp(self):
        await self.run_chart_review(nlp=False)

        tasks = self.ls_client.push_tasks.call_args[0][0]
        self.assertGreater(len(tasks), 0)
        for task in tasks:
            self.assertEqual([], task.matches)

    @ddt.data(True, False)
    async def test_philter(self, run_philter):
        notes = [LabelStudioNote("D1", "John Smith called on 10/13/2010")]
        with mock.patch("cumulus.chart_review.cli.read_notes_from_ndjson", return_value=notes):
            await self.run_chart_review(philter=run_philter)

        tasks = self.ls_client.push_tasks.call_args[0][0]
        self.assertEqual(1, len(tasks))
        task = tasks[0]

        # Regardless of philter, we keep the original cTAKES match text
        self.assertEqual({"John", "Smith", "called"}, {m.text for m in task.matches})

        if run_philter:
            expected_text = "**** ***** called on **/**/****"
        else:
            expected_text = "John Smith called on 10/13/2010"
        self.assertEqual(expected_text, task.text)
