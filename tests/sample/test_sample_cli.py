import gzip
import json
import os

import ddt

from cumulus_etl import cli, common, errors
from tests.utils import AsyncTestCase


@ddt.ddt
class TestSample(AsyncTestCase):
    """Tests for high-level upload-notes support."""

    def setUp(self):
        super().setUp()
        self.tmpdir = self.make_tempdir()
        self.output_csv = f"{self.tmpdir}/out.csv"
        self.input_path = os.path.join(self.datadir, "simple/input")

    async def run_sample(
        self,
        seed: int = 1234,
        count: int | None = None,
        input_path: str | None = None,
        args: list[str] | None = None,
    ) -> None:
        base_args = [
            "sample",
            input_path or self.input_path,
            f"--count={count or 1}",
            f"--seed={seed}",
            f"--output={self.output_csv}",
        ]
        args = args or []
        await cli.main([*base_args, *args])

    def assert_output(self, expected: str):
        found = common.read_text(self.output_csv)
        self.assertEqual(found, expected)

    async def test_sampling_options(self):
        await self.run_sample()
        self.assert_output("note_ref\nDocumentReference/44\n")

        await self.run_sample(count=2)  # confirm we can get more
        self.assert_output("note_ref\nDocumentReference/44\nDocumentReference/43\n")

        await self.run_sample(seed=5)  # confirm different seed changes results
        self.assert_output("note_ref\nDiagnosticReport/ultrasound\n")

        await self.run_sample(args=["--type=DiagnosticReport"])
        self.assert_output("note_ref\nDiagnosticReport/ultrasound\n")

    async def test_more_columns(self):
        await self.run_sample(args=["--columns=note,subject,encounter"])
        self.assert_output(
            "note_ref,subject_ref,encounter_id\nDocumentReference/44,Patient/323456,25\n"
        )

    async def test_selected(self):
        with open(f"{self.tmpdir}/in.csv", "w", encoding="utf8") as f:
            f.write("note_ref\n")
            f.write("DiagnosticReport/f201\n")
            f.write("DiagnosticReport/ultrasound\n")

        await self.run_sample(args=[f"--select-by-csv={self.tmpdir}/in.csv"])

        # Normally we'd pick DocRef/44 with the default seed/count
        self.assert_output("note_ref\nDiagnosticReport/ultrasound\n")

    async def test_export_to(self):
        await self.run_sample(args=[f"--export-to={self.tmpdir}/export"])
        self.assert_output("note_ref\nDocumentReference/44\n")
        with gzip.open(f"{self.tmpdir}/export/DocumentReference.ndjson.gz", "rt") as f:
            found_res = json.load(f)
        self.assertEqual(found_res["id"], "44")

    async def test_bad_type(self):
        with self.assert_fatal_exit(errors.ARGS_INVALID):
            await self.run_sample(args=["--type=Patient"])

    async def test_bad_column(self):
        with self.assert_fatal_exit(errors.ARGS_INVALID):
            await self.run_sample(args=["--columns=barcode"])
