import contextlib
import gzip
import io
import json
import os

from cumulus_etl import cli, common, errors
from tests.utils import AsyncTestCase


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
        output: str | None = None,
        args: list[str] | None = None,
    ) -> None:
        base_args = [
            "sample",
            input_path or self.input_path,
            f"--count={count or 1}",
            f"--seed={seed}",
            f"--output={output or self.output_csv}",
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
        self.assert_output("note_ref\nDocumentReference/43\nDocumentReference/44\n")

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

    async def test_stdout(self):
        stdout = io.StringIO()
        with contextlib.redirect_stdout(stdout):
            await self.run_sample(output="-")
        self.assertEqual(stdout.getvalue(), "note_ref\nDocumentReference/44\n")

    async def test_bad_type(self):
        with self.assert_fatal_exit(errors.ARGS_INVALID):
            await self.run_sample(args=["--type=Patient"])

    async def test_bad_column(self):
        with self.assert_fatal_exit(errors.ARGS_INVALID):
            await self.run_sample(args=["--columns=barcode"])

    async def test_only_samples_text_notes(self):
        notedir = f"{self.tmpdir}/notes"
        os.makedirs(notedir)
        with common.NdjsonWriter(f"{notedir}/docrefs.ndjson") as writer:
            writer.write({"resourceType": "DocumentReference", "id": "no-text"})
            writer.write(
                {
                    "resourceType": "DocumentReference",
                    "id": "url-only",
                    "content": [
                        {
                            "attachment": {
                                "contentType": "text/plain",
                                "url": "https://example.com/note",
                            }
                        }
                    ],
                }
            )
            writer.write(
                {
                    "resourceType": "DocumentReference",
                    "id": "with-text",
                    "content": [{"attachment": {"contentType": "text/plain", "data": "aGVsbG8="}}],
                }
            )

        await self.run_sample(input_path=notedir, count=2)
        # Asked for two, but only one got picked because the non-text notes were skipped
        self.assert_output("note_ref\nDocumentReference/with-text\n")

    async def test_errors_out_with_anon_but_no_phi_dir(self):
        # Write an anon csv that won't match anything, but that's fine
        common.write_text(f"{self.tmpdir}/anon.csv", "note_ref\nDocumentReference/xxx\n")

        with self.assert_fatal_exit(errors.ARGS_INVALID):
            await self.run_sample(args=[f"--select-by-anon-csv={self.tmpdir}/anon.csv"])

        await self.run_sample(
            args=[f"--select-by-anon-csv={self.tmpdir}/anon.csv", f"--phi-dir={self.tmpdir}/phi"]
        )
        self.assert_output("note_ref\n")  # no matches, but no failure either
