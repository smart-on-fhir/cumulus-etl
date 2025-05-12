"""Tests for upload_notes/cli.py"""

import base64
import itertools
import os
import shutil
import tempfile
from collections.abc import Iterable
from unittest import mock

import cumulus_fhir_support
import ddt
import respx

from cumulus_etl import cli, common, errors
from cumulus_etl.upload_notes.labelstudio import LabelStudioClient, LabelStudioNote
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
class TestUploadNotes(CtakesMixin, AsyncTestCase):
    """Tests for high-level upload-notes support."""

    def setUp(self):
        super().setUp()

        tmpdir = tempfile.mkdtemp()
        # Comment out this next line when debugging, to persist directory
        self.addCleanup(shutil.rmtree, tmpdir)

        self.input_path = os.path.join(self.datadir, "simple/input")
        self.phi_path = os.path.join(tmpdir, "phi")
        self.export_path = os.path.join(tmpdir, "export")

        self.bsv_path = os.path.join(tmpdir, "ctakes.bsv")
        common.write_text(self.bsv_path, "C0028081|T184|42984000|SNOMEDCT_US|night sweats|Sweats")

        self.token_path = os.path.join(tmpdir, "ls-token.txt")
        common.write_text(self.token_path, "abc123")

        self.ls_client_mock = self.patch(
            "cumulus_etl.upload_notes.cli.LabelStudioClient", spec=LabelStudioClient
        )
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

    async def run_upload_notes(
        self,
        *extra_args,
        input_path=None,
        phi_path=None,
        anon_docrefs=None,
        docrefs=None,
        nlp=True,
        philter=None,
        no_philter=None,
        overwrite=False,
        skip_init_checks=True,
    ) -> None:
        args = [
            "upload-notes",
            input_path or self.input_path,
            "https://localhost/labelstudio",
            phi_path or self.phi_path,
            f"--ctakes-overrides={self.ctakes_overrides.name}",
            f"--symptoms-bsv={self.bsv_path}",
            f"--export-to={self.export_path}",
            "--ls-project=21",
            f"--ls-token={self.token_path}",
        ]
        if skip_init_checks:
            args += ["--skip-init-checks"]
        if anon_docrefs:
            args += ["--anon-docrefs", anon_docrefs]
        if docrefs:
            args += ["--docrefs", docrefs]
        if nlp:
            args += ["--nlp"]
        if philter:
            args += ["--philter", philter]
        if no_philter:
            args += ["--no-philter"]
        if overwrite:
            args += ["--overwrite"]
        await cli.main([*args, *extra_args])

    @staticmethod
    def make_docref(
        doc_id: str,
        text: str | None = None,
        content: list[dict] | None = None,
        enc_id: str | None = None,
        patient_id: str | None = None,
        date: str | None = None,
        period_start: str | None = None,
    ) -> dict:
        docref = {
            "resourceType": "DocumentReference",
            "id": doc_id,
        }

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
        docref["content"] = content

        enc_id = enc_id or f"enc-{doc_id}"
        docref["context"] = {"encounter": [{"reference": f"Encounter/{enc_id}"}]}

        patient_id = patient_id or "P1"
        docref["subject"] = {"reference": f"Patient/{patient_id}"}

        if date:
            docref["date"] = date

        if period_start:
            docref["context"]["period"] = {"start": period_start}

        return docref

    @staticmethod
    def make_dxreport(
        doc_id: str,
        text: str | None = None,
        content: list[dict] | None = None,
        enc_id: str | None = None,
        patient_id: str | None = None,
        date: str | None = None,
        period_start: str | None = None,
    ) -> dict:
        dxreport = {
            "resourceType": "DiagnosticReport",
            "id": doc_id,
        }

        if content is None:
            text = text or "What's up doc?"
            content = [
                {
                    "contentType": "text/plain",
                    "data": base64.standard_b64encode(text.encode("utf8")).decode("ascii"),
                },
            ]
        dxreport["presentedForm"] = content

        enc_id = enc_id or f"enc-{doc_id}"
        dxreport["encounter"] = {"reference": f"Encounter/{enc_id}"}

        patient_id = patient_id or "P1"
        dxreport["subject"] = {"reference": f"Patient/{patient_id}"}

        if date:
            dxreport["issued"] = date

        if period_start:
            dxreport["effectivePeriod"] = {"start": period_start}

        return dxreport

    @staticmethod
    def make_resource(*args, resource_type: str = "DocumentReference", **kwargs) -> dict:
        if resource_type == "DiagnosticReport":
            return TestUploadNotes.make_dxreport(*args, **kwargs)
        else:
            return TestUploadNotes.make_docref(*args, **kwargs)

    @staticmethod
    def mock_search_url(
        respx_mock: respx.MockRouter, patient: str, resource_type: str, doc_ids: Iterable[str]
    ) -> None:
        bundle = {
            "resourceType": "Bundle",
            "entry": [
                {
                    "resource": TestUploadNotes.make_resource(doc_id, resource_type=resource_type),
                }
                for doc_id in doc_ids
            ],
        }

        respx_mock.get(
            f"https://localhost/{resource_type}?patient={patient}&_elements=content"
        ).respond(json=bundle)

    @staticmethod
    def mock_read_url(
        respx_mock: respx.MockRouter,
        doc_id: str,
        code: int = 200,
        docref: dict | None = None,
        **kwargs,
    ) -> None:
        docref = docref or TestUploadNotes.make_docref(doc_id, **kwargs)
        respx_mock.get(f"https://localhost/DocumentReference/{doc_id}").respond(
            status_code=code, json=docref
        )

    @staticmethod
    def write_id_file(path: str, header: str, ids: list[tuple | str]) -> None:
        """Fills a file with the provided docref ids of (docref_id, patient_id) tuples"""
        lines = [header] + [x if isinstance(x, str) else ",".join(x) for x in ids]
        with open(path, "w", encoding="utf8") as f:
            f.write("\n".join(lines))

    def get_exported_refs(self) -> set[str]:
        dxreports = cumulus_fhir_support.read_multiline_json(
            f"{self.export_path}/DiagnosticReport.ndjson"
        )
        docrefs = cumulus_fhir_support.read_multiline_json(
            f"{self.export_path}/DocumentReference.ndjson"
        )
        return {f"DiagnosticReport/{row['id']}" for row in dxreports} | {
            f"DocumentReference/{row['id']}" for row in docrefs
        }

    def get_pushed_refs(self) -> set[str]:
        notes = self.ls_client.push_tasks.call_args[0][0]
        return set(itertools.chain.from_iterable(n.doc_mappings.keys() for n in notes))

    @staticmethod
    def wrap_note(title: str, text: str, first: bool = True, date: str | None = None) -> str:
        """Format a note in the expected output format, with header"""
        finalized = ""
        if not first:
            finalized += "\n\n\n"
            finalized += "########################################\n"
            finalized += "########################################\n"
        finalized += f"{title}\n"
        finalized += f"{date or 'Unknown time'}\n"
        finalized += "########################################\n"
        finalized += "########################################\n\n\n"
        finalized += text.strip()
        return finalized

    async def test_real_and_fake_docrefs_conflict(self):
        """Verify that you can't pass in both real and fake docrefs"""
        with self.assertRaises(SystemExit) as cm:
            await self.run_upload_notes(anon_docrefs="foo", docrefs="bar")
        self.assertEqual(errors.ARGS_CONFLICT, cm.exception.code)

    @respx.mock(assert_all_mocked=False)
    async def test_gather_anon_docrefs_from_server(self, respx_mock):
        self.mock_search_url(
            respx_mock, "P1", "DocumentReference", ["NotMe", "D1", "NotThis", "D3"]
        )
        respx_mock.post(os.environ["URL_CTAKES_REST"]).pass_through()  # ignore cTAKES

        with tempfile.NamedTemporaryFile() as file:
            self.write_id_file(
                file.name,
                "anon_document_ref,anon_subject_id,subject_id",
                [
                    (f"DocumentReference/{ANON_D1}", ANON_P1, "P1"),
                    (f"DocumentReference/{ANON_D3}", ANON_P1, "P1"),
                    # unknown stuff is gracefully ignored:
                    ("DocumentReference/unknown-doc", "unknown-patient", "unknown-patient"),
                ],
            )
            await self.run_upload_notes(input_path="https://localhost", anon_docrefs=file.name)

        self.assertEqual(
            {"DocumentReference/D1", "DocumentReference/D3"},
            self.get_exported_refs(),
        )
        self.assertEqual(
            {"DocumentReference/D1", "DocumentReference/D3"},
            self.get_pushed_refs(),
        )

    @respx.mock(assert_all_mocked=False)
    async def test_gather_real_docrefs_from_server(self, respx_mock):
        self.mock_read_url(respx_mock, "D1")
        self.mock_read_url(respx_mock, "D2")
        self.mock_read_url(respx_mock, "D3", code=500)  # confirm failure is graceful
        respx_mock.post(os.environ["URL_CTAKES_REST"]).pass_through()  # ignore cTAKES

        with tempfile.NamedTemporaryFile() as file:
            self.write_id_file(file.name, "docref_id", ["D1", "D2", "D3"])
            await self.run_upload_notes(input_path="https://localhost", docrefs=file.name)

        self.assertEqual({"DocumentReference/D1", "DocumentReference/D2"}, self.get_exported_refs())
        self.assertEqual({"DocumentReference/D1", "DocumentReference/D2"}, self.get_pushed_refs())

    @mock.patch("cumulus_etl.upload_notes.downloader.loaders.FhirNdjsonLoader")
    async def test_gather_all_docrefs_from_server(self, mock_loader):
        # Mock out the bulk export loading, as that's well tested elsewhere
        async def load_resources(*args):
            del args
            return common.RealDirectory(self.input_path)

        load_resources_mock = mock_loader.return_value.load_resources
        load_resources_mock.side_effect = load_resources

        # Do the actual upload-notes push
        await self.run_upload_notes(input_path="https://localhost")

        # Make sure we drive the bulk export correctly
        self.assertEqual(1, mock_loader.call_count)
        self.assertEqual("https://localhost", mock_loader.call_args[0][0].path)
        self.assertEqual(self.export_path, mock_loader.call_args[1]["export_to"])
        self.assertEqual(
            [mock.call({"DiagnosticReport", "DocumentReference"})],
            load_resources_mock.call_args_list,
        )

        # Make sure we do read the result and push the docrefs out
        self.assertEqual(
            {
                "DiagnosticReport/f201",
                "DiagnosticReport/ultrasound",
                "DocumentReference/43",
                "DocumentReference/44",
            },
            self.get_pushed_refs(),
        )

    async def test_gather_anon_docrefs_from_folder(self):
        with tempfile.NamedTemporaryFile() as file:
            self.write_id_file(
                file.name,
                "docref_id,patient_id",
                [
                    (ANON_43, "patient"),  # patient doesn't matter
                ],
            )
            await self.run_upload_notes(anon_docrefs=file.name)

        self.assertEqual({"DocumentReference/43"}, self.get_exported_refs())
        self.assertEqual({"DocumentReference/43"}, self.get_pushed_refs())

    @ddt.data(
        ("DiagnosticReport", "f201", True),
        ("DocumentReference", "44", False),
    )
    @ddt.unpack
    async def test_gather_real_docrefs_from_folder(self, resource_type, ident, add_unknown):
        with tempfile.NamedTemporaryFile() as file:
            ids = [ident, "unknown-doc"] if add_unknown else [ident]
            self.write_id_file(file.name, f"{resource_type.lower()}_id", ids)
            await self.run_upload_notes(docrefs=file.name)

        self.assertEqual({f"{resource_type}/{ident}"}, self.get_exported_refs())
        self.assertEqual({f"{resource_type}/{ident}"}, self.get_pushed_refs())

    async def test_gather_all_docrefs_from_folder(self):
        await self.run_upload_notes()
        self.assertEqual(
            {
                "DiagnosticReport/f201",
                "DiagnosticReport/ultrasound",
                "DocumentReference/43",
                "DocumentReference/44",
            },
            self.get_exported_refs(),
        )
        self.assertEqual(
            {
                "DiagnosticReport/f201",
                "DiagnosticReport/ultrasound",
                "DocumentReference/43",
                "DocumentReference/44",
            },
            self.get_pushed_refs(),
        )

    async def test_skip_no_attachment_docs(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            with common.NdjsonWriter(f"{tmpdir}/docs.ndjson") as writer:
                writer.write(TestUploadNotes.make_docref("D1", content=[]))
                writer.write(TestUploadNotes.make_docref("D2"))
            await self.run_upload_notes(input_path=tmpdir, philter="disable")

        self.assertEqual({"DocumentReference/D2"}, self.get_pushed_refs())

    async def test_successful_push_to_label_studio(self):
        await self.run_upload_notes()

        # Confirm we passed LS args down to the Label Studio client
        self.assertEqual(
            [mock.call("https://localhost/labelstudio", "abc123", 21, {"C0028081": "Sweats"})],
            self.ls_client_mock.call_args_list,
        )

        # Confirm we imported tasks formatted with NLP correctly
        tasks = self.ls_client.push_tasks.call_args[0][0]
        self.assertEqual(["23", "25"], [t.encounter_id for t in tasks])
        self.assertEqual(
            [
                "71b6e68140b2b8dc76a313be69627739573510954ec677d3d503f549673cce97",
                "44fd65be3e9fee4557a6c12cb40ee0659137b9445a813f87d9950a97534ad353",
            ],
            [t.anon_encounter_id for t in tasks],
        )
        self.assertEqual(
            [
                {
                    "DiagnosticReport/ultrasound": "DiagnosticReport/edd7402c6fb62706e9c67b9f801171b3df597391d04b3afad71d827ec88dea45",
                    "DocumentReference/43": "DocumentReference/ee448bae9c010410080183e7914151097dd6b927dbd8a58efcf0859fc32907a6",
                },
                {
                    "DiagnosticReport/f201": "DiagnosticReport/ea57c2bd658d69775ef8a58a9233104ec340f5440b5453a8bbb3acff7f186afd",
                    "DocumentReference/44": "DocumentReference/7a1f76190039b872a3016843e1712048cef3787931c000f0ea66b15962ccf65d",
                },
            ],
            [t.doc_mappings for t in tasks],
        )
        self.assertEqual(
            [
                self.wrap_note(
                    "Ultrasonography of abdomen", "Notes for fever", date="12/01/12 12:00:00"
                )
                + self.wrap_note("Admission MD", "Notes for fever", date="06/23/21", first=False),
                self.wrap_note(
                    "Computed tomography (CT) of head and neck",
                    "Notes! for fever",
                    date="12/01/12 12:00:00",
                )
                + self.wrap_note("Admission MD", "Notes! for fever", date="06/24/21", first=False),
            ],
            [t.text for t in tasks],
        )
        self.assertEqual(
            {
                "begin": 135,
                "end": 138,
                "text": "for",
                "polarity": 0,
                "conceptAttributes": [
                    {
                        "code": "386661006",
                        "cui": "C0015967",
                        "codingScheme": "SNOMEDCT_US",
                        "tui": "T184",
                    },
                    {
                        "code": "50177009",
                        "cui": "C0015967",
                        "codingScheme": "SNOMEDCT_US",
                        "tui": "T184",
                    },
                ],
                "type": "SignSymptomMention",
            },
            tasks[0].ctakes_matches[0].as_json(),
        )

    @ddt.data(True, False)
    async def test_overwrite(self, overwrite):
        """Verify we pass down --overwrite correctly"""
        await self.run_upload_notes(overwrite=overwrite)
        self.assertEqual(overwrite, self.ls_client.push_tasks.call_args[1]["overwrite"])

    async def test_disabled_nlp(self):
        await self.run_upload_notes(nlp=False)

        tasks = self.ls_client.push_tasks.call_args[0][0]
        self.assertGreater(len(tasks), 0)
        for task in tasks:
            self.assertEqual([], task.ctakes_matches)

    @ddt.data(
        ({}, True),  # default args
        ({"philter": "redact"}, True),
        ({"philter": "disable"}, False),
        ({"no_philter": True}, False),
    )
    @ddt.unpack
    async def test_philter_redact(self, upload_args, expect_redacted):
        notes = [
            LabelStudioNote(
                "Pat",
                "PatAnon",
                "EncID",
                "EncAnon",
                title="My Title",
                text="John Smith called on 10/13/2010",
            )
        ]
        with mock.patch("cumulus_etl.upload_notes.cli.read_notes_from_ndjson", return_value=notes):
            await self.run_upload_notes(**upload_args)

        tasks = self.ls_client.push_tasks.call_args[0][0]
        self.assertEqual(1, len(tasks))
        task = tasks[0]

        # Regardless of philter, we keep the original cTAKES match text
        self.assertEqual({"John", "Smith", "called"}, {m.text for m in task.ctakes_matches})

        if expect_redacted:
            expected_text = "**** ***** called on 10/13/2010"  # we don't philter dates
        else:
            expected_text = "John Smith called on 10/13/2010"
        self.assertEqual(self.wrap_note("My Title", expected_text), task.text)

    async def test_philter_label(self):
        notes = [
            LabelStudioNote(
                "Pat",
                "PatAnon",
                "EncID",
                "EncAnon",
                title="My Title",
                text="John Smith called on 10/13/2010",
            )
        ]
        with mock.patch("cumulus_etl.upload_notes.cli.read_notes_from_ndjson", return_value=notes):
            await self.run_upload_notes(philter="label")

        tasks = self.ls_client.push_tasks.call_args[0][0]
        self.assertEqual(1, len(tasks))
        task = tasks[0]

        # High span numbers because we insert some header text
        self.assertEqual({106: 110, 111: 116}, task.philter_map)

    async def test_grouped_datetime(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            with common.NdjsonWriter(f"{tmpdir}/DocumentReference.ndjson") as writer:
                writer.write(TestUploadNotes.make_docref("D1", enc_id="E1", text="DocRef 1"))
                writer.write(
                    TestUploadNotes.make_docref(
                        "D2", enc_id="E1", text="DocRef 2", date="2018-01-03T13:10:10+01:00"
                    )
                )
                writer.write(
                    TestUploadNotes.make_docref(
                        "D3",
                        enc_id="E1",
                        text="DocRef 3",
                        date="2018-01-03T13:10:20Z",
                        period_start="2018",
                    )
                )
            with common.NdjsonWriter(f"{tmpdir}/DiagnosticReport.ndjson") as writer:
                writer.write(
                    TestUploadNotes.make_dxreport(
                        "R1", enc_id="E1", text="DxRep 1", date="2018-01-03T13:10:50+01:00"
                    )
                )
                writer.write(
                    TestUploadNotes.make_dxreport(
                        "R2", enc_id="E1", text="DxRep 2", period_start="2018-01-03T13:10:51+01:00"
                    )
                )
            await self.run_upload_notes(input_path=tmpdir, philter="disable")

        notes = self.ls_client.push_tasks.call_args[0][0]
        self.assertEqual(1, len(notes))
        note = notes[0]

        # The order will be oldest->newest (None placed last)
        self.assertEqual(
            self.wrap_note("Document", "DocRef 3", date="01/01/18")
            + self.wrap_note("Document", "DocRef 2", date="01/03/18 13:10:10", first=False)
            + self.wrap_note("Document", "DxRep 1", date="01/03/18 13:10:50", first=False)
            + self.wrap_note("Document", "DxRep 2", date="01/03/18 13:10:51", first=False)
            + self.wrap_note("Document", "DocRef 1", first=False),
            note.text,
        )

    async def test_grouped_encounter_offsets(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            with common.NdjsonWriter(f"{tmpdir}/DocumentReference.ndjson") as writer:
                writer.write(TestUploadNotes.make_docref("D1", enc_id="43"))
                writer.write(TestUploadNotes.make_docref("D2", enc_id="43"))
            await self.run_upload_notes(input_path=tmpdir)

        notes = self.ls_client.push_tasks.call_args[0][0]
        self.assertEqual(1, len(notes))
        note = notes[0]

        # Did we mark that both IDs occur in one note correctly?
        self.assertEqual(
            {
                "DocumentReference/D1": f"DocumentReference/{ANON_D1}",
                "DocumentReference/D2": f"DocumentReference/{ANON_D2}",
            },
            note.doc_mappings,
        )

        # Did we mark the internal docref spans correctly?
        first_span = (106, 120)
        second_span = (311, 325)
        self.assertEqual("What's up doc?", note.text[first_span[0] : first_span[1]])
        self.assertEqual("What's up doc?", note.text[second_span[0] : second_span[1]])
        self.assertEqual(
            {"DocumentReference/D1": first_span, "DocumentReference/D2": second_span},
            note.doc_spans,
        )

        # Did we edit cTAKES results correctly?
        match1a = (106, 112)
        match1b = (113, 115)
        match1c = (116, 120)
        match2a = (311, 317)
        match2b = (318, 320)
        match2c = (321, 325)
        self.assertEqual("What's", note.text[match1a[0] : match1a[1]])
        self.assertEqual("up", note.text[match1b[0] : match1b[1]])
        self.assertEqual("doc?", note.text[match1c[0] : match1c[1]])
        self.assertEqual("What's", note.text[match2a[0] : match2a[1]])
        self.assertEqual("up", note.text[match2b[0] : match2b[1]])
        self.assertEqual("doc?", note.text[match2c[0] : match2c[1]])
        spans = {x.span().key() for x in note.ctakes_matches}
        self.assertEqual({match1a, match1b, match1c, match2a, match2b, match2c}, spans)

    async def test_docrefs_without_encounters_are_skipped(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            with common.NdjsonWriter(f"{tmpdir}/DocumentReference.ndjson") as writer:
                # One docref without any context/encounter info
                docref1 = self.make_docref("D1")
                del docref1["context"]
                writer.write(docref1)

                # And one with all the normal goodies
                writer.write(self.make_docref("D2"))

            await self.run_upload_notes(input_path=tmpdir)

        self.assertEqual({"DocumentReference/D1", "DocumentReference/D2"}, self.get_exported_refs())
        self.assertEqual({"DocumentReference/D2"}, self.get_pushed_refs())

    @mock.patch("cumulus_etl.nlp.restart_ctakes_with_bsv", new=lambda *_: False)
    async def test_nlp_restart_failure_is_noticed(self):
        with self.assertRaises(SystemExit) as cm:
            await self.run_upload_notes()
        self.assertEqual(cm.exception.code, errors.CTAKES_OVERRIDES_INVALID)

    @mock.patch("cumulus_etl.nlp.check_ctakes")
    @mock.patch("cumulus_etl.nlp.check_negation_cnlpt")
    @mock.patch("cumulus_etl.cli_utils.is_url_available")
    async def test_init_checks(self, mock_url, mock_cnlpt, mock_ctakes):
        # Start with error case for our URL check (against label studio)
        mock_url.return_value = False
        with self.assertRaises(SystemExit) as cm:
            await self.run_upload_notes(skip_init_checks=False)
        self.assertEqual(mock_ctakes.call_count, 1)
        self.assertEqual(mock_cnlpt.call_count, 1)
        self.assertEqual(mock_url.call_count, 1)
        self.assertEqual(cm.exception.code, errors.LABEL_STUDIO_MISSING)

        # Let the URL pass and confirm we now run successfully
        mock_url.return_value = True
        await self.run_upload_notes(skip_init_checks=False)

    async def test_highlights(self):
        # Test both commas and multiple args. Add some highlights that are internal words which
        # won't get found. Use two identical texts in same encounter to test grouping too.
        with tempfile.TemporaryDirectory() as tmpdir:
            with common.NdjsonWriter(f"{tmpdir}/docs.ndjson") as writer:
                text = "Report of nightmare on Elm St. But 'Freddy' isn't real. A+ Street."
                writer.write(TestUploadNotes.make_docref("D1", enc_id="E1", text=text))
                writer.write(TestUploadNotes.make_docref("D2", enc_id="E1", text=text))
            await self.run_upload_notes(
                "--highlight=Elm", "--highlight=st,FREDDY,a+", input_path=tmpdir, philter="disable"
            )

        tasks = self.ls_client.push_tasks.call_args[0][0]
        self.assertEqual(len(tasks), 1)
        self.assertEqual(
            [tasks[0].text[span.begin : span.end] for span in tasks[0].highlights],
            ["Elm", "St", "Freddy", "A+", "Elm", "St", "Freddy", "A+"],
        )
        self.assertEqual(
            [span.key() for span in tasks[0].highlights],
            [
                (129, 132),
                (133, 135),
                (142, 148),
                (162, 164),
                (386, 389),
                (390, 392),
                (399, 405),
                (419, 421),
            ],
        )

    async def test_highlight_boundaries(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            with common.NdjsonWriter(f"{tmpdir}/docs.ndjson") as writer:
                text = ".A+/x.A+,.A+x..A+.*.A+"  # 1st, 4th, and 5th ".A+" will match
                writer.write(TestUploadNotes.make_docref("D1", enc_id="E1", text=text))
            await self.run_upload_notes("--highlight=.a+", input_path=tmpdir, philter="disable")

        tasks = self.ls_client.push_tasks.call_args[0][0]
        self.assertEqual(len(tasks), 1)
        self.assertEqual(
            [span.key() for span in tasks[0].highlights],
            [(106, 109), (120, 123), (125, 128)],
        )

    async def test_sorting(self):
        tmpdir = self.make_tempdir()

        with common.NdjsonWriter(f"{tmpdir}/docs.ndjson") as writer:
            writer.write(
                TestUploadNotes.make_docref(
                    "D4",
                    enc_id="E1",
                    patient_id="P1",
                    date="2000-01-04",
                )
            )
            writer.write(
                TestUploadNotes.make_docref(
                    "D3",
                    enc_id="E2",
                    patient_id="P2",
                    date="2000-01-03",
                )
            )
            writer.write(
                TestUploadNotes.make_docref(
                    "D2",
                    enc_id="E3",
                    patient_id="P1",
                    date="2000-01-02",
                )
            )
            writer.write(
                TestUploadNotes.make_docref(
                    "D1",
                    enc_id="E2",
                    patient_id="P2",
                    date="2000-01-01",
                )
            )
        await self.run_upload_notes(input_path=tmpdir, philter="disable")
        tasks = self.ls_client.push_tasks.call_args[0][0]
        self.assertEqual(
            [list(task.doc_mappings.keys()) for task in tasks],
            # Earliest patient first, then earliest encounters, then earliest docs
            [
                ["DocumentReference/D1", "DocumentReference/D3"],
                ["DocumentReference/D2"],
                ["DocumentReference/D4"],
            ],
        )
