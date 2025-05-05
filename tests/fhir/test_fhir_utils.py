"""Tests for fhir_utils.py"""

import base64
import datetime
import shutil
from unittest import mock

import ddt

from cumulus_etl import common, fhir
from tests import utils


@ddt.ddt
class TestReferenceHandlers(utils.AsyncTestCase):
    """Tests for the unref_ and ref_ methods"""

    @ddt.data(
        ({"reference": "Patient/123"}, "Patient", "123"),
        ({"reference": "123", "type": "Patient"}, "Patient", "123"),
        ({"reference": "#123"}, None, "#123"),
        # Synthea style reference
        (
            {"reference": "Patient?identifier=http://example.com|123"},
            "Patient",
            "identifier=http://example.com|123",
        ),
    )
    @ddt.unpack
    def test_unref_successes(self, full_reference, expected_type, expected_id):
        parsed = fhir.unref_resource(full_reference)
        self.assertEqual((expected_type, expected_id), parsed)

    @ddt.data(
        None,
        "",
        "123",  # with no type field
        "?/123",
        "http://example.com/Patient/123",
    )
    def test_unref_failures(self, reference):
        with self.assertRaises(ValueError):
            fhir.unref_resource({"reference": reference})

    @ddt.data(
        ("Patient", "123", "Patient/123"),
        (None, "#123", "#123"),
    )
    @ddt.unpack
    def test_ref_resource(self, resource_type, resource_id, expected):
        self.assertEqual({"reference": expected}, fhir.ref_resource(resource_type, resource_id))

    def test_ref_resource_no_id(self):
        with self.assertRaisesRegex(ValueError, "Missing resource ID"):
            fhir.ref_resource(None, "")


@ddt.ddt
class TestDateParsing(utils.AsyncTestCase):
    """Tests for the parse_datetime method"""

    @ddt.data(
        (None, None),
        ("", None),
        ("abc", None),
        ("abc-de", None),
        ("abc-de-fg", None),
        ("2018", datetime.datetime(2018, 1, 1)),  # naive
        ("2021-07", datetime.datetime(2021, 7, 1)),  # naive
        ("1992-11-06", datetime.datetime(1992, 11, 6)),  # naive
        (
            "1992-11-06T13:28:17.239+02:00",
            datetime.datetime(
                1992,
                11,
                6,
                13,
                28,
                17,
                239000,
                tzinfo=datetime.timezone(datetime.timedelta(hours=2)),
            ),
        ),
        (
            "1992-11-06T13:28:17.239Z",
            datetime.datetime(1992, 11, 6, 13, 28, 17, 239000, tzinfo=datetime.timezone.utc),
        ),
    )
    @ddt.unpack
    def test_parse_datetime(self, input_value, expected_value):
        parsed = fhir.parse_datetime(input_value)
        self.assertEqual(expected_value, parsed)


@ddt.ddt
class TestUrlParsing(utils.AsyncTestCase):
    """Tests for URL parsing"""

    @ddt.data(
        ("//host", SystemExit),
        ("https://host", ""),
        ("https://host/root", ""),
        ("https://Group/MyGroup", ""),  # Group is hostname here
        ("https://host/root/?key=/Group/Testing/", ""),
        ("https://host/root/Group/MyGroup", "MyGroup"),
        ("https://host/root/Group/MyGroup/", "MyGroup"),
        ("https://host/Group/MyGroup/$export", "MyGroup"),
        ("https://host/Group/MyGroup?key=value", "MyGroup"),
        ("https://host/root/Group/Group/", "Group"),
    )
    @ddt.unpack
    def test_parse_group_from_url(self, url, expected_group):
        if isinstance(expected_group, str):
            group = fhir.FhirUrl(url).group
            assert expected_group == group
        else:
            with self.assertRaises(expected_group):
                fhir.FhirUrl(url)

    @ddt.data(
        "http://host/root/",
        "http://host/root/$export?",
        "http://host/root/Group/xxx",
        "http://host/root/Group/xxx#fragment",
        "http://host/root/Group/xxx/$export?_type=Patient",
        "http://host/root/Patient",
        "http://host/root/Patient/$export",
    )
    def test_parse_base_url_from_url(self, url):
        """Verify that we detect the auth root for an input URL"""
        parsed = fhir.FhirUrl(url)
        self.assertEqual(parsed.full_url, url)
        self.assertEqual(parsed.root_url, "http://host/root")


@ddt.ddt
class TestDocrefNotesUtils(utils.AsyncTestCase):
    """Tests for the utility methods dealing with document reference clinical notes"""

    def make_docref(self, docref_id: str, mimetype: str, note: str) -> dict:
        return {
            "id": docref_id,
            "resourceType": "DocumentReference",
            "content": [
                {
                    "attachment": {
                        "contentType": mimetype,
                        "data": base64.standard_b64encode(note.encode("utf8")).decode("ascii"),
                    },
                },
            ],
        }

    @ddt.data(
        ("text/html", "<html><body>He<b>llooooo</b></html>", "Hellooooo"),  # strips html
        (  # strips xhtml
            "application/xhtml+xml",
            '<!DOCTYPE html>\n<html xmlns="http://www.w3.org/1999/xhtml"><html><body>x<b>html!</b></html>',
            "xhtml!",
        ),
        ("text/plain", "What¿up", "What up"),  # strips ¿
    )
    @ddt.unpack
    async def test_docref_note_conversions(self, mimetype, incoming_note, expected_note):
        docref = self.make_docref("1", mimetype, incoming_note)
        resulting_note = await fhir.get_clinical_note(None, docref)
        self.assertEqual(resulting_note, expected_note)

    async def test_handles_no_data(self):
        with self.assertRaisesRegex(ValueError, "No data or url field present"):
            await fhir.get_clinical_note(
                None,
                {
                    "id": "no data",
                    "resourceType": "DocumentReference",
                    "content": [{"attachment": {"contentType": "text/plain"}}],
                },
            )

    async def test_handles_bad_resource_type(self):
        with self.assertRaisesRegex(ValueError, "Patient is not a supported clinical note type."):
            await fhir.get_clinical_note(
                None,
                {
                    "id": "nope",
                    "resourceType": "Patient",
                },
            )

    async def test_docref_note_caches_results(self):
        """Verify that get_clinical_note has internal caching"""

        async def assert_note_is(docref_id, text, expected_text):
            docref = self.make_docref(docref_id, "text/plain", text)
            note = await fhir.get_clinical_note(None, docref)
            self.assertEqual(expected_text, note)

        # Confirm that we cache
        await assert_note_is("same", "hello", "hello")
        await assert_note_is("same", "goodbye", "hello")

        # Confirm that we cache empty string correctly (i.e. empty string is not handled same as None)
        await assert_note_is("empty", "", "")
        await assert_note_is("empty", "not empty", "")

        # Confirm that a new id is not cached
        await assert_note_is("new", "fresh", "fresh")

        # Sanity-check that if we blow away the cache, we get new text
        shutil.rmtree(common.get_temp_dir("notes"))
        await assert_note_is("same", "goodbye", "goodbye")

    @ddt.data(
        (None, None),
        ("", None),
        ("#contained", None),
        ("Reference/relative", {"id": "ref"}),
        ("https://example.com/absolute/url", {"id": "ref"}),
    )
    @ddt.unpack
    async def test_download_reference(self, reference, expected_result):
        mock_client = mock.AsyncMock()
        mock_client.request.return_value = utils.make_response(json_payload={"id": "ref"})

        result = await fhir.download_reference(mock_client, reference)
        self.assertEqual(expected_result, result)
        self.assertEqual(
            [mock.call("GET", reference)] if expected_result else [],
            mock_client.request.call_args_list,
        )
