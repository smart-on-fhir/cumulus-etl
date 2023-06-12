"""Tests for etl/studies/covid_symptom/covid_tasks.py"""

import os

import ddt
import respx

from cumulus_etl import common
from cumulus_etl.etl.studies import covid_symptom

from tests.ctakesmock import CtakesMixin
from tests import i2b2_mock_data
from tests.test_etl_tasks import TaskTestCase


@ddt.ddt
class TestCovidSymptomNlpResultsTask(CtakesMixin, TaskTestCase):
    """Test case for CovidSymptomNlpResultsTask"""

    def setUp(self):
        super().setUp()
        self.job_config.ctakes_overrides = self.ctakes_overrides.name

    async def test_unknown_modifier_extensions_skipped_for_nlp_symptoms(self):
        """Verify we ignore unknown modifier extensions during a custom read task (nlp symptoms)"""
        docref0 = i2b2_mock_data.documentreference()
        docref0["subject"]["reference"] = "Patient/1234"
        self.make_json("DocumentReference.0", "0", **docref0)
        docref1 = i2b2_mock_data.documentreference()
        docref1["subject"]["reference"] = "Patient/5678"
        docref1["modifierExtension"] = [{"url": "unrecognized"}]
        self.make_json("DocumentReference.1", "1", **docref1)

        await covid_symptom.CovidSymptomNlpResultsTask(self.job_config, self.scrubber).run()

        # Confirm that only symptoms from docref 0 got stored
        self.assertEqual(1, self.format.write_records.call_count)
        df = self.format.write_records.call_args[0][0]
        expected_subject = self.codebook.db.patient("1234")
        self.assertEqual({expected_subject}, set(df.subject_id))

    @ddt.data(
        # (coding, expected valid note)
        # Invalid codes
        ([], False),
        ([{"system": "http://cumulus.smarthealthit.org/i2b2", "code": "NOTE:0"}], False),
        ([{"system": "http://loinc.org", "code": "00000-0"}], False),
        ([{"system": "http://example.org", "code": "nope"}], False),
        # Valid codes
        ([{"system": "http://cumulus.smarthealthit.org/i2b2", "code": "NOTE:3710480"}], True),
        ([{"system": "http://loinc.org", "code": "57053-1"}], True),
        ([{"system": "nope", "code": "nope"}, {"system": "http://loinc.org", "code": "57053-1"}], True),
    )
    @ddt.unpack
    async def test_ed_note_filtering_for_nlp(self, codings, expected):
        """Verify we filter out any non-emergency-department note"""
        # Use one doc with category set, and one with type set. Either should work.
        docref0 = i2b2_mock_data.documentreference()
        docref0["category"] = [{"coding": codings}]
        del docref0["type"]
        self.make_json("DocumentReference.0", "0", **docref0)
        docref1 = i2b2_mock_data.documentreference()
        docref1["type"] = {"coding": codings}
        self.make_json("DocumentReference.1", "1", **docref1)

        await covid_symptom.CovidSymptomNlpResultsTask(self.job_config, self.scrubber).run()

        self.assertEqual(1 if expected else 0, self.format.write_records.call_count)
        if expected:
            df = self.format.write_records.call_args[0][0]
            self.assertEqual(4, len(df))

    async def test_non_ed_visit_is_skipped_for_covid_symptoms(self):
        """Verify we ignore non ED visits for the covid symptoms NLP"""
        docref0 = i2b2_mock_data.documentreference()
        docref0["type"]["coding"][0]["code"] = "NOTE:nope"  # pylint: disable=unsubscriptable-object
        self.make_json("DocumentReference.0", "skipped", **docref0)
        docref1 = i2b2_mock_data.documentreference()
        docref1["type"]["coding"][0]["code"] = "NOTE:149798455"  # pylint: disable=unsubscriptable-object
        self.make_json("DocumentReference.1", "present", **docref1)

        await covid_symptom.CovidSymptomNlpResultsTask(self.job_config, self.scrubber).run()

        # Confirm that only symptoms from docref 'present' got stored
        self.assertEqual(1, self.format.write_records.call_count)
        df = self.format.write_records.call_args[0][0]
        expected_docref = self.codebook.db.resource_hash("present")
        self.assertEqual({expected_docref}, set(df.docref_id))

    @ddt.data(
        ({"status": "entered-in-error"}, False),
        ({"status": "superseded"}, False),
        ({"status": "current"}, True),
        ({"docStatus": "preliminary"}, False),
        ({"docStatus": "entered-in-error"}, False),
        ({"docStatus": "final"}, True),
        ({"docStatus": "amended"}, True),
        ({}, True),  # without any docStatus, we still run NLP on it ("status" is required and can't be skipped)
    )
    @ddt.unpack
    async def test_bad_doc_status_is_skipped_for_covid_symptoms(self, status: dict, should_process: bool):
        """Verify we ignore certain docStatus codes for the covid symptoms NLP"""
        docref = i2b2_mock_data.documentreference()
        docref.update(status)
        self.make_json("DocumentReference.0", "doc", **docref)

        await covid_symptom.CovidSymptomNlpResultsTask(self.job_config, self.scrubber).run()
        self.assertEqual(1 if should_process else 0, self.format.write_records.call_count)

    @ddt.data(
        # list of (URL, contentType), expected text
        ([("http://localhost/file-cough", "text/plain")], "cough"),  # handles absolute URL
        ([("file-cough", "text/html")], "cough"),  # handles html
        ([("file-cough", "application/xhtml+xml")], "cough"),  # handles xhtml
        ([("file-cough", "text/html"), ("file-fever", "text/plain")], "fever"),  # prefers text/plain to html
        ([("file-cough", "application/xhtml+xml"), ("file-fever", "text/html")], "fever"),  # prefers html to xhtml
        ([("file-cough", "text/nope")], None),  # ignores unsupported mimetypes
    )
    @ddt.unpack
    @respx.mock
    async def test_note_urls_downloaded(self, attachments, expected_text):
        """Verify that we download any attachments with URLs"""
        # We return three words due to how our cTAKES mock works. It wants 3 words -- fever word is in middle.
        respx.get("http://localhost/file-cough").respond(text="has cough bad")
        respx.get("http://localhost/file-fever").respond(text="has fever bad")
        respx.post(os.environ["URL_CTAKES_REST"]).pass_through()  # ignore cTAKES

        docref0 = i2b2_mock_data.documentreference()
        docref0["content"] = [{"attachment": {"url": a[0], "contentType": a[1]}} for a in attachments]
        self.make_json("DocumentReference.0", "doc0", **docref0)

        async with self.job_config.client:
            await covid_symptom.CovidSymptomNlpResultsTask(self.job_config, self.scrubber).run()

        if expected_text:
            self.assertEqual(1, self.format.write_records.call_count)
            df = self.format.write_records.call_args[0][0]
            self.assertEqual(expected_text, df.iloc[0].match["text"])
        else:
            self.assertEqual(0, self.format.write_records.call_count)

    async def test_nlp_errors_saved(self):
        docref = i2b2_mock_data.documentreference()
        self.make_json("DocumentReference.2", "B", **docref)
        del docref["context"]  # this will cause this docref to fail
        self.make_json("DocumentReference.1", "A", **docref)
        self.make_json("DocumentReference.3", "C", **docref)

        await covid_symptom.CovidSymptomNlpResultsTask(self.job_config, self.scrubber).run()

        self.assertEqual(["nlp-errors.ndjson"], os.listdir(f"{self.errors_dir}/covid_symptom__nlp_results"))
        self.assertEqual(
            ["A", "C"],  # pre-scrubbed versions of the docrefs are stored, for easier debugging
            [x["id"] for x in common.read_ndjson(f"{self.errors_dir}/covid_symptom__nlp_results/nlp-errors.ndjson")],
        )
