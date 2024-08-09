"""Tests for etl/studies/covid_symptom/covid_tasks.py"""

import os
from unittest import mock

import cumulus_fhir_support
import ddt
import respx

from cumulus_etl.etl.studies import covid_symptom
from tests import i2b2_mock_data
from tests.ctakesmock import CtakesMixin
from tests.etl import BaseEtlSimple, TaskTestCase


@ddt.ddt
class TestCovidSymptomNlpResultsTask(CtakesMixin, TaskTestCase):
    """Test case for CovidSymptomNlpResultsTask"""

    def setUp(self):
        super().setUp()
        self.job_config.ctakes_overrides = self.ctakes_overrides.name

    async def test_prepare_failure(self):
        """Verify that if ctakes can't be restarted, we skip"""
        task = covid_symptom.CovidSymptomNlpResultsTask(self.job_config, self.scrubber)
        with mock.patch("cumulus_etl.nlp.restart_ctakes_with_bsv", return_value=False):
            self.assertFalse(await task.prepare_task())
        self.assertEqual(task.summaries[0].had_errors, True)

    @mock.patch("cumulus_etl.nlp.check_ctakes")
    @mock.patch("cumulus_etl.nlp.check_negation_cnlpt")
    async def test_negation_init_check(self, mock_cnlpt, mock_ctakes):
        await covid_symptom.CovidSymptomNlpResultsTask.init_check()
        self.assertEqual(mock_ctakes.call_count, 1)
        self.assertEqual(mock_cnlpt.call_count, 1)

    @mock.patch("cumulus_etl.nlp.check_ctakes")
    @mock.patch("cumulus_etl.nlp.check_term_exists_cnlpt")
    async def test_term_exists_init_check(self, mock_cnlpt, mock_ctakes):
        await covid_symptom.CovidSymptomNlpResultsTermExistsTask.init_check()
        self.assertEqual(mock_ctakes.call_count, 1)
        self.assertEqual(mock_cnlpt.call_count, 1)

    @mock.patch("cumulus_etl.nlp.ctakes_extract", side_effect=ValueError("oops"))
    async def test_ctakes_error(self, mock_extract):
        """Verify we skip docrefs when a cTAKES error happens"""
        self.make_json("DocumentReference", "doc", **i2b2_mock_data.documentreference())

        task = covid_symptom.CovidSymptomNlpResultsTask(self.job_config, self.scrubber)
        with self.assertLogs(level="WARN") as cm:
            await task.run()

        self.assertEqual(len(cm.output), 1)
        self.assertRegex(
            cm.output[0], r"Could not extract symptoms for docref .* \(ValueError\): oops"
        )

        # Confirm that we skipped the doc
        self.assertEqual(self.format.write_records.call_count, 1)
        batch = self.format.write_records.call_args[0][0]
        self.assertEqual(len(batch.rows), 0)

    @mock.patch("cumulus_etl.nlp.list_polarity", side_effect=ValueError("oops"))
    async def test_cnlpt_error(self, mock_extract):
        """Verify we skip docrefs when a cNLPT error happens"""
        self.make_json("DocumentReference", "doc", **i2b2_mock_data.documentreference())

        task = covid_symptom.CovidSymptomNlpResultsTask(self.job_config, self.scrubber)
        with self.assertLogs(level="WARN") as cm:
            await task.run()

        self.assertEqual(len(cm.output), 1)
        self.assertRegex(
            cm.output[0], r"Could not check polarity for docref .* \(ValueError\): oops"
        )

        # Confirm that we skipped the doc
        self.assertEqual(self.format.write_records.call_count, 1)
        batch = self.format.write_records.call_args[0][0]
        self.assertEqual(len(batch.rows), 0)

    async def test_extract_polarity_type_error(self):
        """Verify we bail if a bogus polarity model was given"""
        self.make_json("DocumentReference", "doc", **i2b2_mock_data.documentreference())

        task = covid_symptom.CovidSymptomNlpResultsTask(self.job_config, self.scrubber)
        task.polarity_model = None
        with self.assertLogs(level="WARN") as cm:
            await task.run()

        self.assertEqual(len(cm.output), 1)
        self.assertRegex(cm.output[0], "Unknown polarity method: None")

        # Confirm that we skipped the doc
        self.assertEqual(self.format.write_records.call_count, 1)
        batch = self.format.write_records.call_args[0][0]
        self.assertEqual(len(batch.rows), 0)

    async def test_unknown_modifier_extensions_skipped_for_nlp_symptoms(self):
        """Verify we ignore unknown modifier extensions during a custom read task (nlp symptoms)"""
        docref0 = i2b2_mock_data.documentreference()
        docref0["subject"]["reference"] = "Patient/1234"
        self.make_json("DocumentReference", "0", **docref0)
        docref1 = i2b2_mock_data.documentreference()
        docref1["subject"]["reference"] = "Patient/5678"
        docref1["modifierExtension"] = [{"url": "unrecognized"}]
        self.make_json("DocumentReference", "1", **docref1)

        await covid_symptom.CovidSymptomNlpResultsTask(self.job_config, self.scrubber).run()

        # Confirm that only symptoms from docref 0 got stored
        self.assertEqual(1, self.format.write_records.call_count)
        batch = self.format.write_records.call_args[0][0]
        expected_subject = self.codebook.db.patient("1234")
        self.assertEqual({expected_subject}, {row["subject_id"] for row in batch.rows})

    @ddt.data(
        # (coding, expected valid note)
        # Invalid codes
        ([], False),
        ([{"system": "http://cumulus.smarthealthit.org/i2b2", "code": "NOTE:0"}], False),
        (
            [
                {
                    "system": "https://fhir.cerner.com/96976f07-eccb-424c-9825-e0d0b887148b/codeSet/72",
                    "code": "0",
                }
            ],
            False,
        ),
        ([{"system": "http://loinc.org", "code": "00000-0"}], False),
        ([{"system": "http://example.org", "code": "nope"}], False),
        # Valid codes
        ([{"system": "http://cumulus.smarthealthit.org/i2b2", "code": "NOTE:3710480"}], True),
        (
            [
                {
                    "system": "https://fhir.cerner.com/96976f07-eccb-424c-9825-e0d0b887148b/codeSet/72",
                    "code": "3710480",
                }
            ],
            True,
        ),
        ([{"system": "http://loinc.org", "code": "57053-1"}], True),
        (
            [{"system": "nope", "code": "nope"}, {"system": "http://loinc.org", "code": "57053-1"}],
            True,
        ),
    )
    @ddt.unpack
    async def test_ed_note_filtering_for_nlp(self, codings, expected):
        """Verify we filter out any non-emergency-department note"""
        # Use one doc with category set, and one with type set. Either should work.
        docref0 = i2b2_mock_data.documentreference()
        docref0["category"] = [{"coding": codings}]
        del docref0["type"]
        self.make_json("DocumentReference", "0", **docref0)
        docref1 = i2b2_mock_data.documentreference()
        docref1["type"] = {"coding": codings}
        self.make_json("DocumentReference", "1", **docref1)

        await covid_symptom.CovidSymptomNlpResultsTask(self.job_config, self.scrubber).run()

        self.assertEqual(1, self.format.write_records.call_count)
        batch = self.format.write_records.call_args[0][0]
        self.assertEqual(4 if expected else 0, len(batch.rows))

    async def test_non_ed_visit_is_skipped_for_covid_symptoms(self):
        """Verify we ignore non ED visits for the covid symptoms NLP"""
        docref0 = i2b2_mock_data.documentreference()
        docref0["type"]["coding"][0]["code"] = "NOTE:nope"
        self.make_json("DocumentReference", "skipped", **docref0)
        docref1 = i2b2_mock_data.documentreference()
        docref1["type"]["coding"][0]["code"] = "NOTE:149798455"
        self.make_json("DocumentReference", "present", **docref1)

        await covid_symptom.CovidSymptomNlpResultsTask(self.job_config, self.scrubber).run()

        # Confirm that only symptoms from docref 'present' got stored
        self.assertEqual(1, self.format.write_records.call_count)
        batch = self.format.write_records.call_args[0][0]
        expected_docref = self.codebook.db.resource_hash("present")
        self.assertEqual({expected_docref}, {row["docref_id"] for row in batch.rows})

    @ddt.data(
        ({"status": "entered-in-error"}, False),
        ({"status": "superseded"}, False),
        ({"status": "current"}, True),
        ({"docStatus": "preliminary"}, False),
        ({"docStatus": "entered-in-error"}, False),
        ({"docStatus": "final"}, True),
        ({"docStatus": "amended"}, True),
        # without any docStatus, we still run NLP on it ("status" is required and can't be skipped)
        ({}, True),
    )
    @ddt.unpack
    async def test_bad_doc_status_is_skipped_for_covid_symptoms(
        self, status: dict, should_process: bool
    ):
        """Verify we ignore certain docStatus codes for the covid symptoms NLP"""
        docref = i2b2_mock_data.documentreference()
        docref.update(status)
        self.make_json("DocumentReference", "doc", **docref)

        await covid_symptom.CovidSymptomNlpResultsTask(self.job_config, self.scrubber).run()
        self.assertEqual(1, self.format.write_records.call_count)
        batch = self.format.write_records.call_args[0][0]
        self.assertEqual(2 if should_process else 0, len(batch.rows))

    @ddt.data(
        # list of (URL, contentType), expected text
        ([("http://localhost/file-cough", "text/plain")], "cough"),  # handles absolute URL
        ([("file-cough", "text/html")], "cough"),  # handles html
        ([("file-cough", "application/xhtml+xml")], "cough"),  # handles xhtml
        # prefers text/plain to html
        ([("file-cough", "text/html"), ("file-fever", "text/plain")], "fever"),
        # prefers html to xhtml
        ([("file-cough", "application/xhtml+xml"), ("file-fever", "text/html")], "fever"),
        ([("file-cough", "text/nope")], None),  # ignores unsupported mimetypes
    )
    @ddt.unpack
    @respx.mock(assert_all_mocked=False, assert_all_called=False)
    async def test_note_urls_downloaded(self, attachments, expected_text, respx_mock):
        """Verify that we download any attachments with URLs"""
        # We return three words due to how our cTAKES mock works. It wants 3 words -- fever word is in middle.
        respx_mock.get("http://localhost/file-cough").respond(text="has cough bad")
        respx_mock.get("http://localhost/file-fever").respond(text="has fever bad")
        respx_mock.post(os.environ["URL_CTAKES_REST"]).pass_through()  # ignore cTAKES

        docref0 = i2b2_mock_data.documentreference()
        docref0["content"] = [
            {"attachment": {"url": a[0], "contentType": a[1]}} for a in attachments
        ]
        self.make_json("DocumentReference", "doc0", **docref0)

        async with self.job_config.client:
            await covid_symptom.CovidSymptomNlpResultsTask(self.job_config, self.scrubber).run()

        self.assertEqual(1, self.format.write_records.call_count)
        batch = self.format.write_records.call_args[0][0]
        if expected_text:
            self.assertEqual(expected_text, batch.rows[0]["match"]["text"])
        else:
            self.assertEqual(0, len(batch.rows))

    async def test_nlp_errors_saved(self):
        docref = i2b2_mock_data.documentreference()
        self.make_json("DocumentReference", "B", **docref)
        del docref["context"]  # this will cause this docref to fail
        self.make_json("DocumentReference", "A", **docref)
        self.make_json("DocumentReference", "C", **docref)

        await covid_symptom.CovidSymptomNlpResultsTask(self.job_config, self.scrubber).run()

        self.assertEqual(
            ["nlp-errors.ndjson"], os.listdir(f"{self.errors_dir}/covid_symptom__nlp_results")
        )
        self.assertEqual(
            ["A", "C"],  # pre-scrubbed versions of the docrefs are stored, for easier debugging
            [
                x["id"]
                for x in cumulus_fhir_support.read_multiline_json(
                    f"{self.errors_dir}/covid_symptom__nlp_results/nlp-errors.ndjson"
                )
            ],
        )

    async def test_group_values_noted(self):
        """Verify that the task does keep track of group values per batch"""
        docref = i2b2_mock_data.documentreference()
        self.make_json("DocumentReference", "first-doc", **docref)
        self.make_json("DocumentReference", "not-examined", **docref, docStatus="preliminary")
        self.make_json("DocumentReference", "second-doc", **docref)
        self.make_json("DocumentReference", "third-doc", **docref)

        self.job_config.batch_size = 4  # two symptoms for each doc that has them
        await covid_symptom.CovidSymptomNlpResultsTask(self.job_config, self.scrubber).run()

        self.assertEqual(2, self.format.write_records.call_count)

        # First batch
        first_batch = self.format.write_records.call_args_list[0][0][0]
        expected_row_docs = {
            self.codebook.db.resource_hash("first-doc"),
            self.codebook.db.resource_hash("second-doc"),
        }
        self.assertEqual(expected_row_docs, {row["docref_id"] for row in first_batch.rows})
        self.assertEqual(expected_row_docs, first_batch.groups)

        # Second batch
        second_batch = self.format.write_records.call_args_list[1][0][0]
        expected_row_docs = {self.codebook.db.resource_hash("third-doc")}
        self.assertEqual(expected_row_docs, {row["docref_id"] for row in second_batch.rows})
        self.assertEqual(expected_row_docs, second_batch.groups)

    async def test_zero_symptoms(self):
        """Verify that we write out a marker for DocRefs we did examine, even if no symptoms appeared"""
        docref = i2b2_mock_data.documentreference()
        docref_no_text = i2b2_mock_data.documentreference("")
        self.make_json("DocumentReference", "zero-symptoms", **docref_no_text)
        self.make_json("DocumentReference", "not-examined", **docref, docStatus="preliminary")

        await covid_symptom.CovidSymptomNlpResultsTask(self.job_config, self.scrubber).run()

        self.assertEqual(1, self.format.write_records.call_count)
        batch = self.format.write_records.call_args_list[0][0][0]

        rows = batch.rows
        self.assertEqual(1, len(rows))

        row = rows[0]
        anon_docref_id = self.codebook.db.resource_hash("zero-symptoms")
        self.assertEqual(f"{anon_docref_id}.0", row["id"])
        self.assertEqual(anon_docref_id, row["docref_id"])
        self.assertIsNone(row["match"])


class TestCovidSymptomEtl(BaseEtlSimple):
    """Tests the end-to-end ETL of covid symptom tasks."""

    DATA_ROOT = "covid"

    async def test_basic_run(self):
        await self.run_etl(tasks=["covid_symptom__nlp_results"])
        self.assert_output_equal()

    async def test_term_exists_task(self):
        # This one isn't even tagged for the study - we only want this upon request
        await self.run_etl(tasks=["covid_symptom__nlp_results_term_exists"])
        self.assert_output_equal("term-exists")
