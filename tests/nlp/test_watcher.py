"""Tests for nlp/watcher.py"""

import tempfile
from unittest import mock

from cumulus_etl import common, errors, nlp
from tests.ctakesmock import CtakesMixin
from tests.utils import AsyncTestCase


class TestNLPWatcher(AsyncTestCase):
    """Generic test case for service watching code"""

    @mock.patch("cumulus_etl.cli_utils.is_url_available", new=lambda x: False)
    def test_ctakes_down(self):
        """Verify we report cTAKES being down correctly"""
        with self.assertRaises(SystemExit) as cm:
            nlp.check_ctakes()
        self.assertEqual(errors.CTAKES_MISSING, cm.exception.code)

    @mock.patch("cumulus_etl.cli_utils.is_url_available", new=lambda x: False)
    def test_negation_cnlpt_down(self):
        """Verify we report negation being down correctly"""
        with self.assertRaises(SystemExit) as cm:
            nlp.check_negation_cnlpt()
        self.assertEqual(errors.CNLPT_MISSING, cm.exception.code)

    @mock.patch("cumulus_etl.cli_utils.is_url_available", new=lambda x: False)
    def test_term_exists_cnlpt_down(self):
        """Verify we report term exists being down correctly"""
        with self.assertRaises(SystemExit) as cm:
            nlp.check_term_exists_cnlpt()
        self.assertEqual(errors.CNLPT_MISSING, cm.exception.code)

    def test_restart_ctakes_no_folder(self):
        self.assertFalse(nlp.restart_ctakes_with_bsv("", ""))

    def test_restart_ctakes_nonexistent_folder(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            self.assertFalse(nlp.restart_ctakes_with_bsv(f"{tmpdir}/nope", ""))

    def test_restart_ctakes_file_not_folder(self):
        with tempfile.NamedTemporaryFile() as file:
            self.assertFalse(nlp.restart_ctakes_with_bsv(file.name, ""))


class TestCTakesWatcher(CtakesMixin, AsyncTestCase):
    """Test case for cTAKES watching code that needs a real server"""

    @mock.patch("select.poll")
    @mock.patch("time.sleep", new=lambda x: None)  # don't sleep during restart
    def test_restart_timeout(self, mock_poll):
        mock_poller = mock.MagicMock()
        mock_poller.poll.return_value = False
        mock_poll.return_value = mock_poller

        with tempfile.NamedTemporaryFile() as file:
            common.write_text(file.name, "C0028081|T184|night sweats|Sweats")
            with self.assertRaises(SystemExit) as cm:
                nlp.restart_ctakes_with_bsv(self.ctakes_overrides.name, file.name)
            self.assertEqual(errors.CTAKES_RESTART_FAILED, cm.exception.code)
