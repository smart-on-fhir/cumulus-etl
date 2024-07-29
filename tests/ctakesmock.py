"""Holds mock methods for ctakesclient.client"""

import http.server
import json
import multiprocessing
import os
import signal
import socketserver
import tempfile
import unittest
from functools import partial
from unittest import mock

from ctakesclient import typesystem
from ctakesclient.transformer import TransformerModel


class CtakesMixin(unittest.TestCase):
    """
    Add this mixin to your test class to properly mock out calls to the NLP server

    See the docstring for fake_ctakes_extract() for guidance on the fake results this generates.
    """

    ctakes_port = 8888  # will be incremented each instance

    def setUp(self):
        super().setUp()

        # just freeze the version in place
        version_patcher = mock.patch("ctakesclient.__version__", new="1.2.0")
        self.addCleanup(version_patcher.stop)
        version_patcher.start()

        CtakesMixin.ctakes_port += 1
        os.environ["URL_CTAKES_REST"] = f"http://localhost:{CtakesMixin.ctakes_port}/"
        self.ctakes_overrides = tempfile.TemporaryDirectory()
        self._run_fake_ctakes_server(f"{self.ctakes_overrides.name}/symptoms.bsv")

        cnlp_patcher = mock.patch(
            "cumulus_etl.nlp.extract.ctakesclient.transformer.list_polarity",
            side_effect=fake_transformer_list_polarity,
        )
        self.addCleanup(cnlp_patcher.stop)
        self.cnlp_mock = cnlp_patcher.start()

    def tearDown(self) -> None:
        super().tearDown()
        del os.environ["URL_CTAKES_REST"]
        self.ctakes_server.kill()
        self.ctakes_server.join()
        self.ctakes_overrides = None

    def was_ctakes_called(self) -> bool:
        return bool(self._ctakes_called.value)

    def _run_fake_ctakes_server(self, overrides_path: str) -> None:
        """Starts a mock cTAKES server process"""
        has_started = multiprocessing.Event()
        self._ctakes_called = multiprocessing.Value("b")
        self._ctakes_called.value = 0
        self.ctakes_server = multiprocessing.Process(
            target=partial(
                _serve_with_restarts,
                overrides_path,
                CtakesMixin.ctakes_port,
                self._ctakes_called,
                has_started,
            ),
            daemon=True,
        )
        self.ctakes_server.start()
        has_started.wait()  # make sure we don't proceed with test until the server we started is ready


def _get_mtime(path) -> float | None:
    """Gets mtime of a filename"""
    try:
        return os.stat(path).st_mtime
    except FileNotFoundError:
        return None


def _serve_with_restarts(
    overrides_path: str,
    port: int,
    was_called: multiprocessing.Value,
    has_started: multiprocessing.Event,
) -> None:
    server_address = ("", port)
    mtime = None

    while True:  # loop that keeps spawning new servers as they get restarted
        ctakes_server = KillableServer(server_address, FakeCTakesHandler)
        ctakes_server.timeout = 0.1
        ctakes_server.was_called = was_called
        has_started.set()

        while True:  # loop that handles requests for a given server until we decide to restart
            ctakes_server.handle_request()

            # Check if the overrides path changed on us, and restart if so
            new_mtime = _get_mtime(overrides_path)
            if new_mtime != mtime:
                # File changed! Let's restart
                ctakes_server.server_close()
                mtime = new_mtime
                break


class KillableServer(socketserver.ForkingMixIn, http.server.HTTPServer):
    """
    An HTTP server that will forcefully kill all open requests when closing.

    This is used so that we can ensure all sockets are closed when we restart.
    """

    was_called: multiprocessing.Value = None

    def server_close(self):
        children = self.active_children or set()
        for child in children:
            os.kill(child, signal.SIGKILL)
        super().server_close()


class FakeCTakesHandler(http.server.BaseHTTPRequestHandler):
    """A handler that provides a fake cTAKES response"""

    # We explicitly do not set a class timeout.
    # We don't want that behavior, because if our mock code is misbehaving and not detecting an overrides file change,
    # if we time out a request on our end, it will look like a successful file change detection and mask a testing bug.

    def do_POST(self):
        """Serve a POST request."""
        self.server.was_called.value = 1  # signal to test framework that we were actually called

        content_len = int(self.headers.get("Content-Length", 0))
        sentence = self.rfile.read(content_len)

        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.end_headers()

        answer = fake_ctakes_extract(sentence.decode("utf8")).as_json()
        self.wfile.write(json.dumps(answer).encode("utf8"))


def fake_ctakes_extract(sentence: str) -> typesystem.CtakesJSON:
    """
    Simple fake response from cTAKES

    The output is fairly static:
    - The 1st word is marked as a 'patient'
    - The 2nd word is marked as a 'fever'
    - The 3rd word is marked as a 'itch'
    - The rest are ignored
    """
    words = sentence.split()

    if len(words) < 3:
        return typesystem.CtakesJSON()

    patient_word = words[0]
    patient_begin = 0
    patient_end = len(patient_word)

    fever_word = words[1]
    fever_begin = patient_end + 1
    fever_end = fever_begin + len(fever_word)

    itch_word = words[2]
    itch_begin = fever_end + 1
    itch_end = itch_begin + len(itch_word)

    # Response template inspired by response to "Patient has a fever and an itch"
    response = {
        "SignSymptomMention": [
            # A first normal symptom match on fever
            {
                "begin": fever_begin,
                "end": fever_end,
                "text": fever_word,
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
            # A second covid symptom match on nausea (for the same word as above), just for more matches during testing
            {
                "begin": fever_begin,
                "end": fever_end,
                "text": fever_word,
                "polarity": 0,
                "conceptAttributes": [
                    {
                        "code": "422587007",
                        "cui": "C0027497",
                        "codingScheme": "SNOMEDCT_US",
                        "tui": "T184",
                    },
                ],
                "type": "SignSymptomMention",
            },
            # This itch word will be stripped from symptom list after our ETL code receives it, since it is non-covid
            {
                "begin": itch_begin,
                "end": itch_end,
                "text": itch_word,
                "polarity": 0,
                "conceptAttributes": [
                    {
                        "code": "418290006",
                        "cui": "C0033774",
                        "codingScheme": "SNOMEDCT_US",
                        "tui": "T184",
                    },
                    {
                        "code": "279333002",
                        "cui": "C0033774",
                        "codingScheme": "SNOMEDCT_US",
                        "tui": "T184",
                    },
                    {
                        "code": "424492005",
                        "cui": "C0033774",
                        "codingScheme": "SNOMEDCT_US",
                        "tui": "T184",
                    },
                    {
                        "code": "418363000",
                        "cui": "C0033774",
                        "codingScheme": "SNOMEDCT_US",
                        "tui": "T184",
                    },
                ],
                "type": "SignSymptomMention",
            },
        ],
        "IdentifiedAnnotation": [
            {
                "begin": patient_begin,
                "end": patient_end,
                "text": patient_word,
                "polarity": 0,
                "conceptAttributes": [
                    {"code": "n/a", "cui": "CE_64", "codingScheme": "custom", "tui": "T0NA"},
                ],
                "type": "IdentifiedAnnotation",
            },
            {
                "begin": fever_begin,
                "end": fever_end,
                "text": fever_word,
                "polarity": 0,
                "conceptAttributes": [
                    {"code": "n/a", "cui": "a0_27", "codingScheme": "custom", "tui": "T0NA"},
                    {"code": "n/a", "cui": "DIS_31", "codingScheme": "custom", "tui": "T0NA"},
                    {"code": "n/a", "cui": "a0_36", "codingScheme": "custom", "tui": "T0NA"},
                ],
                "type": "IdentifiedAnnotation",
            },
        ],
    }

    return typesystem.CtakesJSON(response)


async def fake_transformer_list_polarity(
    sentence: str, spans: list[tuple], client=None, model=TransformerModel.NEGATION
) -> list[typesystem.Polarity]:
    """Simple always-positive fake response from cNLP."""
    del sentence, client

    # To better detect which model is in use, ensure a small difference between them
    if model == TransformerModel.TERM_EXISTS:
        # First span is negative
        return [typesystem.Polarity.neg] + [typesystem.Polarity.pos] * (len(spans) - 1)
    else:
        return [typesystem.Polarity.pos] * len(spans)
