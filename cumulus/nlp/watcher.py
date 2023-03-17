"""Monitoring support for cTAKES and cNLP"""

import contextlib
import select
import socket
import time
import urllib.parse

import ctakesclient

from cumulus import errors


def is_url_available(url: str) -> bool:
    """Returns whether we are able to make connections to the given URL, with a few retries."""
    url_parsed = urllib.parse.urlparse(url)

    num_tries = 6  # try six times / wait five seconds
    for i in range(num_tries):
        try:
            socket.create_connection((url_parsed.hostname, url_parsed.port))
            return True
        except ConnectionRefusedError:
            if i < num_tries - 1:
                time.sleep(3)

    return False


def check_ctakes() -> None:
    """
    Verifies that cTAKES is available to receive requests.

    Will block while waiting for cTAKES.
    """
    # Check if our cTAKES server is ready (it may still not be fully ready once the socket is open, but at least it
    # will accept requests and then block the reply on it finishing its initialization)
    ctakes_url = ctakesclient.client.get_url_ctakes_rest()
    if not is_url_available(ctakes_url):
        errors.fatal(
            f"A running cTAKES server was not found at:\n    {ctakes_url}\n\n"
            "Please set the URL_CTAKES_REST environment variable or start the docker support services.",
            errors.CTAKES_MISSING,
        )


def check_cnlpt() -> None:
    """
    Verifies that the cNLP transformer server is running.
    """
    cnlpt_url = ctakesclient.transformer.get_url_cnlp_negation()

    if not is_url_available(cnlpt_url):
        errors.fatal(
            f"A running cNLP transformers server was not found at:\n    {cnlpt_url}\n\n"
            "Please set the URL_CNLP_NEGATION environment variable or start the docker support services.",
            errors.CNLPT_MISSING,
        )


@contextlib.contextmanager
def wait_for_ctakes_restart():
    """
    Waits for cTAKES to restart after the block of code being managed is finished.

    This is used when replacing the custom dictionary (symptoms.bsv) that cTAKES uses.
    When replacing it, cTAKES will restart itself, and this context manager avoids race conditions like
    trying to use the old cTAKES before it has restarted or trying to use the new cTAKES before it is ready.
    """
    url = urllib.parse.urlparse(ctakesclient.client.get_url_ctakes_rest())

    # *** Acquire socket connection with cTAKES (cTAKES is required to exist already) ***
    connection = socket.create_connection((url.hostname, url.port))
    poller = select.poll()
    poller.register(connection, select.POLLRDHUP)  # will watch for remote disconnect (death or remote timeout)

    # *** Yield to caller ***
    yield

    # *** Wait for cTAKES to shut down ***
    # This will stop in 20s regardless, as the server will time our connection out.
    # But that is plenty of time for cTAKES to notice our changes, which is usually on the order of milliseconds.
    # So we set 20s as well because having no timeout at all is a bad idea (and for misbehaving tests).
    if not poller.poll(20000):
        errors.fatal(
            "Timed out waiting for cTAKES server to restart.\n"
            "Are you using an up to date smartonfhir/ctakes-covid image?",
            errors.CTAKES_RESTART_FAILED,
        )

    # *** Wait for cTAKES to come back up ***
    # Sleep for a bit to give the server a little more time to die, as it might accept incoming connections
    # while being killed, and we'd be tricked into thinking that it had come up again.
    time.sleep(1)
    check_ctakes()
