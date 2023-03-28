"""Monitoring support for cTAKES and cNLP"""

import contextlib
import os
import select
import shutil
import socket
import sys
import time
import urllib.parse

import ctakesclient

from cumulus import cli_utils, errors


def check_ctakes() -> None:
    """
    Verifies that cTAKES is available to receive requests.

    Will block while waiting for cTAKES.
    """
    # Check if our cTAKES server is ready (it may still not be fully ready once the socket is open, but at least it
    # will accept requests and then block the reply on it finishing its initialization)
    ctakes_url = ctakesclient.client.get_url_ctakes_rest()
    if not cli_utils.is_url_available(ctakes_url):
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

    if not cli_utils.is_url_available(cnlpt_url):
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


def restart_ctakes_with_bsv(ctakes_overrides: str, bsv_path: str) -> bool:
    """Hands a new bsv over to cTAKES and waits for it to restart and be ready again with the new bsv file"""
    # This whole setup is slightly janky. But it is designed with these constraints:
    # 1. We'd like to feed cTAKES different custom dictionaries, including ones invented by the user.
    # 2. cTAKES has no ability to accept a new dictionary on the fly.
    # 3. cTAKES is not able to hold multiple dictionaries at once.
    # 4. We're usually running under docker.
    #
    # Taken altogether, if we want to feed cTAKES a dictionary, we need to start it fresh with that dictionary.
    # But one docker cannot manage another docker's lifecycle.
    #
    # So what Cumulus does is use a cTAKES docker image that specifically supports placing override dictionaries
    # in a well-known path (/overrides). The docker image will watch for modifications there and restart cTAKES.
    #
    # Then, we place our custom dictionaries in a folder that is mounted as /overrides on the cTAKES side.
    # In our default docker setup, this is /ctakes-overrides on our side.
    # In other setups, you can pass --ctakes-overrides to set the folder.
    #
    # Because writing a new bsv file will cause a cTAKES restart to happen, we have to beware of race conditions.
    # So we'll use the wait_for_ctakes_restart context manager to ensure cTAKES noticed our change and is ready.

    if not ctakes_overrides:
        # Graceful skipping of this feature if ctakes-override is empty (usually just in tests).
        print("Warning: --ctakes-override is not defined.", file=sys.stderr)
        return False
    elif not os.path.isdir(ctakes_overrides):
        print(
            f"Warning: the cTAKES overrides folder does not exist at:\n  {ctakes_overrides}\n"
            "Consider using --ctakes-overrides.",
            file=sys.stderr,
        )
        return False

    with wait_for_ctakes_restart():
        shutil.copyfile(bsv_path, os.path.join(ctakes_overrides, "symptoms.bsv"))
    return True
