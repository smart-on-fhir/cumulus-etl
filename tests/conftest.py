"""Global test fixtures and setup"""

import tempfile

import pytest

from cumulus_etl import common


@pytest.fixture(autouse=True)
def isolated_temp_dir():
    """The global temp dir should be defined by default"""
    with tempfile.TemporaryDirectory() as tempdir:
        common.set_global_temp_dir(tempdir)
        yield
