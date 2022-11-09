"""Tests for oracle connections"""

import unittest
from cumulus.loaders.i2b2.oracle import connect


@unittest.skip('oracle client drivers fail to load on new OSX')
class TestSQLDump(unittest.TestCase):
    """Test case for connecting to oracle"""

    def test_connect(self):
        connect.connect()

    def test_LD_LIBRARY_PATH(self):
        # Fails on new MACs due to no available Oracle Python Driver :(
        connect.get_library_path()


if __name__ == '__main__':
    unittest.main()
