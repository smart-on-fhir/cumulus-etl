"""Tests for oracle connections"""

import unittest
from cumulus.loaders.i2b2.oracle import connect


@unittest.skip('needs a running oracle server currently')
class TestSQLDump(unittest.TestCase):
    """Test case for connecting to oracle"""

    def test_connect(self):
        connect.connect()


if __name__ == '__main__':
    unittest.main()
