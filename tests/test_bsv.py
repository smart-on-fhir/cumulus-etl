import unittest
from etl import bsv

EXAMPLE = """
278112009|C1314939|involvement
193462001|C0917801|insomnia
24199005|C0085631|Agitation
"""

class TestBSV(unittest.TestCase):

    def ignore_test_concat(self):
        self.assertTrue(False, 'bsv concat bug : some BSV output rows are twice as long as they should be. ')