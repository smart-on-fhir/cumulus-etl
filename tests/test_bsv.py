import unittest

import bsv
import ctakes

EXAMPLE = """
278112009|C1314939|involvement
193462001|C0917801|insomnia
24199005|C0085631|Agitation
"""

class TestBSV(unittest.TestCase):

    def test_concat(self):
        bsv.str_to_bsv_list()




if __name__ == '__main__':
    unittest.main()
