from cumulus_etl import formats
from tests import utils


class TestFormats(utils.AsyncTestCase):
    def test_invalid_format(self):
        with self.assertRaisesRegex(ValueError, "Unknown output format name 'blarg'."):
            formats.get_format_class("blarg")
