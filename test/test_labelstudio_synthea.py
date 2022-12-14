import unittest
import os
from cumulus import common
from cumulus.labelstudio import LabelStudio

class TestLabelStudioSynthea(unittest.TestCase):

    def test_save_labelstudio(self, folder='/Users/andy/binrepos/NLP_to_FHIR/fake_synthea_notes'):
        for filename in os.listdir(folder):
            if filename.endswith('.txt'):
                path = os.path.join(folder, filename)
                synthetic_note = common.read_text(path)

                data = LabelStudio(synthetic_note, None).as_json()
                common.write_json(path=f'{path}.json', data=data, indent=4)


if __name__ == '__main__':
    unittest.main()
