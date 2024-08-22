"""
Run text through philter to remove PHI from free-form text.

See https://github.com/SironaMedical/philter-lite for more details.
"""

import os

import nltk
import philter_lite


class Philter:
    """Simple class to abstract away Philter text processing"""

    def __init__(self):
        # Ensure all the nltk data that our filter_config (below) needs is available.
        # In docker deployments, these should already be shipped with our docker image.
        nltk.download("averaged_perceptron_tagger_eng", quiet=True)

        # philter-lite does not seem to have any easy way to reference this default config...?
        filter_config = os.path.join(os.path.dirname(__file__), "philter-config.toml")
        self.filters = philter_lite.load_filters(filter_config)

    def detect_phi(self, text: str) -> philter_lite.CoordinateMap:
        """
        Find PHI spans using Philter.

        :param text: the text to scrub
        :returns: a map of where the spans to scrub are
        """
        include_map, _, _ = philter_lite.detect_phi(text, self.filters)
        return include_map

    def scrub_text(self, text: str) -> str:
        """
        Scrub text of PHI using Philter.

        :param text: the text to scrub
        :returns: the scrubbed text, with PHI replaced by asterisks ("*")
        """
        include_map = self.detect_phi(text)
        return philter_lite.transform_text_asterisk(text, include_map)
