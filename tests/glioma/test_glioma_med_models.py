"""Tests for Glioma medication models helpers"""

import ddt

from cumulus_etl.etl.studies.glioma.glioma_med_models import drug_type_field
from tests import utils


@ddt.ddt
class TestGliomaMedModelHelpers(utils.AsyncTestCase):
    """Test case for glioma's medication model helpers."""

    @ddt.data(
        (None, "chemotherapy"),
        ("some_default", "chemotherapy"),
        ("another_default", "another cancer drug type"),
    )
    @ddt.unpack
    def test_drug_type_field(self, default, drug_type):
        """Test the drug_type_field helper function."""
        field = drug_type_field(default=default, drug_type=drug_type)
        self.assertEqual(field.default, default)
        self.assertIn(drug_type, field.description)
        self.assertIn("Extract the", field.description)
        self.assertIn(
            "class or therapy modality documented for this medication, if present",
            field.description,
        )
