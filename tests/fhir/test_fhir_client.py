"""Tests for FhirClient and similar"""

import argparse
from unittest import mock

import ddt

from cumulus_etl import errors, fhir, store
from tests.utils import AsyncTestCase


@ddt.ddt
class TestFhirClient(AsyncTestCase):
    """
    Test case for FHIR client oauth2 / request support.

    i.e. tests for fhir_client.py
    """

    def test_bad_args_exits(self):
        """Verify that we detect the auth root for an input URL"""
        args = argparse.Namespace(
            fhir_url=None,
            smart_client_id="id-but-no-key",
            smart_jwks=None,
            smart_key=None,
            basic_user=None,
            basic_passwd=None,
            bearer_token=None,
        )
        with self.assertRaises(SystemExit) as cm:
            fhir.create_fhir_client_for_cli(args, store.Root("http://example.invalid/root/"), [])
        self.assertEqual(cm.exception.code, errors.ARGS_INVALID)

    @mock.patch("cumulus_fhir_support.FhirClient.create_for_cli")
    def test_can_find_auth_root(self, mock_client):
        """Verify that we detect the auth root for an input URL"""
        args = argparse.Namespace(
            fhir_url=None,
            smart_client_id=None,
            smart_jwks=None,
            smart_key=None,
            basic_user=None,
            basic_passwd=None,
            bearer_token=None,
        )
        fhir.create_fhir_client_for_cli(
            args, store.Root("http://example.invalid/root/Group/xxx/$export?_type=Patient"), []
        )
        self.assertEqual("http://example.invalid/root", mock_client.call_args[0][0])

    @ddt.data(
        ({"DocumentReference", "Patient"}, {"Binary", "DocumentReference", "Patient"}),
        ({"MedicationRequest", "Condition"}, {"Medication", "MedicationRequest", "Condition"}),
        ({"Patient"}, {"Patient"}),
    )
    @ddt.unpack
    @mock.patch("cumulus_fhir_support.FhirClient.create_for_cli")
    def test_added_binary_scope(self, resources_in, expected_resources_out, mock_client):
        """Verify that we add a Binary scope if DocumentReference is requested"""
        args = argparse.Namespace(
            fhir_url=None,
            smart_client_id=None,
            smart_jwks=None,
            smart_key=None,
            basic_user=None,
            basic_passwd=None,
            bearer_token=None,
        )
        fhir.create_fhir_client_for_cli(args, store.Root("/tmp"), resources_in)
        self.assertEqual(mock_client.call_args[0][1], expected_resources_out)
