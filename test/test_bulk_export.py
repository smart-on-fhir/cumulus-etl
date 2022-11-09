"""Tests for bulk export support"""

import os
import tempfile
import time
import unittest
from io import BytesIO
from json import dumps
from unittest import mock

import ddt
import freezegun
import responses
from jwcrypto import jwk, jwt
from requests.adapters import HTTPAdapter
from responses import matchers
from urllib3 import HTTPResponse

from cumulus import common, etl, loaders, store
from cumulus.loaders.fhir.backend_service import BackendServiceServer, FatalError
from cumulus.loaders.fhir.bulk_export import BulkExporter


def make_response(status_code=200, json=None, text=None, reason=None, headers=None):
    """Makes a fake response for ease of testing"""
    headers = dict(headers or {})
    headers.setdefault('Content-Type', 'application/json' if json else 'text/plain; charset=utf-8')
    json = dumps(json) if json else None
    body = (json or text or '').encode('utf8')
    http_response = HTTPResponse(
        status=status_code,
        body=BytesIO(body),
        reason=reason,
        headers=headers or {},
    )
    response = HTTPAdapter().build_response(mock.MagicMock(url='fake_request_url'), http_response)
    response.raw = BytesIO(body)
    return response


class TestBulkLoader(unittest.TestCase):
    """
    Test case for bulk export support in the etl pipeline and ndjson loader.

    i.e. tests for fhir_ndjson.py

    This does no actual bulk loading.
    """

    def setUp(self):
        super().setUp()
        self.root = store.Root('http://localhost:9999')

        self.jwks_file = tempfile.NamedTemporaryFile()  # pylint: disable=consider-using-with
        self.jwks_path = self.jwks_file.name
        self.jwks_file.write(b'{"fake":"jwks"}')
        self.jwks_file.flush()

        # Mock out the backend service and bulk export code by default. We don't care about actually doing any
        # bulk work in this test case, just confirming the flow.

        server_patcher = mock.patch('cumulus.loaders.fhir.fhir_ndjson.BackendServiceServer')
        self.addCleanup(server_patcher.stop)
        self.mock_server = server_patcher.start()

        exporter_patcher = mock.patch('cumulus.loaders.fhir.fhir_ndjson.BulkExporter')
        self.addCleanup(exporter_patcher.stop)
        self.mock_exporter = exporter_patcher.start()

    @mock.patch('cumulus.etl.loaders.FhirNdjsonLoader')
    def test_etl_passes_args(self, mock_loader):
        """Verify that we are passed the client ID and JWKS from the command line"""
        mock_loader.side_effect = ValueError  # just to stop the etl pipeline once we get this far

        with self.assertRaises(ValueError):
            etl.main(['http://localhost:9999', '/tmp/output', '/tmp/phi', '--skip-init-checks', '--input-format=ndjson',
                      '--smart-client-id=x', '--smart-jwks=y'])

        self.assertEqual(1, mock_loader.call_count)
        self.assertEqual('x', mock_loader.call_args[1]['client_id'])
        self.assertEqual('y', mock_loader.call_args[1]['jwks'])

    def test_required_arguments(self):
        """Verify that we require both a client ID and a JWK Set"""
        # No SMART args at all
        with self.assertRaises(SystemExit):
            loaders.FhirNdjsonLoader(self.root).load_all()

        # No JWKS
        with self.assertRaises(SystemExit):
            loaders.FhirNdjsonLoader(self.root, client_id='foo').load_all()

        # No client ID
        with self.assertRaises(SystemExit):
            loaders.FhirNdjsonLoader(self.root, jwks=self.jwks_path).load_all()

        # Works fine if both given
        loaders.FhirNdjsonLoader(self.root, client_id='foo', jwks=self.jwks_path).load_all()

    def test_reads_client_id_from_file(self):
        """Verify that we require both a client ID and a JWK Set."""
        # First, confirm string is used directly if file doesn't exist
        loader = loaders.FhirNdjsonLoader(self.root, client_id='/direct-string')
        self.assertEqual('/direct-string', loader.client_id)

        # Now read from a file that exists
        with tempfile.NamedTemporaryFile() as file:
            file.write(b'\ninside-file\n')
            file.flush()
            loader = loaders.FhirNdjsonLoader(self.root, client_id=file.name)
            self.assertEqual('inside-file', loader.client_id)

    def test_export_flow(self):
        """
        Verify that we make all the right calls into the bulk export helper classes.

        This is a little lower-level than I would normally test, but the benefit of ensuring this flow here is that
        the other test cases can focus on just the helper classes and trust that the flow works, without us needing to
        do the full flow each time.
        """
        mock_server_instance = mock.MagicMock()
        self.mock_server.return_value = mock_server_instance
        mock_exporter_instance = mock.MagicMock()
        self.mock_exporter.return_value = mock_exporter_instance

        loaders.FhirNdjsonLoader(self.root, client_id='foo', jwks=self.jwks_path).load_all()

        expected_resources = [
            'Condition',
            'DocumentReference',
            'Encounter',
            'Observation',
            'Patient',
        ]

        self.assertEqual(1, self.mock_server.call_count)
        self.assertEqual(((self.root.path, 'foo', {'fake': 'jwks'}, expected_resources), ), self.mock_server.call_args)

        self.assertEqual(1, self.mock_exporter.call_count)
        self.assertEqual(mock_server_instance, self.mock_exporter.call_args[0][0])
        self.assertEqual(expected_resources, self.mock_exporter.call_args[0][1])

        self.assertEqual(1, mock_exporter_instance.export.call_count)

    def test_fatal_errors_are_fatal(self):
        """Verify that when a FatalError is raised, we do really quit"""
        self.mock_server.side_effect = FatalError

        with self.assertRaises(SystemExit) as cm:
            loaders.FhirNdjsonLoader(self.root, client_id='foo', jwks=self.jwks_path).load_all()

        self.assertEqual(1, self.mock_server.call_count)
        self.assertEqual(2, cm.exception.code)


@ddt.ddt
@freezegun.freeze_time('Sep 15th, 2021 1:23:45')
@mock.patch('cumulus.loaders.fhir.backend_service.uuid.uuid4', new=lambda: '1234')
class TestBulkServer(unittest.TestCase):
    """
    Test case for bulk export server oauth2 / request support.

    i.e. tests for backend_service.py
    """

    def setUp(self):
        super().setUp()

        # By default, set up a working server and auth. Tests can break things as needed.

        self.client_id = 'my-client-id'
        self.jwk = jwk.JWK.generate(kty='RSA', alg='RS384', kid='a', key_ops=['sign', 'verify']).export(as_dict=True)
        self.jwks = {'keys': [self.jwk]}
        self.server_url = 'https://example.com/fhir'
        self.token_url = 'https://auth.example.com/token'

        # Generate expected JWT
        token = jwt.JWT(
            header={
                'alg': 'RS384',
                'kid': 'a',
                'typ': 'JWT',
            },
            claims={
                'iss': self.client_id,
                'sub': self.client_id,
                'aud': self.token_url,
                'exp': int(time.time()) + 299,  # aided by freezegun not changing time under us
                'jti': '1234',
            },
        )
        token.make_signed_token(key=jwk.JWK(**self.jwk))
        self.expected_jwt = token.serialize()

        # Initialize responses mock
        self.responses = responses.RequestsMock(assert_all_requests_are_fired=False)
        self.addCleanup(self.responses.stop)
        self.responses.start()

        # We ask for smart-configuration to discover the token endpoint
        self.smart_configuration = {
            'capabilities': ['client-confidential-asymmetric'],
            'token_endpoint': self.token_url,
            'token_endpoint_auth_methods_supported': ['private_key_jwt'],
            'token_endpoint_auth_signing_alg_values_supported': ['RS384'],
        }
        self.responses.get(
            f'{self.server_url}/.well-known/smart-configuration',
            match=[matchers.header_matcher({'Accept': 'application/json'})],
            json=self.smart_configuration,
        )

        # Set up mocks for fhirclient (we don't need to test its oauth code by mocking server responses there)
        self.mock_client = mock.MagicMock()  # FHIRClient instance
        self.mock_server = self.mock_client.server  # FHIRServer instance
        client_patcher = mock.patch('cumulus.loaders.fhir.backend_service.FHIRClient')
        self.addCleanup(client_patcher.stop)
        self.mock_client_class = client_patcher.start()  # FHIRClient class
        self.mock_client_class.return_value = self.mock_client

    def test_auth_initial_authorize(self):
        """Verify that we authorize correctly upon class initialization"""
        BackendServiceServer(self.server_url, self.client_id, self.jwks, ['Condition', 'Patient'])

        # Check initialization of FHIRClient
        self.assertListEqual([mock.call(settings={
            'api_base': f'{self.server_url}/',
            'app_id': self.client_id,
            'jwt_token': self.expected_jwt,
            'scope': 'system/Condition.read system/Patient.read',
        })], self.mock_client_class.call_args_list)

        # Check authorization calls to FHIRClient
        self.assertFalse(self.mock_client.wants_patient)  # otherwise fhirclient adds scopes
        self.assertListEqual([mock.call()], self.mock_client.prepare.call_args_list)
        self.assertListEqual([mock.call()], self.mock_client.authorize.call_args_list)

    def test_get_with_new_header(self):
        """Verify that we issue a GET correctly for the happy path"""
        server = BackendServiceServer(self.server_url, self.client_id, self.jwks, [])

        # This is mostly confirming that we call mocks correctly, but that's important since we're mocking out all
        # of fhirclient. Since we do that, we need to confirm we're driving it well.

        # With new header and stream
        server.request('GET', 'foo', headers={'Test': 'Value'}, stream=True)
        self.assertEqual([mock.call({
            'Accept': 'application/fhir+json',
            'Accept-Charset': 'UTF-8',
            'Test': 'Value',
        })], self.mock_server.auth.signed_headers.call_args_list)
        self.assertEqual([mock.call(
            'GET',
            f'{self.server_url}/foo',
            headers=self.mock_server.auth.signed_headers.return_value,
            stream=True,
        )], self.mock_server.session.request.call_args_list)

    def test_get_with_overriden_header(self):
        """Verify that we issue a GET correctly for the happy path"""
        server = BackendServiceServer(self.server_url, self.client_id, self.jwks, [])

        # With overriding a header and default stream (False)
        server.request('GET', 'bar', headers={'Accept': 'text/plain'})
        self.assertEqual([mock.call({
            'Accept': 'text/plain',
            'Accept-Charset': 'UTF-8',
        })], self.mock_server.auth.signed_headers.call_args_list)
        self.assertEqual([mock.call(
            'GET',
            f'{self.server_url}/bar',
            headers=self.mock_server.auth.signed_headers.return_value,
            stream=False,
        )], self.mock_server.session.request.call_args_list)

    @ddt.data(
        {},  # no keys
        {'keys': [{'alg': 'RS384'}]},  # no key op
        {'keys': [{'alg': 'RS384', 'key_ops': ['verify'], 'kid': 'a'}]},  # bad key op
        {'keys': [{'alg': 'RS128', 'key_ops': ['sign']}], 'kid': 'a'},  # bad algo
        {'keys': [{'alg': 'RS384', 'key_ops': ['sign']}]},  # no kid
    )
    def test_jwks_without_suitable_key(self, bad_jwks):
        with self.assertRaisesRegex(FatalError, 'No private ES384 or RS384 key found'):
            BackendServiceServer(self.server_url, self.client_id, bad_jwks, [])

    @ddt.data(
        {'token_endpoint_auth_methods_supported': None},
        {'token_endpoint_auth_methods_supported': ['nope']},
        {'token_endpoint_auth_signing_alg_values_supported': None},
        {'token_endpoint_auth_signing_alg_values_supported': ['nope']},
        {'token_endpoint': None},
        {'token_endpoint': ''},
    )
    def test_bad_smart_config(self, bad_config_override):
        """Verify that we require fully correct smart configurations."""
        for entry, value in bad_config_override.items():
            if value is None:
                del self.smart_configuration[entry]
            else:
                self.smart_configuration[entry] = value

        self.responses.reset()
        self.responses.get(
            f'{self.server_url}/.well-known/smart-configuration',
            match=[matchers.header_matcher({'Accept': 'application/json'})],
            json=self.smart_configuration,
        )

        with self.assertRaisesRegex(FatalError, 'does not support the client-confidential-asymmetric protocol'):
            BackendServiceServer(self.server_url, self.client_id, self.jwks, [])

    def test_authorize_error_with_response(self):
        """Verify that we translate authorize http response errors into FatalErrors."""
        error = Exception()
        error.response = mock.MagicMock()
        error.response.json.return_value = {'error_description': 'Ouch!'}
        self.mock_client.authorize.side_effect = error
        with self.assertRaisesRegex(FatalError, 'Could not authenticate with the FHIR server: Ouch!'):
            BackendServiceServer(self.server_url, self.client_id, self.jwks, [])

    def test_authorize_error_without_response(self):
        """Verify that we translate authorize non-response errors into FatalErrors."""
        self.mock_client.authorize.side_effect = Exception('no memory')
        with self.assertRaisesRegex(FatalError, 'Could not authenticate with the FHIR server: no memory'):
            BackendServiceServer(self.server_url, self.client_id, self.jwks, [])

    def test_get_error_401(self):
        """Verify that an expired token is refreshed."""
        server = BackendServiceServer(self.server_url, self.client_id, self.jwks, [])

        self.mock_server.session.request.side_effect = [make_response(status_code=401), make_response()]
        self.mock_server.reauthorize.return_value = None  # fhirclient gives None if there is no refresh token

        # Check that we correctly tried to re-authenticate
        self.assertEqual(200, server.request('GET', 'foo').status_code)
        self.assertEqual(1, self.mock_server.reauthorize.call_count)
        self.assertEqual(2, self.mock_client_class.call_count)
        self.assertEqual(2, self.mock_client.prepare.call_count)
        self.assertEqual(2, self.mock_client.authorize.call_count)

    def test_get_error_429(self):
        """Verify that 429 errors are passed through and not treated as exceptions."""
        server = BackendServiceServer(self.server_url, self.client_id, self.jwks, [])

        # Confirm 429 passes
        self.mock_server.session.request.return_value = make_response(status_code=429)
        self.assertEqual(429, server.request('GET', 'foo').status_code)

        # Sanity check that 430 does not
        self.mock_server.session.request.return_value = make_response(status_code=430)
        with self.assertRaises(FatalError):
            server.request('GET', 'foo')

    @ddt.data(
        {'json': {'resourceType': 'OperationOutcome', 'issue': [{'diagnostics': 'testmsg'}]}},  # OperationOutcome
        {'json': {'issue': [{'diagnostics': 'msg'}]}, 'reason': 'testmsg'},  # non-OperationOutcome json
        {'text': 'testmsg'},  # just pure text content
        {'reason': 'testmsg'},
    )
    def test_get_error_other(self, response_args):
        """Verify that other http errors are FatalErrors."""
        server = BackendServiceServer(self.server_url, self.client_id, self.jwks, [])

        self.mock_server.session.request.return_value = make_response(status_code=500, **response_args)
        with self.assertRaisesRegex(FatalError, 'testmsg'):
            server.request('GET', 'foo')


@ddt.ddt
@freezegun.freeze_time('Sep 15th, 2021 1:23:45')
class TestBulkExporter(unittest.TestCase):
    """
    Test case for bulk export logic.

    i.e. tests for bulk_export.py
    """

    def setUp(self):
        super().setUp()
        self.tmpdir = tempfile.TemporaryDirectory()  # pylint: disable=consider-using-with
        self.server = mock.MagicMock()
        self.exporter = BulkExporter(self.server, ['Condition', 'Patient'], self.tmpdir.name)

    def test_happy_path(self):
        """Verify an end-to-end bulk export with no problems and no waiting works as expected"""
        self.server.request.side_effect = [
            make_response(status_code=202, headers={'Content-Location': 'https://example.com/poll'}),  # kickoff
            make_response(json={'output': [
                {'type': 'Condition', 'url': 'https://example.com/con1'},
                {'type': 'Condition', 'url': 'https://example.com/con2'},
                {'type': 'Patient', 'url': 'https://example.com/pat1'},
            ]}),  # status
            make_response(json={'type': 'Condition1'}),  # download
            make_response(json={'type': 'Condition2'}),  # download
            make_response(json={'type': 'Patient1'}),  # download
            make_response(status_code=202),  # delete request
        ]

        self.exporter.export()

        self.assertListEqual([
            mock.call('GET', '$export?_type=Condition,Patient&_since=1800-01-01T00:00:00Z',
                      headers={'Prefer': 'respond-async'}),
            mock.call('GET', 'https://example.com/poll', headers={'Accept': 'application/json'}),
            mock.call('GET', 'https://example.com/con1', headers={'Accept': 'application/fhir+ndjson'}, stream=True),
            mock.call('GET', 'https://example.com/con2', headers={'Accept': 'application/fhir+ndjson'}, stream=True),
            mock.call('GET', 'https://example.com/pat1', headers={'Accept': 'application/fhir+ndjson'}, stream=True),
            mock.call('DELETE', 'https://example.com/poll', headers=None),
        ], self.server.request.call_args_list)

        self.assertEqual({'type': 'Condition1'}, common.read_json(f'{self.tmpdir.name}/Condition.000.ndjson'))
        self.assertEqual({'type': 'Condition2'}, common.read_json(f'{self.tmpdir.name}/Condition.001.ndjson'))
        self.assertEqual({'type': 'Patient1'}, common.read_json(f'{self.tmpdir.name}/Patient.000.ndjson'))

    def test_export_error(self):
        """Verify that we download and present any server-reported errors during the bulk export"""
        self.server.request.side_effect = [
            make_response(status_code=202, headers={'Content-Location': 'https://example.com/poll'}),  # kickoff
            make_response(json={
                'error': [
                    {'type': 'OperationOutcome', 'url': 'https://example.com/err1'},
                    {'type': 'OperationOutcome', 'url': 'https://example.com/err2'},
                ],
                'output': [  # include an output too, to confirm we don't bother trying to download it
                    {'type': 'Condition', 'url': 'https://example.com/con1'},
                ]
            }),  # status
            make_response(json={'type': 'OperationOutcome', 'issue': [{'diagnostics': 'errmsg1'}]}),  # error
            make_response(json={'type': 'OperationOutcome', 'issue': [{'diagnostics': 'errmsg2'}]}),  # error
            make_response(status_code=202),  # delete request
        ]

        with self.assertRaisesRegex(FatalError, 'Errors occurred during export:\n - errmsg1\n - errmsg2'):
            self.exporter.export()

        self.assertListEqual([
            mock.call('GET', '$export?_type=Condition,Patient&_since=1800-01-01T00:00:00Z',
                      headers={'Prefer': 'respond-async'}),
            mock.call('GET', 'https://example.com/poll', headers={'Accept': 'application/json'}),
            mock.call('GET', 'https://example.com/err1', headers=None),
            mock.call('GET', 'https://example.com/err2', headers=None),
            mock.call('DELETE', 'https://example.com/poll', headers=None),
        ], self.server.request.call_args_list)

    def test_unexpected_status_code(self):
        """Verify that we bail if we see a successful code we don't understand"""
        self.server.request.return_value = make_response(status_code=204)  # "no content"
        with self.assertRaisesRegex(FatalError, 'Unexpected status code 204'):
            self.exporter.export()

    @mock.patch('cumulus.loaders.fhir.bulk_export.time.sleep')
    def test_delay(self, mock_sleep):
        """Verify that we wait the amount of time the server asks us to"""
        self.server.request.side_effect = [
            # Kicking off bulk export
            make_response(status_code=429, headers={'Retry-After': '3600'}),  # one hour
            make_response(status_code=202, headers={'Content-Location': 'https://example.com/poll'}),  # kickoff done
            # Checking status of bulk export
            make_response(status_code=429),  # default of one minute
            make_response(status_code=202, headers={'Retry-After': '18000'}),  # five hours
            make_response(status_code=429, headers={'Retry-After': '64800'}),  # 18 hours (putting us over a day)
        ]

        with self.assertRaisesRegex(FatalError, 'Timed out waiting'):
            self.exporter.export()

        # 86460 == 24 hours + one minute
        self.assertEqual(86460, self.exporter._total_wait_time)  # pylint: disable=protected-access

        self.assertListEqual([
            mock.call(3600),
            mock.call(60),
            mock.call(18000),
            mock.call(64800),
        ], mock_sleep.call_args_list)

    def test_delete_if_interrupted(self):
        """Verify that we still delete the export on the server if we raise an exception during the middle of export"""
        self.server.request.side_effect = [
            make_response(status_code=202, headers={'Content-Location': 'https://example.com/poll'}),  # kickoff done
            FatalError('Test Status Call Failed'),  # status error
            make_response(status_code=501),  # also verify that an error during delete does not override the first
        ]

        with self.assertRaisesRegex(FatalError, 'Test Status Call Failed'):
            self.exporter.export()

        self.assertListEqual([
            mock.call('GET', '$export?_type=Condition,Patient&_since=1800-01-01T00:00:00Z',
                      headers={'Prefer': 'respond-async'}),
            mock.call('GET', 'https://example.com/poll', headers={'Accept': 'application/json'}),
            mock.call('DELETE', 'https://example.com/poll', headers=None),
        ], self.server.request.call_args_list)


class TestBulkExportEndToEnd(unittest.TestCase):
    """
    Test case for doing an entire bulk export loop, without mocking python code.

    Server responses are mocked, but that's it. This is more of a functional test case than a unit test case.
    """

    def setUp(self) -> None:
        super().setUp()

        self.root = store.Root('http://localhost:9999/fhir')
        self.client_id = 'test-client-id'

        self.jwks_file = tempfile.NamedTemporaryFile()  # pylint: disable=consider-using-with
        jwk_token = jwk.JWK.generate(kty='EC', alg='ES384', curve='P-384', kid='a',
                                     key_ops=['sign', 'verify']).export(as_dict=True)
        jwks = {'keys': [jwk_token]}
        self.jwks_file.write(dumps(jwks).encode('utf8'))
        self.jwks_file.flush()
        self.jwks_path = self.jwks_file.name

        # === Now set up requests ===

        # /.well-known/smart-configuration
        responses.get(
            f'{self.root.path}/.well-known/smart-configuration',
            match=[matchers.header_matcher({'Accept': 'application/json'})],
            json={
                'capabilities': ['client-confidential-asymmetric'],
                'token_endpoint': f'{self.root.path}/token',
                'token_endpoint_auth_methods_supported': ['private_key_jwt'],
                'token_endpoint_auth_signing_alg_values_supported': ['ES384'],
            },
        )

        # /metadata (most of this is just to pass validation -- this endpoint is just for fhirclient to get a token url)
        responses.get(
            f'{self.root.path}/metadata',
            json={
                'date': '1900-01-01',
                'fhirVersion': '4.0.1',
                'format': ['application/fhir+json'],
                'kind': 'instance',
                'resourceType': 'CapabilityStatement',
                'rest': [{
                    'mode': 'server',
                    'security': {
                        'extension': [{
                            'url': 'http://fhir-registry.smarthealthit.org/StructureDefinition/oauth-uris',
                            'extension': [
                                # Notably only offer a token URL, just like the bulk-data-server has.
                                # Some versions of fhirclient also expect an authorize URL, but we should still work
                                # in cases where that isn't available.
                                {'url': 'token', 'valueUri': f'{self.root.path}/token'}
                            ],
                        }],
                    },
                }],
                'status': 'active',
            },
        )

        # /token
        responses.post(
            f'{self.root.path}/token',
            json={
                'access_token': '1234567890',
            },
        )

        # /$export
        responses.get(
            f'{self.root.path}/$export',
            match=[
                matchers.header_matcher({
                    'Accept': 'application/fhir+json',
                    'Authorization': 'Bearer 1234567890',
                    'Prefer': 'respond-async',
                }),
                matchers.query_param_matcher({
                    '_type': 'Condition,DocumentReference,Encounter,Observation,Patient',
                    '_since': '1800-01-01T00:00:00Z',
                })
            ],
            headers={'Content-Location': f'{self.root.path}/poll'},
            status=202,
        )

        # /poll
        responses.get(
            f'{self.root.path}/poll',
            match=[matchers.header_matcher({
                'Accept': 'application/json',
                'Authorization': 'Bearer 1234567890',
            })],
            json={
                'output': [{'type': 'Patient', 'url': f'{self.root.path}/download/patient1'}],
            },
        )

        # /download/patient1
        responses.get(
            f'{self.root.path}/download/patient1',
            match=[matchers.header_matcher({
                'Accept': 'application/fhir+ndjson',
                'Authorization': 'Bearer 1234567890',
            })],
            json={  # content doesn't really matter
                'id': 'testPatient1',
                'resourceType': 'Patient',
            },
        )

        # DELETE /poll
        responses.delete(
            f'{self.root.path}/poll',
            match=[matchers.header_matcher({
                'Accept': 'application/fhir+json',
                'Authorization': 'Bearer 1234567890',
            })],
            status=202,
        )

    @responses.activate(assert_all_requests_are_fired=True)
    def test_successful_bulk_export(self):
        """Verify a happy path bulk export, from toe to tip"""
        loader = loaders.FhirNdjsonLoader(self.root, client_id=self.client_id, jwks=self.jwks_path)
        tmpdir = loader.load_all()

        self.assertEqual(
            {'id': 'testPatient1', 'resourceType': 'Patient'},
            common.read_json(os.path.join(tmpdir.name, 'Patient.000.ndjson')),
        )
