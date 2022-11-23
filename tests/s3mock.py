"""
Support for mocking out an S3 server during a test.

ALso, patch aiobotocore (used by pandas) to work with moto
See https://github.com/aio-libs/aiobotocore/issues/755
"""

import os
import unittest
from typing import Callable
from unittest.mock import MagicMock, patch

import aiobotocore.awsrequest
import aiobotocore.endpoint
import aiohttp
import aiohttp.client_reqrep
import aiohttp.typedefs
import botocore.awsrequest
import botocore.model
from moto import mock_s3


class S3Mixin(unittest.TestCase):
    """Subclass this to automatically support s3:// paths in your tests"""
    def setUp(self):
        super().setUp()

        # Moto recommends clearing out these variables, to avoid using any
        # real credentials floating around in your environment
        os.environ['AWS_ACCESS_KEY_ID'] = 'testing'
        os.environ['AWS_SECRET_ACCESS_KEY'] = 'testing'
        os.environ['AWS_SECURITY_TOKEN'] = 'testing'
        os.environ['AWS_SESSION_TOKEN'] = 'testing'
        os.environ['AWS_DEFAULT_REGION'] = 'us-east-1'

        # Work around https://github.com/aio-libs/aiobotocore/issues/755
        aiobmock = patch_aiobotocore()
        self.addCleanup(aiobmock.stop)
        aiobmock.start()

        # Start Moto's mock S3 server
        s3mock = mock_s3()
        self.addCleanup(s3mock.stop)
        s3mock.start()


###############################################################################
#
# Everything below is a lightly-edited copy of
# https://github.com/aio-libs/aiobotocore/issues/755#issuecomment-1242592893
#
###############################################################################

class MockAWSResponse(aiobotocore.awsrequest.AioAWSResponse):
    """Mock aws response"""
    def __init__(self, response: botocore.awsrequest.AWSResponse):
        super().__init__('', response.status_code, {},
                         MockHttpClientResponse(response))
        self._moto_response = response

    # adapt async methods to use moto's response
    async def _content_prop(self) -> bytes:
        return self._moto_response.content

    async def _text_prop(self) -> str:
        return self._moto_response.text


class MockHttpClientResponse(aiohttp.client_reqrep.ClientResponse):
    """Mock http response"""
    def __init__(self, response: botocore.awsrequest.AWSResponse):
        # pylint: disable=super-init-not-called

        async def read(self, n: int = -1) -> bytes:
            del self, n
            # streaming/range requests. used by s3fs
            return response.content

        self.content = MagicMock(aiohttp.StreamReader)
        self.content.read = read

    @property
    # pylint: disable-next=invalid-overridden-method
    def raw_headers(self) -> aiohttp.typedefs.RawHeaders:
        return tuple()


def patch_aiobotocore():
    def factory(original: Callable) -> Callable:
        def patched_convert_to_response_dict(
            http_response: botocore.awsrequest.AWSResponse,
            operation_model: botocore.model.OperationModel
        ):
            return original(MockAWSResponse(http_response), operation_model)

        return patched_convert_to_response_dict

    original = aiobotocore.endpoint.convert_to_response_dict
    return patch.object(aiobotocore.endpoint, 'convert_to_response_dict',
                        new=factory(original))
