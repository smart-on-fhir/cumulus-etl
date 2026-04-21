"""
Support for mocking out an S3 server during a test.
"""

import os

import cumulus_fhir_support as cfs
import moto
import s3fs
from moto.server import ThreadedMotoServer

from tests import utils


class S3Mixin(utils.AsyncTestCase):
    """Subclass this to automatically support s3:// paths in your tests"""

    ENDPOINT_URL = "http://localhost:5000"

    def setUp(self):
        super().setUp()

        # Moto recommends clearing out these variables, to avoid using any
        # real credentials floating around in your environment
        self.patch_dict(
            os.environ,
            {
                "AWS_ACCESS_KEY_ID": "testing",
                "AWS_SECRET_ACCESS_KEY": "testing",
                "AWS_SECURITY_TOKEN": "testing",
                "AWS_SESSION_TOKEN": "testing",
                "AWS_DEFAULT_REGION": "us-east-1",
            },
        )

        # We use a moto server rather than starting moto.mock_s3() because we've found that
        # aiobotocore (used by s3fs) and moto do not get along well.
        # See https://github.com/aio-libs/aiobotocore/issues/755
        # But an external server avoids all that.
        self.server = ThreadedMotoServer()
        self.server.start()

        s3mock = moto.mock_aws()
        self.addCleanup(s3mock.stop)
        s3mock.start()

        # Insert our new endpoint into the default S3 args
        orig_register = cfs.FsPath.register_options

        def option_wrapper(**kwargs):
            orig_register(**kwargs, endpoint_url=S3Mixin.ENDPOINT_URL)

        option_wrapper()
        self.patch("cumulus_fhir_support.FsPath.register_options", new=option_wrapper)

        # Create a helpful S3FS filesystem already safely using our endpoint, and a starting bucket
        s3fs.S3FileSystem.clear_instance_cache()
        self.s3fs = s3fs.S3FileSystem(endpoint_url=S3Mixin.ENDPOINT_URL)
        self.bucket = "mockbucket"
        self.bucket_path = cfs.FsPath(f"s3://{self.bucket}")

        try:
            self.s3fs.mkdir(self.bucket)  # create the bucket as a quickstart
        except Exception:
            self._kill_moto_server()
            self.fail("Stale moto server")

    def tearDown(self):
        super().tearDown()
        self._kill_moto_server()

    def _kill_moto_server(self):
        self.server.stop()
        s3fs.S3FileSystem.clear_instance_cache()
