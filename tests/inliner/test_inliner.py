"""Tests for inliner/inliner.py"""

import gzip
import json
import os
from unittest import mock

import cumulus_fhir_support
import ddt

from cumulus_etl import common, errors, inliner, store
from tests import s3mock, utils


class TestInlinerBase(utils.AsyncTestCase):
    """Base class for the inline tests"""

    def setUp(self):
        super().setUp()

        self.input_dir = self.make_tempdir()

        self.requester = self.patch("cumulus_etl.fhir.request_attachment")
        self.requester.return_value = utils.make_response(text="hello")

    async def run_inline(self, resources=None, mimetypes=None) -> None:
        resources = resources or ["DiagnosticReport", "DocumentReference"]
        mimetypes = mimetypes or ["text/html"]
        await inliner.inliner(mock.MagicMock(), store.Root(self.input_dir), resources, mimetypes)

    @staticmethod
    def make_attachment(
        *,
        url: str | None = None,
        content_type: str | None = None,
        data: str | None = None,
        size: int | None = None,
        sha1: str | None = None,
    ) -> dict:
        attachment = {}
        if url:
            attachment["url"] = url
        if content_type:
            attachment["contentType"] = content_type
        if data:
            attachment["data"] = data
        if size:
            attachment["size"] = size
        if sha1:
            attachment["hash"] = sha1
        return attachment

    @staticmethod
    def make_dx_report(*attachments) -> dict:
        return {
            "resourceType": "DiagnosticReport",
            "presentedForm": [*attachments],
        }

    @staticmethod
    def make_docref(*attachments) -> dict:
        if not attachments:
            return {"resourceType": "DocumentReference"}
        return {
            "resourceType": "DocumentReference",
            "content": [{"attachment": attachment} for attachment in attachments],
        }


@ddt.ddt
class TestInliner(TestInlinerBase):
    async def test_happy_path(self):
        """Test a bunch of different scenarios being handled correctly"""
        with common.NdjsonWriter(f"{self.input_dir}/docrefs.jsonl") as writer:
            # No attachments
            writer.write(self.make_docref())
            # Already has inlined data
            writer.write(
                self.make_docref(
                    self.make_attachment(url="Binary/x", data="xxx", content_type="text/html")
                ),
            )
            # Has neither url nor data
            writer.write(self.make_docref(self.make_attachment(content_type="text/html")))
            # Does not specify a content type (ignored currently)
            writer.write(self.make_docref(self.make_attachment(url="Binary/h")))
            # Not even a docref! Not a normal thing to happen
            writer.write({"resourceType": "Patient"})
            # Several attachments, with some different, some same content types
            writer.write(
                self.make_docref(
                    self.make_attachment(url="Binary/h1", content_type="text/html"),
                    self.make_attachment(url="Binary/p", content_type="text/plain"),
                    # Just a second one, to confirm we *do* handle multiple attachments, not just the
                    # first one. Add size/hash too, to confirm they are overridden.
                    self.make_attachment(
                        url="Binary/h2", content_type="text/html", size=1, sha1="x"
                    ),
                )
            )
        with common.NdjsonWriter(f"{self.input_dir}/reports.jsonl") as writer:
            # Confirm we handle DiagnosticReports too
            writer.write(
                self.make_dx_report(
                    self.make_attachment(url="Binary/dxr1", content_type="text/html"),
                    self.make_attachment(url="Binary/dxr2", content_type="text/html"),
                )
            )

        await self.run_inline()

        self.assertEqual({"docrefs.jsonl", "reports.jsonl"}, set(os.listdir(self.input_dir)))

        docref_rows = list(
            cumulus_fhir_support.read_multiline_json(f"{self.input_dir}/docrefs.jsonl")
        )
        self.assertEqual(
            docref_rows,
            [
                # These first few are left alone
                self.make_docref(),
                self.make_docref(
                    self.make_attachment(url="Binary/x", data="xxx", content_type="text/html"),
                ),
                self.make_docref(self.make_attachment(content_type="text/html")),
                self.make_docref(self.make_attachment(url="Binary/h")),
                {"resourceType": "Patient"},
                # OK here we do handle a few
                self.make_docref(
                    self.make_attachment(
                        url="Binary/h1",
                        content_type="text/html; charset=utf-8",
                        data="aGVsbG8=",
                        size=5,
                        sha1="qvTGHdzF6KLavt4PO0gs2a6pQ00=",
                    ),
                    self.make_attachment(url="Binary/p", content_type="text/plain"),
                    self.make_attachment(
                        url="Binary/h2",
                        content_type="text/html; charset=utf-8",
                        data="aGVsbG8=",
                        size=5,
                        sha1="qvTGHdzF6KLavt4PO0gs2a6pQ00=",
                    ),
                ),
            ],
        )

        report_rows = list(
            cumulus_fhir_support.read_multiline_json(f"{self.input_dir}/reports.jsonl")
        )
        self.assertEqual(
            report_rows,
            [
                # OK here we do handle a few
                self.make_dx_report(
                    self.make_attachment(
                        url="Binary/dxr1",
                        content_type="text/html; charset=utf-8",
                        data="aGVsbG8=",
                        size=5,
                        sha1="qvTGHdzF6KLavt4PO0gs2a6pQ00=",
                    ),
                    self.make_attachment(
                        url="Binary/dxr2",
                        content_type="text/html; charset=utf-8",
                        data="aGVsbG8=",
                        size=5,
                        sha1="qvTGHdzF6KLavt4PO0gs2a6pQ00=",
                    ),
                ),
            ],
        )

    async def test_graceful_errors(self):
        """Verify that we don't bail if a network error happens"""

        def requester(_client, attachment):
            if attachment["url"].endswith("/501"):
                raise errors.FatalNetworkError("fatal", None)
            if attachment["url"].endswith("/502"):
                raise errors.TemporaryNetworkError("temp", None)
            return utils.make_response(text="bye")

        self.requester.side_effect = requester

        with common.NdjsonWriter(f"{self.input_dir}/docrefs.ndjson") as writer:
            writer.write(
                self.make_docref(
                    self.make_attachment(url="Binary/501", content_type="text/plain"),
                    self.make_attachment(url="Binary/502", content_type="text/plain"),
                    self.make_attachment(url="Binary/valid", content_type="text/plain"),
                )
            )

        await self.run_inline(mimetypes=["text/plain"])

        docref_rows = list(
            cumulus_fhir_support.read_multiline_json(f"{self.input_dir}/docrefs.ndjson")
        )
        self.assertEqual(
            docref_rows,
            [
                self.make_docref(
                    self.make_attachment(url="Binary/501", content_type="text/plain"),
                    self.make_attachment(url="Binary/502", content_type="text/plain"),
                    self.make_attachment(
                        url="Binary/valid",
                        content_type="text/plain; charset=utf-8",
                        data="Ynll",
                        size=3,
                        sha1="eMmlPi8otUPqYsgmas/fNtXGPmE=",
                    ),
                ),
            ],
        )

    async def test_compressed_file(self):
        """Verify we persist any gzip compression"""

        with common.NdjsonWriter(f"{self.input_dir}/docs.ndjson.gz", compressed=True) as writer:
            writer.write(
                self.make_docref(self.make_attachment(url="Binary/p", content_type="text/html")),
            )

        await self.run_inline()

        # This will fail to read any lines if we didn't write back a real gzip file.
        with gzip.open(f"{self.input_dir}/docs.ndjson.gz", "rt", encoding="utf8") as f:
            inlined = json.load(f)

        self.assertEqual(
            inlined,
            self.make_docref(
                self.make_attachment(
                    url="Binary/p",
                    content_type="text/html; charset=utf-8",
                    data="aGVsbG8=",
                    size=5,
                    sha1="qvTGHdzF6KLavt4PO0gs2a6pQ00=",
                ),
            ),
        )


class TestInlinerOnS3(TestInlinerBase, s3mock.S3Mixin):
    def setUp(self):
        super().setUp()
        self.input_dir = self.bucket_url

    async def test_inline_on_s3(self):
        """Quick test that we can read from an arbitrary input dir using fsspec"""
        with common.NdjsonWriter(f"{self.bucket_url}/docrefs.ndjson") as writer:
            writer.write(
                self.make_docref(
                    self.make_attachment(url="Binary/valid", content_type="text/custom"),
                )
            )

        await self.run_inline(mimetypes=["text/custom"])

        docref_rows = list(
            cumulus_fhir_support.read_multiline_json(
                f"{self.bucket_url}/docrefs.ndjson",
                fsspec_fs=self.s3fs,
            )
        )
        self.assertEqual(
            docref_rows,
            [
                self.make_docref(
                    self.make_attachment(
                        url="Binary/valid",
                        content_type="text/custom; charset=utf-8",
                        data="aGVsbG8=",
                        size=5,
                        sha1="qvTGHdzF6KLavt4PO0gs2a6pQ00=",
                    ),
                ),
            ],
        )
