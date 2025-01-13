"""Base classes for ETL-oriented tests"""

import datetime
import os
import shutil
import tempfile
from unittest import mock

import pytest

from cumulus_etl import cli, common, deid, fhir
from cumulus_etl.etl.config import JobConfig
from tests import ctakesmock, utils


@pytest.mark.skipif(not shutil.which(deid.MSTOOL_CMD), reason="MS tool not installed")
class BaseEtlSimple(ctakesmock.CtakesMixin, utils.TreeCompareMixin, utils.AsyncTestCase):
    """
    Base test case for basic runs of etl methods

    Subclasses may want to override self.input_path to point at their own input data.

    Don't put actual tests in here, but rather in subclasses below.
    """

    # Subclasses may want to override this with a folder that has input/, output/, and a codebook.json
    DATA_ROOT = "simple"

    def setUp(self):
        super().setUp()

        self.root_path = os.path.join(self.datadir, self.DATA_ROOT)
        self.input_path = os.path.join(self.root_path, "input")

        self.tmpdir = tempfile.mkdtemp()
        # Comment out this next line when debugging, to persist directory
        self.addCleanup(shutil.rmtree, self.tmpdir)

        self.output_path = os.path.join(self.tmpdir, "output")
        self.phi_path = os.path.join(self.tmpdir, "phi")

        self.enforce_consistent_uuids()

    async def run_etl(
        self,
        *args,
        input_path=None,
        output_path=None,
        phi_path=None,
        output_format: str | None = "ndjson",
        comment=None,
        batch_size=None,
        tasks=None,
        tags: list[str] | None = None,
        philter=True,
        errors_to=None,
        export_to: str | None = None,
        input_format: str = "ndjson",
        export_group: str = "test-group",
        export_timestamp: str = "2020-10-13T12:00:20-05:00",
        skip_init_checks: bool = True,
    ) -> None:
        args = [
            input_path or self.input_path,
            output_path or self.output_path,
            phi_path or self.phi_path,
            f"--input-format={input_format}",
            f"--ctakes-overrides={self.ctakes_overrides.name}",
            *args,
        ]
        if skip_init_checks:
            args.append("--skip-init-checks")
        if export_group is not None:
            args.append(f"--export-group={export_group}")
        if export_timestamp:
            args.append(f"--export-timestamp={export_timestamp}")
        if output_format:
            args.append(f"--output-format={output_format}")
        if comment:
            args.append(f"--comment={comment}")
        if batch_size:
            args.append(f"--batch-size={batch_size}")
        if tasks:
            args.append(f"--task={','.join(tasks)}")
        if tags:
            args.append(f"--task-filter={','.join(tags)}")
        if philter:
            args.append("--philter")
        if export_to:
            args.append(f"--export-to={export_to}")
        if errors_to:
            args.append(f"--errors-to={errors_to}")
        await cli.main(args)

    def enforce_consistent_uuids(self):
        """Make sure that UUIDs will be the same from run to run"""
        # First, copy codebook over. This will help ensure that the order of
        # calls doesn't matter as much. If *every* UUID were recorded in the
        # codebook, this is all we'd need to do.
        os.makedirs(self.phi_path)
        shutil.copy(os.path.join(self.root_path, "codebook.json"), self.phi_path)

    def assert_output_equal(self, folder: str = "output"):
        """Compares the etl output with the expected json structure"""
        self.assert_etl_output_equal(os.path.join(self.root_path, folder), self.output_path)


class TaskTestCase(utils.AsyncTestCase):
    """Base class for task-focused test suites"""

    def setUp(self) -> None:
        super().setUp()

        client = fhir.FhirClient("http://localhost/", [])
        self.tmpdir = self.make_tempdir()
        self.input_dir = os.path.join(self.tmpdir, "input")
        self.output_dir = os.path.join(self.tmpdir, "output")
        self.phi_dir = os.path.join(self.tmpdir, "phi")
        self.errors_dir = os.path.join(self.tmpdir, "errors")
        os.makedirs(self.input_dir)
        os.makedirs(self.phi_dir)
        self.json_file_count = 0

        self.export_url = "https://example.com/Group/test-group/$export"
        self.job_config = JobConfig(
            self.input_dir,
            self.input_dir,
            self.output_dir,
            self.phi_dir,
            "ndjson",
            "ndjson",
            client,
            timestamp=common.datetime_now(),
            batch_size=5,
            dir_errors=self.errors_dir,
            export_group_name="test-group",
            export_datetime=datetime.datetime(
                2012, 10, 10, 5, 30, 12, tzinfo=datetime.timezone.utc
            ),
            export_url=self.export_url,
        )

        def make_formatter(dbname: str, **kwargs):
            formatter = mock.MagicMock(dbname=dbname, **kwargs)
            self.format_count += 1
            if self.format_count == 1:
                self.format = self.format or formatter
                return self.format
            elif self.format_count == 2:
                self.format2 = self.format2 or formatter
                return self.format2
            elif self.format_count == 3:
                self.format3 = self.format3 or formatter
                return self.format3
            else:
                return formatter  # stop keeping track

        self.format = None
        self.format2 = None  # etl__completion (or second output table for a few tasks)
        self.format3 = None  # etl__completion for double-table tasks
        self.format_count = 0
        self.create_formatter_mock = mock.MagicMock(side_effect=make_formatter)
        self.job_config.create_formatter = self.create_formatter_mock

        self.scrubber = deid.Scrubber()
        self.codebook = self.scrubber.codebook

        # Keeps consistent IDs
        shutil.copy(os.path.join(self.datadir, "simple/codebook.json"), self.phi_dir)

    def make_json(self, resource_type, resource_id, **kwargs):
        self.json_file_count += 1
        filename = f"{self.input_dir}/{self.json_file_count}.ndjson"
        with common.NdjsonWriter(filename) as writer:
            writer.write({"resourceType": resource_type, **kwargs, "id": resource_id})
