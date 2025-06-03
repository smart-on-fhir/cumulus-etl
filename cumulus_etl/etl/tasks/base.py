"""ETL tasks"""

import contextlib
import dataclasses
import os
from collections.abc import AsyncIterator, Iterator
from typing import ClassVar

import cumulus_fhir_support
import pyarrow
import rich.live
import rich.progress
import rich.table
import rich.text

import cumulus_etl
from cumulus_etl import batching, cli_utils, common, completion, deid, formats, store
from cumulus_etl.etl import config

# Defined here, as syntactic sugar for when you subclass your own task and re-define read_entries()
EntryAtom = dict | list[dict]
EntryIterator = AsyncIterator[EntryAtom | tuple[EntryAtom, ...]]


@dataclasses.dataclass(kw_only=True)
class OutputTable:
    """A collection of properties for a given task output target / table"""

    # *** name ***
    # This field determines the name of the output table.
    # Usually this is just the task name (None means "use the task name")
    name: str | None = None

    def get_name(self, task):
        return self.name or task.name

    # *** resource_type ***
    # This field determines the schema of the output table.
    # Put a FHIR resource name (like "Observation") here, to fill the output table with an appropriate schema.
    # - None disables using a schema
    # - "__same__" means to use the same resource name as our input
    # - Or use any FHIR resource name
    resource_type: str | None = "__same__"

    def get_resource_type(self, task):
        return task.resource if self.resource_type == "__same__" else self.resource_type

    # *** group_field ***
    # Set group_field if your task generates a group of interdependent records (like NLP results from a document).
    # In that example, you might set group_field to "docref_id", as that identifies a single group.
    # See CovidSymptomNlpResultsTask for an example, but keep reading for more details.
    #
    # The goal here is to allow Format classes to remove any old records of this group when reprocessing a task.
    # Consider the case where the NLP logic changes and now your task only generates four records, where it used to
    # generate five. How does Cumulus ETL know to go and delete that fifth record?
    #
    # By specifying group_field (which EtlTask passes to the formatter), you are telling the formatter that it can
    # delete any old members that match on the group field when appending data.
    #
    # This has some implications for how your task streams records from read_entries() though.
    # Instead of streaming record-by-record, stream (yield) a list of records at a time (i.e. a group at a time).
    # See CovidSymptomNlpResultsTask for an example.
    #
    # This will tell the code that creates batches to not break up your group, so that the formatter gets a whole
    # group at one time, and not split across batches.
    group_field: str | None = None

    # *** uniqueness_fields ***
    # The set of fields which together, determine a unique row. There should be no duplicates that
    # share the same value for all these fields. Default is ["id"]
    uniqueness_fields: set[str] | None = None

    # *** update_existing ***
    # Whether to update existing rows or (if False) to ignore them and leave them in place.
    update_existing: bool = True

    # *** visible ***
    # Whether this table should be user-visible in the progress output.
    visible: bool = True


class EtlTask:
    """
    A single ETL task definition.

    Subclasses might want to re-implement read_entries() if they have custom processing.
    """

    # Properties:
    name: ClassVar[str] = None  # task & table name
    # incoming resource that this task operates on (will be included in bulk exports etc)
    resource: ClassVar[str] = None
    tags: ClassVar[set[str]] = []
    # whether this task needs bulk MS tool de-id run on its inputs (NLP tasks usually don't)
    needs_bulk_deid: ClassVar[bool] = True

    outputs: ClassVar[list[OutputTable]] = [OutputTable()]

    ##########################################################################################
    #
    # Main interface (create & run a task)
    #
    ##########################################################################################

    def __init__(self, task_config: config.JobConfig, scrubber: deid.Scrubber):
        assert self.name  # noqa: S101
        assert self.resource  # noqa: S101
        self.task_config = task_config
        self.scrubber = scrubber
        # create format placeholders
        self.formatters: list[formats.Format | None] = [None] * len(self.outputs)
        self.summaries: list[config.JobSummary] = [
            config.JobSummary(output.get_name(self)) for output in self.outputs
        ]

    async def run(self) -> list[config.JobSummary]:
        """
        Executes a single task and returns the summary.

        It is expected that each task creates a single output table.
        """
        common.print_header(f"{self.name}:")

        # Set up progress table with a slight left indent
        grid = rich.table.Table.grid(padding=(0, 0, 0, 1), pad_edge=True)
        progress = cli_utils.make_progress_bar()
        text_box = rich.text.Text()
        grid.add_row(progress)
        grid.add_row(text_box)

        with rich.live.Live(grid):
            if not await self.prepare_task():
                return self.summaries

            entries = self.read_entries(progress=progress)

            # At this point we have a giant iterable of de-identified FHIR objects, ready to be written out.
            # We want to batch them up, to allow resuming from interruptions more easily.
            await self._write_tables_in_batches(entries, progress=progress, status=text_box)

            with self._indeterminate_progress(progress, "Finalizing"):
                # Ensure that we touch every output table (to create them and/or to confirm schema).
                # Consider case of Medication for an EHR that only has inline Medications inside
                # MedicationRequest. The Medication table wouldn't get created otherwise.
                # Plus this is a good place to push any schema changes. (The reason it's nice if
                # the table & schema exist is so that downstream SQL can be dumber.)
                self._touch_remaining_tables()

                # If the input data indicates we should delete some IDs, do that here.
                self._delete_requested_ids()

                # Mark this group & resource combo as complete
                self._update_completion_table()

                # All data is written, now do any final cleanup the formatters want
                for formatter in self.formatters:
                    formatter.finalize()

        return self.summaries

    @classmethod
    def make_batch_from_rows(
        cls, resource_type: str | None, rows: list[dict], groups: set[str] | None = None
    ):
        schema = cls.get_schema(resource_type, rows)
        return formats.Batch(rows, groups=groups, schema=schema)

    ##########################################################################################
    #
    # Internal helpers
    #
    ##########################################################################################

    @contextlib.contextmanager
    def _indeterminate_progress(self, progress: rich.progress.Progress, description: str):
        task = progress.add_task(description=description, total=None)
        yield
        progress.update(task, completed=1, total=1)

    async def _write_tables_in_batches(
        self, entries: EntryIterator, *, progress: rich.progress.Progress, status: rich.text.Text
    ) -> None:
        """Writes all entries to each output tables in batches"""

        def update_status():
            status.plain = "\n".join(
                f"{x.success:,} written to {x.label}"
                for i, x in enumerate(self.summaries)
                if self.outputs[i].visible
            )

        batch_index = 0
        format_progress_task = None
        update_status()

        async for batches in batching.batch_iterate_streams(entries, self.task_config.batch_size):
            if format_progress_task is not None:
                # hide old batches, to save screen space
                progress.update(format_progress_task, visible=False)
            format_progress_task = progress.add_task(
                f"Writing batch {batch_index + 1:,}", total=None
            )

            # Batches is a tuple of lists of resources - the tuple almost never matters, but it is there in case the
            # task is generating multiple types of resources. Like MedicationRequest creating Medications as it goes.
            # Each tuple of batches collectively adds up to roughly our target batch size.
            for table_index, rows in enumerate(batches):
                if not rows:
                    continue

                batch_len = len(rows)
                summary = self.summaries[table_index]
                summary.attempt += batch_len
                if self._write_one_table_batch(rows, table_index, batch_index):
                    summary.success += batch_len
                    update_status()
                else:
                    summary.had_errors = True

                self.table_batch_cleanup(table_index, batch_index)

            progress.update(format_progress_task, completed=1, total=1)
            batch_index += 1

    def _touch_remaining_tables(self):
        """Writes empty dataframe to any table we haven't written to yet"""
        for table_index, formatter in enumerate(self.formatters):
            if formatter is None:  # No data got written yet
                # just write an empty dataframe (should be fast)
                self._write_one_table_batch([], table_index, 0)

    def _delete_requested_ids(self):
        """
        Deletes IDs that have been marked for deletion.

        Formatters are expected to already exist when this is called.

        This usually happens via the `deleted` array from a bulk export.
        Which clients usually drop in a deleted/ folder in the download directory.
        But in our case, that's abstracted away into a JobConfig.deleted_ids dictionary.
        """
        for index, output in enumerate(self.outputs):
            resource = output.get_resource_type(self)
            if not resource or resource.lower() != output.get_name(self):
                # Only delete from the main table for the resource
                continue

            deleted_ids = self.task_config.deleted_ids.get(resource, set())
            if not deleted_ids:
                continue

            deleted_ids = {
                self.scrubber.codebook.fake_id(resource, x, caching_allowed=False)
                for x in deleted_ids
            }
            self.formatters[index].delete_records(deleted_ids)

    def _update_completion_table(self) -> None:
        # Create completion rows
        batch = formats.Batch(
            rows=[
                {
                    "table_name": output.get_name(self),
                    "group_name": self.task_config.export_group_name,
                    "export_time": self.task_config.export_datetime.isoformat(),
                    "export_url": self.task_config.export_url,
                    "etl_version": cumulus_etl.__version__,
                    "etl_time": self.task_config.timestamp.isoformat(),
                }
                for output in self.outputs
                if not output.get_name(self).startswith("etl__")
            ],
            schema=completion.completion_schema(),
        )

        # Write it out
        formatter = self.task_config.create_formatter(**completion.completion_format_args())
        if not formatter.write_records(batch):
            # Completion crosses output table boundaries, so just mark the first one as failed
            self.summaries[0].had_errors = True
        formatter.finalize()

    def _get_formatter(self, table_index: int) -> formats.Format:
        """
        Lazily create output table formatters.

        We do this not because it's a heavy operation (it shouldn't be), but because it lets us know if we can skip
        touching a table that didn't have any input data.
        """
        if self.formatters[table_index] is None:
            table_info = self.outputs[table_index]

            self.formatters[table_index] = self.task_config.create_formatter(
                table_info.get_name(self),
                group_field=table_info.group_field,
                uniqueness_fields=table_info.uniqueness_fields,
                update_existing=table_info.update_existing,
            )

        return self.formatters[table_index]

    @staticmethod
    def _uniquify_rows(rows: list[dict], uniqueness_fields: set[str]) -> list[dict]:
        """
        Drop duplicates inside the batch to guarantee to the formatter that each row is unique.

        This does not fix uniqueness across batches, but formatters that care about that can control for it.

        For context:
        - We have seen duplicates inside and across files generated by Cerner bulk exports. So this is a real
          concern found in the wild, and we can't just trust input data to be "clean."
        - The deltalake backend in particular would prefer the ID to be at least unique inside a batch, so that
          it can more easily handle merge logic. Duplicate IDs across batches will be naturally overwritten as
          new batches are merged in.
        - Other backends like ndjson can currently just live with duplicates across batches, that's fine.
        """
        id_set = set()
        uniqueness_fields = sorted(uniqueness_fields) if uniqueness_fields else ["id"]

        def is_unique(row):
            nonlocal id_set
            row_id = tuple(row[field] for field in uniqueness_fields)
            if row_id in id_set:
                return False
            id_set.add(row_id)
            return True

        # Uniquify in reverse, so that later rows will be preferred.
        # This makes it easy to throw updates of source data alongside the originals,
        # by either appending to an ndjson file or adding new files that sort later.
        rows = [row for row in reversed(rows) if is_unique(row)]
        # But keep original row ordering for cleanliness of ndjson output-format ordering.
        rows.reverse()
        return rows

    def _write_one_table_batch(self, rows: list[dict], table_index: int, batch_index: int) -> bool:
        # Checkpoint scrubber data before writing to the store, because if we get interrupted, it's safer to have an
        # updated codebook with no data than data with an inaccurate codebook.
        self.scrubber.save()

        output = self.outputs[table_index]
        formatter = self._get_formatter(table_index)
        rows = self._uniquify_rows(rows, formatter.uniqueness_fields)
        groups = self.pop_current_group_values(table_index)
        batch = self.make_batch_from_rows(output.get_resource_type(self), rows, groups=groups)

        # Now we write that batch to the target folder, in the requested format (e.g. ndjson).
        success = formatter.write_records(batch)
        if not success:
            # We should write the "bad" batch to the error dir, for later review
            self._write_errors(batch, batch_index)

        return success

    def _write_errors(self, batch: formats.Batch, batch_index: int) -> None:
        """Takes the dataframe and writes it to the error dir, if one was provided"""
        if not self.task_config.dir_errors:
            return

        error_root = store.Root(os.path.join(self.task_config.dir_errors, self.name), create=True)
        error_path = error_root.joinpath(f"write-error.{batch_index:03}.ndjson")
        common.write_rows_to_ndjson(error_path, batch.rows)

    ##########################################################################################
    #
    # Helpers for subclasses
    #
    ##########################################################################################

    def read_ndjson(
        self, *, progress: rich.progress.Progress | None = None, resources: list[str] | None = None
    ) -> Iterator[dict]:
        """
        Grabs all ndjson files from a folder, of particular resource types.

        If `resources` is provided, those resources will be read (in the provided order).
        That is, ["Condition", "Encounter"] will first read all Conditions, then all Encounters.
        If `resources` is not provided, the task's main resource (self.resource) will be used.
        """
        input_root = store.Root(self.task_config.dir_input)
        resources = resources or [self.resource]

        if progress:
            # Make new task to track processing of rows
            row_task = progress.add_task("Reading", total=None)

            # Find total number of lines
            filenames = common.ls_resources(input_root, set(resources))
            total = sum(common.read_local_line_count(filename) for filename in filenames)
            progress.update(row_task, total=total, visible=bool(total))

        # Actually read the lines.
        # We read in the resources in the provided order, because it can matter to the caller.
        # You may want to process all linked resources first, and only then the "real" resource
        # (like we do for Medications and MedicationRequests).
        for resource in resources:
            for line in common.read_resource_ndjson(input_root, resource):
                yield line
                if progress:
                    progress.advance(row_task)

    async def read_entries(self, *, progress: rich.progress.Progress = None) -> EntryIterator:
        """
        Reads input entries for the job.

        Defaults to reading ndjson files for our resource types and de-identifying/scrubbing them.

        If you override this in a subclass, you can optionally include a sub-list of data,
        the elements of which will be guaranteed to all be in the same output batch.
        See comments for EtlTask.group_field for why you might do this.

        You can also return multiple/separate streams of entries by yielding a tuple of values.
        Something like "yield x, y" or "yield x, [y, z]" - these streams of entries will be kept
        separated into two different DataFrames.
        """
        for x in filter(self.scrubber.scrub_resource, self.read_ndjson(progress=progress)):
            yield x

    def table_batch_cleanup(self, table_index: int, batch_index: int) -> None:
        """Override to add any necessary cleanup from writing a batch out (releasing memory etc)"""
        del table_index
        del batch_index

    @classmethod
    async def init_check(cls) -> None:
        """
        Called during ETL initialization to see if all your external dependencies are available.

        If your subclass needs certain services available or the ability to connect to a FHIR server, this is a good
        place to check that. Raise a fatal exception if not
        """
        # Sample method:
        # if not cli_utils.is_url_available("my-url"):
        #   errors.fatal("Oh no!", errors.SERVICE_MISSING)

    async def prepare_task(self) -> bool:
        """
        If your subclass needs to do any preparation at the beginning of run(), override this.

        :returns: False if this task should be skipped and end immediately
        """
        return True

    def pop_current_group_values(self, table_index: int) -> set[str]:
        """
        If your subclass uses the `group_field` output parameter, this returns all group values for the current batch.

        Consider the case where you generate NLP results from docrefs and your group_field is `docref_id`.
        You processed note A with five results yesterday.
        If you re-process note A today, but it now generates no (or less) results, there may be entries in the
        database that the formatter should delete.
        This set of group values will indicate which groups this current batch represents.
        Any group members not overwritten by this batch can be deleted.

        (Your subclass should clear its internal tracking as part of this call, for the next batch.)
        """
        del table_index
        return set()

    @classmethod
    def get_schema(cls, resource_type: str | None, rows: list[dict]) -> pyarrow.Schema | None:
        """
        Creates a properly-schema'd Table from the provided batch.

        Can be overridden as needed for non-FHIR outputs.
        """
        if resource_type:
            return cumulus_fhir_support.pyarrow_schema_from_rows(resource_type, rows)
        return None
