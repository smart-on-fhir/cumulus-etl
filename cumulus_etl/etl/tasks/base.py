"""ETL tasks"""

import dataclasses
import os
from collections.abc import AsyncIterator, Iterator

import pyarrow

from cumulus_etl import common, deid, fhir, formats, store
from cumulus_etl.etl import config
from cumulus_etl.etl.tasks import batching

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

    # *** schema ***
    # This field determines the schema of the output table.
    # Put a FHIR resource name (like "Observation") here, to fill the output table with an appropriate schema.
    # - None disables using a schema
    # - "__same__" means to use the same resource name as our input
    # - Or use any FHIR resource name
    schema: str | None = "__same__"

    def get_schema(self, task):
        return task.resource if self.schema == "__same__" else self.schema

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


class EtlTask:
    """
    A single ETL task definition.

    Subclasses might want to re-implement read_entries() if they have custom processing.
    """

    # Properties:
    name: str = None  # task & table name
    resource: str = None  # incoming resource that this task operates on (will be included in bulk exports etc)
    tags: set[str] = []

    outputs: list[OutputTable] = [OutputTable()]

    ##########################################################################################
    #
    # Main interface (create & run a task)
    #
    ##########################################################################################

    def __init__(self, task_config: config.JobConfig, scrubber: deid.Scrubber):
        assert self.name  # nosec
        assert self.resource  # nosec
        self.task_config = task_config
        self.scrubber = scrubber
        self.formatters: list[formats.Format | None] = [None] * len(self.outputs)  # create format placeholders
        self.summaries: list[config.JobSummary] = [config.JobSummary(output.get_name(self)) for output in self.outputs]

    async def run(self) -> list[config.JobSummary]:
        """
        Executes a single task and returns the summary.

        It is expected that each task creates a single output table.
        """
        common.print_header(f"{self.name}:")

        if not await self.prepare_task():
            return self.summaries

        entries = self.read_entries()

        # At this point we have a giant iterable of de-identified FHIR objects, ready to be written out.
        # We want to batch them up, to allow resuming from interruptions more easily.
        await self._write_tables_in_batches(entries)

        # Ensure that we touch every output table (to create them and/or to confirm schema).
        # Consider case of Medication for an EHR that only has inline Medications inside MedicationRequest.
        # The Medication table wouldn't get created otherwise. Plus this is a good place to push any schema changes.
        # (The reason it's nice if the table & schema exist is so that downstream SQL can be dumber.)
        self._touch_remaining_tables()

        # All data is written, now do any final cleanup the formatters want
        for table_index, formatter in enumerate(self.formatters):
            formatter.finalize()
            print(f"  ⭐ done with {formatter.dbname} ({self.summaries[table_index].success:,} processed) ⭐")

        return self.summaries

    @classmethod
    def make_batch_from_rows(cls, formatter: formats.Format, rows: list[dict], index: int = 0):
        schema = cls.get_schema(formatter, rows)
        return formats.Batch(rows, schema=schema, index=index)

    ##########################################################################################
    #
    # Internal helpers
    #
    ##########################################################################################

    async def _write_tables_in_batches(self, entries: EntryIterator) -> None:
        """Writes all entries to each output tables in batches"""
        batch_index = 0
        async for batches in batching.batch_iterate(entries, self.task_config.batch_size):
            # Batches is a tuple of lists of resources - the tuple almost never matters, but it is there in case the
            # task is generating multiple types of resources. Like MedicationRequest creating Medications as it goes.
            # Each tuple of batches collectively adds up to roughly our target batch size.
            for table_index, rows in enumerate(batches):
                if not rows:
                    continue

                formatter = self._get_formatter(table_index)
                batch_len = len(rows)

                summary = self.summaries[table_index]
                summary.attempt += batch_len
                if self._write_one_table_batch(formatter, rows, batch_index):
                    summary.success += batch_len

                self.table_batch_cleanup(table_index, batch_index)

                print(f"  {summary.success:,} processed for {formatter.dbname}")

            batch_index += 1

    def _touch_remaining_tables(self):
        """Writes empty dataframe to any table we haven't written to yet"""
        for table_index, formatter in enumerate(self.formatters):
            if formatter is None:  # No data got written yet
                formatter = self._get_formatter(table_index)
                self._write_one_table_batch(formatter, [], 0)  # just write an empty dataframe (should be fast)

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
                resource_type=table_info.get_schema(self),
            )

        return self.formatters[table_index]

    def _uniquify_rows(self, rows: list[dict]) -> list[dict]:
        """
        Drop duplicates inside the batch to guarantee to the formatter that the "id" column is unique.

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

        def is_unique(row):
            nonlocal id_set
            if row["id"] in id_set:
                return False
            id_set.add(row["id"])
            return True

        return [row for row in rows if is_unique(row)]

    def _write_one_table_batch(self, formatter: formats.Format, rows: list[dict], batch_index: int) -> bool:
        # Checkpoint scrubber data before writing to the store, because if we get interrupted, it's safer to have an
        # updated codebook with no data than data with an inaccurate codebook.
        self.scrubber.save()

        rows = self._uniquify_rows(rows)
        batch = self.make_batch_from_rows(formatter, rows, index=batch_index)

        # Now we write that batch to the target folder, in the requested format (e.g. parquet).
        success = formatter.write_records(batch)
        if not success:
            # We should write the "bad" batch to the error dir, for later review
            self._write_errors(batch)

        return success

    def _write_errors(self, batch: formats.Batch) -> None:
        """Takes the dataframe and writes it to the error dir, if one was provided"""
        if not self.task_config.dir_errors:
            return

        error_root = store.Root(os.path.join(self.task_config.dir_errors, self.name), create=True)
        error_path = error_root.joinpath(f"write-error.{batch.index:03}.ndjson")
        common.write_rows_to_ndjson(error_path, batch.rows)

    ##########################################################################################
    #
    # Helpers for subclasses
    #
    ##########################################################################################

    def read_ndjson(self) -> Iterator[dict]:
        """
        Grabs all ndjson files from a folder, of a particular resource type.

        Supports filenames like Condition.ndjson, Condition.000.ndjson, or 1.Condition.ndjson.
        """
        input_root = store.Root(self.task_config.dir_input)
        return common.read_resource_ndjson(input_root, self.resource)

    async def read_entries(self) -> EntryIterator:
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
        for x in filter(self.scrubber.scrub_resource, self.read_ndjson()):
            yield x

    def table_batch_cleanup(self, table_index: int, batch_index: int) -> None:
        """Override to add any necessary cleanup from writing a batch out (releasing memory etc)"""
        del table_index
        del batch_index

    async def prepare_task(self) -> bool:
        """
        If your subclass needs to do any preparation at the beginning of run(), override this.

        :returns: False if this task should be skipped and end immediately
        """
        return True

    @classmethod
    def get_schema(cls, formatter: formats.Format, rows: list[dict]) -> pyarrow.Schema | None:
        """
        Creates a properly-schema'd Table from the provided batch.

        Can be overridden as needed for non-FHIR outputs.
        """
        if formatter.resource_type:
            return fhir.pyarrow_schema_from_resource_batch(formatter.resource_type, rows)
        return None