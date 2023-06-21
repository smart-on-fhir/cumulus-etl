"""ETL tasks"""

import os
import sys
from collections.abc import AsyncIterable, AsyncIterator, Iterable, Iterator
from typing import TypeVar

import pandas

from cumulus_etl import common, deid, errors, store
from cumulus_etl.etl import config

T = TypeVar("T")
AnyTask = TypeVar("AnyTask", bound="EtlTask")


async def _batch_slice(iterable: AsyncIterable[T | list[T]], n: int) -> AsyncIterator[T]:
    """
    Returns the first n elements of iterable, flattening lists, but including an entire list if we would end in middle.

    For example, list(_batch_slice([1, [2.1, 2.1], 3], 2)) returns [1, 2.1, 2.2]

    Note that this will only flatten elements that are actual Python lists (isinstance list is True)
    """
    count = 0
    async for item in iterable:
        if isinstance(item, list):
            for x in item:
                yield x
            count += len(item)
        else:
            yield item
            count += 1

        if count >= n:
            return


async def _async_chain(first: T, rest: AsyncIterator[T]) -> AsyncIterator[T]:
    """An asynchronous version of itertools.chain([first], rest)"""
    yield first
    async for x in rest:
        yield x


async def _batch_iterate(iterable: AsyncIterable[T | list[T]], size: int) -> AsyncIterator[AsyncIterator[T]]:
    """
    Yields sub-iterators, each roughly {size} elements from iterable.

    Sub-iterators might be less, if we have reached the end.
    Sub-iterators might be more, if a list is encountered in the source iterable.
    In that case, all elements of the list are included in the same sub-iterator batch, which might put us over size.
    See the comments for the EtlTask.group_field class attribute for why we support this.

    The whole iterable is never fully loaded into memory. Rather we load only one element at a time.

    Example:
        for batch in _batch_iterate([1, [2.1, 2.2], 3, 4, 5], 2):
            print(list(batch))

    Results in:
        [1, 2.1, 2.2]
        [3, 4]
        [5]
    """
    if size < 1:
        raise ValueError("Must iterate by at least a batch of 1")

    true_iterable = aiter(iterable)  # get a real once-through iterable (we want to iterate only once)
    while True:
        iter_slice = _batch_slice(true_iterable, size)
        try:
            peek = await anext(iter_slice)
        except StopAsyncIteration:
            return  # we're done!
        yield _async_chain(peek, iter_slice)


class EtlTask:
    """
    A single ETL task definition.

    Subclasses might want to re-implement read_entries() if they have custom processing.
    """

    name: str = None  # task & table name
    resource: str = None  # incoming resource that this task operates on (will be included in bulk exports etc)
    tags: set[str] = []

    # *** output_resource ***
    # This field determines the schema of the output table.
    # Put a FHIR resource name (like "Observation") here, to fill the output table with an appropriate schema.
    # It defaults to the same name as `resource` above, but you can set to None to disable this feature or use any
    # FHIR resource name.
    output_resource: str = "__same__"

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
    group_field = None

    ##########################################################################################
    #
    # Task factory methods
    #
    ##########################################################################################

    @classmethod
    def get_all_tasks(cls) -> list[type[AnyTask]]:
        """
        Returns classes for every registered task.

        :returns: a list of all EtlTask subclasses, to instantiate and run
        """
        # Nested imports to avoid circular dependencies
        # pylint: disable=import-outside-toplevel
        from cumulus_etl.etl.studies import covid_symptom

        # Right now, just hard-code these. One day we might allow plugins or something similarly dynamic.
        return [
            ConditionTask,
            DocumentReferenceTask,
            EncounterTask,
            MedicationRequestTask,
            ObservationTask,
            PatientTask,
            ProcedureTask,
            ServiceRequestTask,
            covid_symptom.CovidSymptomNlpResultsTask,
        ]

    @classmethod
    def get_selected_tasks(cls, names: Iterable[str] = None, filter_tags: Iterable[str] = None) -> list[type[AnyTask]]:
        """
        Returns classes for every selected task.

        :param names: an exact list of which tasks to select
        :param filter_tags: only tasks that have all the listed tags will be eligible for selection
        :returns: a list of selected EtlTask subclasses, to instantiate and run
        """
        all_tasks = cls.get_all_tasks()

        # Filter out any tasks that don't have every required tag
        filter_tag_set = frozenset(filter_tags or [])
        filtered_tasks = filter(lambda x: filter_tag_set.issubset(x.tags), all_tasks)

        # If the user didn't list any names, great! We're done.
        if names is None:
            selected_tasks = list(filtered_tasks)
            if not selected_tasks:
                print_filter_tags = ", ".join(sorted(filter_tag_set))
                print(f"No tasks left after filtering for '{print_filter_tags}'.", file=sys.stderr)
                raise SystemExit(errors.TASK_SET_EMPTY)
            return selected_tasks

        # They did list names, so now we validate those names and select those tasks.
        all_task_names = {t.name for t in all_tasks}
        filtered_task_mapping = {t.name: t for t in filtered_tasks}
        selected_tasks = []

        for name in names:
            if name not in all_task_names:
                print_names = "\n".join(sorted(f"  {key}" for key in all_task_names))
                print(f"Unknown task '{name}' requested. Valid task names:\n{print_names}", file=sys.stderr)
                raise SystemExit(errors.TASK_UNKNOWN)

            if name not in filtered_task_mapping:
                print_filter_tags = ", ".join(sorted(filter_tag_set))
                print(
                    f"Task '{name}' requested but it does not match the task filter '{print_filter_tags}'.",
                    file=sys.stderr,
                )
                raise SystemExit(errors.TASK_FILTERED_OUT)

            selected_tasks.append(filtered_task_mapping[name])

        return selected_tasks

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

    async def run(self) -> config.JobSummary:
        """
        Executes a single task and returns the summary.

        It is expected that each task creates a single output table.
        """
        summary = config.JobSummary(self.name)
        common.print_header(f"{self.name}:")

        if not await self.prepare_task():
            return summary

        # No data is read or written yet, so do any initial setup the formatter wants
        format_resource_type = self.resource if self.output_resource == "__same__" else self.output_resource
        formatter = self.task_config.create_formatter(
            self.name,
            group_field=self.group_field,
            resource_type=format_resource_type,
        )

        entries = self.read_entries()

        # At this point we have a giant iterable of de-identified FHIR objects, ready to be written out.
        # We want to batch them up, to allow resuming from interruptions more easily.
        index = 0
        async for batch in _batch_iterate(entries, self.task_config.batch_size):
            # Stuff de-identified FHIR json into one big pandas DataFrame
            dataframe = pandas.DataFrame([row async for row in batch])

            # Drop duplicates inside the batch to guarantee to the formatter that the "id" column is unique.
            # This does not fix uniqueness across batches, but formatters that care about that can control for it.
            # For context:
            #  - We have seen duplicates inside and across files generated by Cerner bulk exports. So this is a real
            #    concern found in the wild, and we can't just trust input data to be "clean."
            #  - The deltalake backend in particular would prefer the ID to be at least unique inside a batch, so that
            #    it can more easily handle merge logic. Duplicate IDs across batches will be naturally overwritten as
            #    new batches are merged in.
            #  - Other backends like ndjson can currently just live with duplicates across batches, that's fine.
            dataframe.drop_duplicates("id", inplace=True)

            # Checkpoint scrubber data before writing to the store, because if we get interrupted, it's safer to have an
            # updated codebook with no data than data with an inaccurate codebook.
            self.scrubber.save()

            # Now we write that DataFrame to the target folder, in the requested format (e.g. parquet).
            df_count = len(dataframe)
            summary.attempt += df_count
            if formatter.write_records(dataframe, index):
                summary.success += df_count
            else:
                # We should write the "bad" dataframe to the error dir, for later review
                self._write_errors(dataframe, index)

            print(f"  {summary.success:,} processed for {self.name}")
            index += 1

        # All data is written, now do any final cleanup the formatter wants
        formatter.finalize()
        print(f"  ⭐ done with {self.name} ({summary.success:,} processed) ⭐")

        return summary

    ##########################################################################################
    #
    # Internal helpers
    #
    ##########################################################################################

    def _write_errors(self, df: pandas.DataFrame, index: int) -> None:
        """Takes the dataframe and writes it to the error dir, if one was provided"""
        if not self.task_config.dir_errors:
            return

        error_root = store.Root(os.path.join(self.task_config.dir_errors, self.name), create=True)
        error_path = error_root.joinpath(f"write-error.{index:03}.ndjson")
        df.to_json(error_path, orient="records", lines=True, storage_options=error_root.fsspec_options())

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

    async def read_entries(self) -> AsyncIterator[dict | list[dict]]:
        """
        Reads input entries for the job.

        Defaults to reading ndjson files for our resource types and de-identifying/scrubbing them.

        If you override this in a subclass, you can optionally include a sub-list of data,
        the elements of which will be guaranteed to all be in the same output batch.
        See comments for EtlTask.group_field for why you might do this.
        """
        for x in filter(self.scrubber.scrub_resource, self.read_ndjson()):
            yield x

    async def prepare_task(self) -> bool:
        """
        If your subclass needs to do any preparation at the beginning of run(), override this.

        :returns: False if this task should be skipped and end immediately
        """
        return True


##########################################################################################
#
# Builtin Tasks
#
##########################################################################################


class ConditionTask(EtlTask):
    name = "condition"
    resource = "Condition"
    tags = {"cpu"}


class DocumentReferenceTask(EtlTask):
    name = "documentreference"
    resource = "DocumentReference"
    tags = {"cpu"}


class EncounterTask(EtlTask):
    name = "encounter"
    resource = "Encounter"
    tags = {"cpu"}


class MedicationRequestTask(EtlTask):
    name = "medicationrequest"
    resource = "MedicationRequest"
    tags = {"cpu"}


class ObservationTask(EtlTask):
    name = "observation"
    resource = "Observation"
    tags = {"cpu"}


class PatientTask(EtlTask):
    name = "patient"
    resource = "Patient"
    tags = {"cpu"}


class ProcedureTask(EtlTask):
    name = "procedure"
    resource = "Procedure"
    tags = {"cpu"}


class ServiceRequestTask(EtlTask):
    name = "servicerequest"
    resource = "ServiceRequest"
    tags = {"cpu"}
