"""ETL tasks"""

import copy
import itertools
import os
import sys
from typing import AsyncIterable, AsyncIterator, Iterable, Iterator, List, Set, Type, TypeVar, Union

import ctakesclient
import pandas

from cumulus_etl import common, deid, errors, nlp, store
from cumulus_etl.etl import config

T = TypeVar("T")
AnyTask = TypeVar("AnyTask", bound="EtlTask")


async def _batch_slice(iterable: AsyncIterable[Union[List[T], T]], n: int) -> AsyncIterator[T]:
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


async def _batch_iterate(iterable: AsyncIterable[Union[List[T], T]], size: int) -> AsyncIterator[AsyncIterator[T]]:
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

    # aiter() and anext() were added in python 3.10
    # pylint: disable=unnecessary-dunder-call

    true_iterable = iterable.__aiter__()  # get a real once-through iterable (we want to iterate only once)
    while True:
        iter_slice = _batch_slice(true_iterable, size)
        try:
            peek = await iter_slice.__anext__()
        except StopAsyncIteration:
            return  # we're done!
        yield _async_chain(peek, iter_slice)


class EtlTask:
    """
    A single ETL task definition.

    Subclasses might want to re-implement read_entries() if they have custom processing.
    """

    name: str = None
    resource: str = None
    tags: Set[str] = []

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
    def get_all_tasks(cls) -> List[Type[AnyTask]]:
        """
        Returns classes for every registered task.

        :returns: a list of all EtlTask subclasses, to instantiate and run
        """
        # Right now, just hard-code these. One day we might allow plugins or something similarly dynamic.
        return [
            ConditionTask,
            DocumentReferenceTask,
            EncounterTask,
            MedicationRequestTask,
            ObservationTask,
            PatientTask,
            CovidSymptomNlpResultsTask,
        ]

    @classmethod
    def get_selected_tasks(cls, names: Iterable[str] = None, filter_tags: Iterable[str] = None) -> List[Type[AnyTask]]:
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
        formatter = self.task_config.create_formatter(self.name, self.group_field)

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

    async def read_entries(self) -> AsyncIterator[Union[List[dict], dict]]:
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


class CovidSymptomNlpResultsTask(EtlTask):
    """Covid Symptom study task, to generate symptom lists from ED notes using NLP"""

    name = "covid_symptom__nlp_results"
    resource = "DocumentReference"
    tags = {"covid_symptom", "gpu"}
    group_field = "docref_id"

    # List of recognized emergency department note types. We'll add more as we discover them in use.
    ED_CODES = {
        "http://cumulus.smarthealthit.org/i2b2": {
            "NOTE:3710480",
            "NOTE:3807712",
            "NOTE:149798455",
            "NOTE:159552404",
            "NOTE:189094576",
            "NOTE:189094619",
            "NOTE:189094644",
            "NOTE:318198107",
            "NOTE:318198110",
            "NOTE:318198113",
        },
        "http://loinc.org": {
            "18842-5",  # Discharge Summary
            "28568-4",  # Physician Emergency department Note
            "34111-5",  # Emergency department Note
            "34878-9",  # Emergency medicine Note
            "51846-4",  # Emergency department Consult note
            "54094-8",  # Emergency department Triage note
            "57053-1",  # Nurse Emergency department Note
            "57054-9",  # Nurse Emergency department Triage+care note
            "59258-4",  # Emergency department Discharge summary
            "60280-5",  # Emergency department Discharge instructions
            "68552-9",  # Emergency medicine Emergency department Admission evaluation note
            "74187-6",  # InterRAI Emergency Screener for Psychiatry (ESP) Document
            "74211-4",  # Summary of episode note Emergency department+Hospital
        },
    }

    async def prepare_task(self) -> bool:
        bsv_path = ctakesclient.filesystem.covid_symptoms_path()
        success = nlp.restart_ctakes_with_bsv(self.task_config.ctakes_overrides, bsv_path)
        if not success:
            print(f"Skipping {self.name}.")
        return success

    @classmethod
    def is_ed_coding(cls, coding):
        """Returns true if this is a coding for an emergency department note"""
        return coding.get("code") in cls.ED_CODES.get(coding.get("system"), {})

    def add_error(self, docref: dict) -> None:
        if not self.task_config.dir_errors:
            return

        error_root = store.Root(os.path.join(self.task_config.dir_errors, self.name), create=True)
        error_path = error_root.joinpath("nlp-errors.ndjson")
        with common.NdjsonWriter(error_path, "a") as writer:
            writer.write(docref)

    async def read_entries(self) -> AsyncIterator[Union[List[dict], dict]]:
        """Passes physician notes through NLP and returns any symptoms found"""
        phi_root = store.Root(self.task_config.dir_phi, create=True)

        # one client for both NLP services for now -- no parallel requests yet, so no need to be fancy
        http_client = nlp.ctakes_httpx_client()

        for docref in self.read_ndjson():
            # Check that the note is one of our special allow-listed types (we do this here rather than on the output
            # side to save needing to run everything through NLP).
            # We check both type and category for safety -- we aren't sure yet how EHRs are using these fields.
            codings = list(itertools.chain.from_iterable([cat.get("coding", []) for cat in docref.get("category", [])]))
            codings += docref.get("type", {}).get("coding", [])
            is_er_note = any(self.is_ed_coding(x) for x in codings)
            if not is_er_note:
                continue

            orig_docref = copy.deepcopy(docref)
            if not self.scrubber.scrub_resource(docref, scrub_attachments=False):
                continue

            symptoms = await nlp.covid_symptoms_extract(
                self.task_config.client,
                phi_root,
                docref,
                ctakes_http_client=http_client,
                cnlp_http_client=http_client,
            )
            if symptoms is None:
                self.add_error(orig_docref)
                continue

            # Yield the whole set of symptoms at once, to allow for more easily replacing previous a set of symptoms.
            # This way we don't need to worry about symptoms from the same note crossing batch boundaries.
            # The Format class will replace all existing symptoms from this note at once (because we set group_field).
            yield symptoms