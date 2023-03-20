"""ETL tasks"""

import itertools
import json
import os
import re
import shutil
import sys
from typing import AsyncIterable, AsyncIterator, Iterable, Iterator, List, Set, Type, TypeVar, Union

import ctakesclient
import pandas

from cumulus import common, deid, errors, nlp, store
from cumulus.etl import config

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

            # Checkpoint scrubber data before writing to the store, because if we get interrupted, it's safer to have an
            # updated codebook with no data than data with an inaccurate codebook.
            self.scrubber.save()

            # Now we write that DataFrame to the target folder, in the requested format (e.g. parquet).
            df_count = len(dataframe)
            summary.attempt += df_count
            if formatter.write_records(dataframe, index):
                summary.success += df_count

            print(f"  {summary.success:,} processed for {self.name}")
            index += 1

        # All data is written, now do any final cleanup the formatter wants
        formatter.finalize()
        print(f"  ⭐ done with {self.name} ({summary.success:,} processed) ⭐")

        return summary

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
        pattern = re.compile(rf"([0-9]+.)?{self.resource}(.[0-9]+)?.ndjson")
        all_files = os.listdir(self.task_config.dir_input)
        filenames = list(filter(pattern.match, all_files))

        for filename in filenames:
            with common.open_file(os.path.join(self.task_config.dir_input, filename), "r") as f:
                for line in f:
                    yield json.loads(line)

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

    def restart_ctakes_with_bsv(self, bsv_path) -> bool:
        """Hands a new bsv over to cTAKES and waits for it to restart and be ready again with the new bsv file"""
        # This whole setup is slightly janky. But it is designed with these constraints:
        # 1. We'd like to feed cTAKES different custom dictionaries, including ones invented by the user.
        # 2. cTAKES has no ability to accept a new dictionary on the fly.
        # 3. cTAKES is not able to hold multiple dictionaries at once.
        # 4. We're usually running under docker.
        #
        # Taken altogether, if we want to feed cTAKES a dictionary, we need to start it fresh with that dictionary.
        # But one docker cannot manage another docker's lifecycle.
        #
        # So what Cumulus does is use a cTAKES docker image that specifically supports placing override dictionaries
        # in a well-known path (/overrides). The docker image will watch for modifications there and restart cTAKES.
        #
        # Then, we place our custom dictionaries in a folder that is mounted as /overrides on the cTAKES side.
        # In our default docker setup, this is /ctakes-overrides on our side.
        # In other setups, you can pass --ctakes-overrides to set the folder.
        #
        # Because writing a new bsv file will cause a cTAKES restart to happen, we have to beware of race conditions.
        # So we'll use the wait_for_ctakes_restart context manager to ensure cTAKES noticed our change and is ready.

        target_dir = self.task_config.ctakes_overrides
        if not target_dir:
            # Graceful skipping of this feature if ctakes-override is empty (usually just in tests).
            print(f"Warning: Skipping {self.name} because --ctakes-override is not defined.", file=sys.stderr)
            return False
        elif not os.path.isdir(target_dir):
            print(
                f"Warning: Skipping {self.name} because the cTAKES overrides\n"
                f"folder does not exist at {target_dir}.\n"
                "Consider using --ctakes-overrides.",
                file=sys.stderr,
            )
            return False

        with nlp.wait_for_ctakes_restart():
            shutil.copyfile(bsv_path, os.path.join(target_dir, "symptoms.bsv"))
        return True

    async def prepare_task(self) -> bool:
        return self.restart_ctakes_with_bsv(ctakesclient.filesystem.covid_symptoms_path())

    @classmethod
    def is_ed_coding(cls, coding):
        """Returns true if this is a coding for an emergency department note"""
        return coding.get("code") in cls.ED_CODES.get(coding.get("system"), {})

    async def read_entries(self) -> AsyncIterator[Union[List[dict], dict]]:
        """Passes physician notes through NLP and returns any symptoms found"""
        phi_root = store.Root(self.task_config.dir_phi, create=True)

        for docref in self.read_ndjson():
            # Check that the note is one of our special allow-listed types (we do this here rather than on the output
            # side to save needing to run everything through NLP).
            # We check both type and category for safety -- we aren't sure yet how EHRs are using these fields.
            codings = list(itertools.chain.from_iterable([cat.get("coding", []) for cat in docref.get("category", [])]))
            codings += docref.get("type", {}).get("coding", [])
            is_er_note = any(self.is_ed_coding(x) for x in codings)
            if not is_er_note:
                continue

            if not self.scrubber.scrub_resource(docref, scrub_attachments=False):
                continue

            # Yield the whole set of symptoms at once, to allow for more easily replacing previous a set of symptoms.
            # This way we don't need to worry about symptoms from the same note crossing batch boundaries.
            # The Format class will replace all existing symptoms from this note at once (because we set group_field).
            yield await nlp.covid_symptoms_extract(self.task_config.client, phi_root, docref)
