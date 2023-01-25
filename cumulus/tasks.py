"""ETL tasks"""

import itertools
import json
import logging
import os
import re
import sys
from typing import Iterable, Iterator, List, Set, Type, TypeVar, Union

import pandas

from cumulus import common, config, ctakes, deid, errors

T = TypeVar("T")
AnyTask = TypeVar("AnyTask", bound="EtlTask")


def _batch_slice(iterable: Iterable[Union[List[T], T]], n: int) -> Iterator[T]:
    """
    Returns the first n elements of iterable, flattening lists, but including an entire list if we would end in middle.

    For example, list(_batch_slice([1, [2.1, 2.1], 3], 2)) returns [1, 2.1, 2.2]

    Note that this will only flatten elements that are actual Python lists (isinstance list is True)
    """
    count = 0
    for item in iterable:
        if isinstance(item, list):
            yield from item
            count += len(item)
        else:
            yield item
            count += 1

        if count >= n:
            return


def _batch_iterate(iterable: Iterable[Union[List[T], T]], size: int) -> Iterator[Iterator[T]]:
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

    true_iterable = iter(iterable)  # in case it's actually a list (we want to iterate only once through)
    while True:
        iter_slice = _batch_slice(true_iterable, size)
        try:
            peek = next(iter_slice)
        except StopIteration:
            return  # we're done!
        yield itertools.chain([peek], iter_slice)


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
        assert self.name  # nosec: B101
        assert self.resource  # nosec: B101
        self.task_config = task_config
        self.scrubber = scrubber

    def run(self) -> config.JobSummary:
        """
        Executes a single task and returns the summary.

        It is expected that each task creates a single output table.
        """
        summary = config.JobSummary(self.name)
        common.print_header(f"{self.name}:")

        # No data is read or written yet, so do any initial setup the formatter wants
        self.task_config.format.initialize(summary, self.name)

        entries = self.read_entries()

        # At this point we have a giant iterable of de-identified FHIR objects, ready to be written out.
        # We want to batch them up, to allow resuming from interruptions more easily.
        for index, batch in enumerate(_batch_iterate(entries, self.task_config.batch_size)):
            # Stuff de-identified FHIR json into one big pandas DataFrame
            dataframe = pandas.DataFrame(batch)

            # Checkpoint scrubber data before writing to the store, because if we get interrupted, it's safer to have an
            # updated codebook with no data than data with an inaccurate codebook.
            self.scrubber.save()

            # Now we write that DataFrame to the target folder, in the requested format (e.g. parquet).
            self.write_entries(summary, dataframe, index)

        # All data is written, now do any final cleanup the formatter wants
        self.task_config.format.finalize(summary, self.name)

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

        if not filenames:
            logging.error("Could not find any files for %s in the input folder, skipping that resource.", self.resource)
            return

        for filename in filenames:
            with common.open_file(os.path.join(self.task_config.dir_input, filename), "r") as f:
                for line in f:
                    yield json.loads(line)

    def read_entries(self) -> Iterator[Union[List[dict], dict]]:
        """
        Reads input entries for the job.

        Defaults to reading ndjson files for our resource types and de-identifying/scrubbing them.

        If you override this in a subclass, you can optionally include a sub-list of data,
        the elements of which will be guaranteed to all be in the same output batch.
        See comments for EtlTask.group_field for why you might do this.
        """
        return filter(self.scrubber.scrub_resource, self.read_ndjson())

    def write_entries(self, summary: config.JobSummary, dataframe: pandas.DataFrame, index: int) -> None:
        """Writes a single dataframe to the output"""
        self.task_config.format.write_records(summary, dataframe, self.name, index, group_field=self.group_field)


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

    @staticmethod
    def is_ed_coding(coding):
        # Hard code some i2b2 types that we are interested in (all emergency dept codes -- this is not likely very
        # portable, but it's what we have today).
        return coding.get("system") == "http://cumulus.smarthealthit.org/i2b2" and coding.get("code") in [
            "NOTE:149798455",
            "NOTE:318198113",
            "NOTE:318198110",
            "NOTE:3710480",
            "NOTE:189094619",
            "NOTE:159552404",
            "NOTE:318198107",
            "NOTE:189094644",
            "NOTE:3807712",
            "NOTE:189094576",
        ]

    def read_entries(self) -> Iterator[Union[List[dict], dict]]:
        """Passes physician notes through NLP and returns any symptoms found"""
        for docref in self.read_ndjson():
            if not self.scrubber.scrub_resource(docref, scrub_attachments=False):
                continue

            # Check that the note is one of our special allow-listed types (we do this here rather than on the output
            # side to save needing to run everything through NLP).
            type_codings = docref.get("type", {}).get("coding", [])
            is_er_note = list(filter(self.is_ed_coding, type_codings))
            if not is_er_note:
                continue

            # Yield the whole set of symptoms at once, to allow for more easily replacing previous a set of symptoms.
            # This way we don't need to worry about symptoms from the same note crossing batch boundaries.
            # The Format class will replace all existing symptoms from this note at once (because we set group_field).
            yield ctakes.covid_symptoms_extract(self.task_config.dir_phi, docref)
