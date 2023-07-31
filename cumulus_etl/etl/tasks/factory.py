"""Finds and creates ETL tasks"""

import sys
from collections.abc import Iterable
from typing import TypeVar

from cumulus_etl import errors
from cumulus_etl.etl.studies import covid_symptom
from cumulus_etl.etl.tasks.basic_tasks import (
    ConditionTask,
    DocumentReferenceTask,
    EncounterTask,
    MedicationRequestTask,
    ObservationTask,
    PatientTask,
    ProcedureTask,
    ServiceRequestTask,
)

AnyTask = TypeVar("AnyTask", bound="EtlTask")


def get_all_tasks() -> list[type[AnyTask]]:
    """
    Returns classes for every registered task.

    :returns: a list of all EtlTask subclasses, to instantiate and run
    """
    # Right now, just hard-code these. One day we might allow plugins or something similarly dynamic.
    # Note: tasks will be run in the order listed here.
    return [
        # Run encounter & patient first, to reduce churn on the codebook (the cached mappings would mostly be written
        # out during the encounter task and wouldn't need to be re-written later, one would hope)
        EncounterTask,
        PatientTask,
        # The rest of the tasks in alphabetical order, why not:
        ConditionTask,
        DocumentReferenceTask,
        MedicationRequestTask,
        ObservationTask,
        ProcedureTask,
        ServiceRequestTask,
        covid_symptom.CovidSymptomNlpResultsTask,
    ]


def get_selected_tasks(names: Iterable[str] = None, filter_tags: Iterable[str] = None) -> list[type[AnyTask]]:
    """
    Returns classes for every selected task.

    :param names: an exact list of which tasks to select
    :param filter_tags: only tasks that have all the listed tags will be eligible for selection
    :returns: a list of selected EtlTask subclasses, to instantiate and run
    """
    all_tasks = get_all_tasks()
    names = names and set(names)

    # Filter out any tasks that don't have every required tag
    filter_tag_set = frozenset(filter_tags or [])
    filtered_tasks = list(filter(lambda x: filter_tag_set.issubset(x.tags), all_tasks))

    # If the user didn't list any names, great! We're done.
    if not names:
        if not filtered_tasks:
            print_filter_tags = ", ".join(sorted(filter_tag_set))
            print(f"No tasks left after filtering for '{print_filter_tags}'.", file=sys.stderr)
            raise SystemExit(errors.TASK_SET_EMPTY)
        return filtered_tasks

    # They did list names, so now we validate those names and select those tasks.

    # Check for unknown names the user gave us
    all_task_names = {t.name for t in all_tasks}
    if unknown_names := names - all_task_names:
        print_names = "\n".join(sorted(f"  {key}" for key in all_task_names))
        print(f"Unknown task '{unknown_names.pop()}' requested. Valid task names:\n{print_names}", file=sys.stderr)
        raise SystemExit(errors.TASK_UNKNOWN)

    # Check for names that conflict with the chosen filters
    filtered_task_names = {t.name for t in filtered_tasks}
    if unfiltered_names := names - filtered_task_names:
        print_filter_tags = ", ".join(sorted(filter_tag_set))
        print(
            f"Task '{unfiltered_names.pop()}' requested but it does not match the task filter '{print_filter_tags}'.",
            file=sys.stderr,
        )
        raise SystemExit(errors.TASK_FILTERED_OUT)

    return [task for task in filtered_tasks if task.name in names]
