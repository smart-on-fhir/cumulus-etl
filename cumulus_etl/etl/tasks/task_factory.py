"""Finds and creates ETL tasks"""

import sys
from collections.abc import Iterable
from typing import TypeVar

from cumulus_etl import cli_utils, errors
from cumulus_etl.etl.studies import covid_symptom, hftest
from cumulus_etl.etl.tasks import basic_tasks

AnyTask = TypeVar("AnyTask", bound="EtlTask")  # noqa: F821


def get_all_tasks() -> list[type[AnyTask]]:
    """
    Returns classes for every registered task.

    :returns: a list of all EtlTask subclasses, to instantiate and run
    """
    # Right now, just hard-code these. One day we might allow plugins or something similarly dynamic.
    # Note: tasks will be run in the order listed here.
    return [
        *get_default_tasks(),
        covid_symptom.CovidSymptomNlpResultsGpt35Task,
        covid_symptom.CovidSymptomNlpResultsGpt4Task,
        covid_symptom.CovidSymptomNlpResultsTask,
        covid_symptom.CovidSymptomNlpResultsTermExistsTask,
        hftest.HuggingFaceTestTask,
    ]


def get_default_tasks() -> list[type[AnyTask]]:
    """
    Returns classes for all default tasks (which tend to be the core FHIR resource tasks).

    :returns: a list of all EtlTask subclasses, to instantiate and run
    """
    # Note: tasks will be run in the order listed here.
    return [
        # Run encounter & patient first, to reduce churn on the codebook (we keep cached ID mappings for those two
        # resource and write out those mappings every time a batch has a new encounter/patient - so doing them all
        # upfront reduces the number of times we re-write those mappings later)
        basic_tasks.EncounterTask,
        basic_tasks.PatientTask,
        # The rest of the tasks in alphabetical order, why not:
        basic_tasks.AllergyIntoleranceTask,
        basic_tasks.ConditionTask,
        basic_tasks.DeviceTask,
        basic_tasks.DiagnosticReportTask,
        basic_tasks.DocumentReferenceTask,
        basic_tasks.ImmunizationTask,
        basic_tasks.MedicationRequestTask,
        basic_tasks.ObservationTask,
        basic_tasks.ProcedureTask,
        basic_tasks.ServiceRequestTask,
    ]


def get_selected_tasks(
    names: Iterable[str] | None = None, filter_tags: Iterable[str] | None = None
) -> list[type[AnyTask]]:
    """
    Returns classes for every selected task.

    :param names: an exact list of which tasks to select
    :param filter_tags: only tasks that have all the listed tags will be eligible for selection
    :returns: a list of selected EtlTask subclasses, to instantiate and run
    """
    names = set(cli_utils.expand_comma_list_arg(names, casefold=True))
    filter_tags = list(cli_utils.expand_comma_list_arg(filter_tags, casefold=True))
    filter_tag_set = set(filter_tags)

    if "help" in names:
        # OK, we actually are just going to print the list of all task names and be done.
        _print_task_names()
        raise SystemExit(errors.TASK_HELP)  # not an *error* exactly, but not successful ETL either

    # Just give back the default set if the user didn't specify any constraints
    if not names and not filter_tag_set:
        return get_default_tasks()

    # Filter out any tasks that don't have every required tag
    all_tasks = get_all_tasks()
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
        print(f"Unknown task '{unknown_names.pop()}' requested.", file=sys.stderr)
        _print_task_names(file=sys.stderr)
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


def _print_task_names(*, file=sys.stdout) -> None:
    all_tasks = get_all_tasks()
    all_task_names = {t.name for t in all_tasks}
    print_names = "\n".join(sorted(f"  {key}" for key in all_task_names))
    print(f"Valid task names:\n{print_names}", file=file)
