"""Finds and creates ETL tasks"""

from collections.abc import Iterable
from typing import TypeVar

import rich

from cumulus_etl import cli_utils, errors
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
        basic_tasks.EpisodeOfCareTask,
        basic_tasks.ImmunizationTask,
        basic_tasks.LocationTask,
        basic_tasks.MedicationTask,
        basic_tasks.MedicationDispenseTask,
        basic_tasks.MedicationRequestTask,
        basic_tasks.ObservationTask,
        basic_tasks.OrganizationTask,
        basic_tasks.PractitionerTask,
        basic_tasks.PractitionerRoleTask,
        basic_tasks.ProcedureTask,
        basic_tasks.ServiceRequestTask,
        basic_tasks.SpecimenTask,
    ]


def get_selected_tasks(
    names: Iterable[str] | None = None, *, nlp: bool = False, exclusions: Iterable[str] = []
) -> list[type[AnyTask]]:
    """
    Returns classes for every selected task.

    :param names: an exact list of which tasks to select
    :param nlp: whether we are selecting from NLP or normal tasks
    :param exclusions: a list of resources to explicitly exclude from a task list
    :returns: a list of selected EtlTask subclasses, to instantiate and run
    """
    names = set(cli_utils.expand_comma_list_arg(names, casefold=True))
    exclusions = set(cli_utils.expand_comma_list_arg(exclusions, casefold=True))

    # If we are in NLP mode, we can only select NLP tasks and vice versa.
    all_tasks = get_default_tasks()

    if "help" in names:
        # OK, we actually are just going to print the list of all task names and be done.
        _print_task_names(all_tasks)
        raise SystemExit(errors.TASK_HELP)  # not an *error* exactly, but not successful ETL either

    # What to do if user didn't provide any constraints?
    if not names:
        all_tasks = get_default_tasks()
        names = set([x.name for x in all_tasks])

    # Now we validate those names and select those tasks, along with checking exclusion cases

    # Check for unknown names the user gave us
    all_task_names = {t.name for t in all_tasks}
    if unknown_names := names - all_task_names:
        rich.print(f"Unknown task '{unknown_names.pop()}' requested.")
        _print_task_names(all_tasks)
        raise SystemExit(errors.TASK_UNKNOWN)
    if unknown_names := exclusions - all_task_names:
        rich.print(f"Unknown task '{unknown_names.pop()}' requested for exclusion.")
        _print_task_names(all_tasks)
        raise SystemExit(errors.TASK_UNKNOWN)

    tasks = [task for task in all_tasks if task.name in names]
    if exclusions != {}:
        tasks = [task for task in tasks if task.name not in exclusions]
    return tasks


def _print_task_names(all_tasks: list[type[AnyTask]]) -> None:
    all_task_names = {t.name for t in all_tasks}
    print_names = "\n".join(sorted(f"  {key}" for key in all_task_names))
    rich.print(f"Valid task names:\n{print_names}")
