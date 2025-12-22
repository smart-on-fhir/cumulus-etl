"""Finds and creates ETL tasks"""

import inspect
import sys
from collections.abc import Iterable
from typing import TypeVar

from cumulus_etl import cli_utils, errors
from cumulus_etl.etl.studies import covid_symptom, example, glioma, irae
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
        *get_nlp_tasks(),
    ]


def get_classes_from_module(module) -> list[type[AnyTask]]:
    return [x[1] for x in inspect.getmembers(module, inspect.isclass)]


def get_nlp_tasks() -> list[type[AnyTask]]:
    return [
        *get_classes_from_module(covid_symptom),
        *get_classes_from_module(example),
        *get_classes_from_module(glioma),
        *get_classes_from_module(irae),
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
        basic_tasks.LocationTask,
        basic_tasks.MedicationRequestTask,
        basic_tasks.ObservationTask,
        basic_tasks.OrganizationTask,
        basic_tasks.PractitionerTask,
        basic_tasks.PractitionerRoleTask,
        basic_tasks.ProcedureTask,
        basic_tasks.ServiceRequestTask,
    ]


def get_selected_tasks(
    names: Iterable[str] | None = None,
    *,
    nlp: bool = False,
) -> list[type[AnyTask]]:
    """
    Returns classes for every selected task.

    :param names: an exact list of which tasks to select
    :param nlp: whether we are selecting from NLP or normal tasks
    :returns: a list of selected EtlTask subclasses, to instantiate and run
    """
    names = set(cli_utils.expand_comma_list_arg(names, casefold=True))

    # If we are in NLP mode, we can only select NLP tasks and vice versa.
    all_tasks = get_nlp_tasks() if nlp else get_default_tasks()
    other_tasks = get_default_tasks() if nlp else get_nlp_tasks()

    if "help" in names:
        # OK, we actually are just going to print the list of all task names and be done.
        _print_task_names(all_tasks)
        raise SystemExit(errors.TASK_HELP)  # not an *error* exactly, but not successful ETL either

    # What to do if user didn't provide any constraints?
    if not names:
        return get_default_tasks()

    # They did list names, so now we validate those names and select those tasks.

    # Check for unknown names the user gave us
    all_task_names = {t.name for t in all_tasks}
    other_task_names = {t.name for t in other_tasks}
    if unknown_names := names - all_task_names - other_task_names:
        print(f"Unknown task '{unknown_names.pop()}' requested.", file=sys.stderr)
        _print_task_names(all_tasks, file=sys.stderr)
        raise SystemExit(errors.TASK_UNKNOWN)
    if names - all_task_names:
        errors.fatal(
            "Cannot mix NLP and non-NLP tasks in the same run. "
            "Use 'cumulus-etl nlp' for NLP tasks and 'cumulus-etl etl' for normal FHIR tasks.",
            errors.TASK_MISMATCH,
        )

    tasks = [task for task in all_tasks if task.name in names]

    if nlp:
        models = {getattr(t, "client_class", None) for t in tasks}
        if len(models) > 1:
            errors.fatal("Only one kind of NLP model can be run at once.", errors.TASK_TOO_MANY)

    return tasks


def _print_task_names(all_tasks: list[type[AnyTask]], *, file=sys.stdout) -> None:
    all_task_names = {t.name for t in all_tasks}
    print_names = "\n".join(sorted(f"  {key}" for key in all_task_names))
    print(f"Valid task names:\n{print_names}", file=file)
