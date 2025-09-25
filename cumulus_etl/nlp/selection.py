"""Methods for handling document selection"""

import argparse
import re
import string

import cumulus_fhir_support as cfs
import pyathena

from cumulus_etl import cli_utils, deid, errors, fhir, id_handling


def add_note_selection(parser: argparse.ArgumentParser):
    group = parser.add_argument_group("note selection")
    group.add_argument(
        "--select-by-word",
        metavar="WORD",
        action="append",
        help="only select notes that match the given word (can specify multiple times, "
        "will only match whole words/phrases)",
    )
    group.add_argument(
        "--select-by-regex",
        metavar="REGEX",
        action="append",
        help="only select notes that match the given word regex (can specify multiple times, "
        "will only match whole words/phrases)",
    )
    group.add_argument(
        "--select-by-csv",
        metavar="FILE",
        help="path to a .csv file with original patient and/or note IDs",
    )
    group.add_argument(
        "--select-by-anon-csv",
        metavar="FILE",
        help="path to a .csv file with anonymized patient and/or note IDs",
    )
    group.add_argument(
        "--select-by-athena-table",
        metavar="DB.TABLE",
        help="name of an Athena table with patient and/or note IDs",
    )
    group.add_argument(
        "--allow-large-selection",
        action="store_true",
        help="skip selection size validation",
    )
    return group


def query_athena_table(table: str, args) -> str:
    if "." in table:
        parts = table.split(".", 1)
        database = parts[0]
        table = parts[-1]
    else:
        database = args.athena_database
    if not database:
        errors.fatal(
            "You must provide an Athena database with --athena-database.",
            errors.ATHENA_DATABASE_MISSING,
        )
    if set(table) - set(string.ascii_letters + string.digits + "-_"):
        errors.fatal(
            f"Athena table name '{table}' has invalid characters.",
            errors.ATHENA_TABLE_NAME_INVALID,
        )
    cursor = pyathena.connect(
        region_name=args.athena_region,
        work_group=args.athena_workgroup,
        schema_name=database,
    ).cursor()

    # Make sure the table is not so large that the user is maybe running against a core table
    count = cursor.execute(f'SELECT count(*) FROM "{table}"').fetchone()[0]  # noqa: S608
    if int(count) > 50_000:
        msg = f"Athena cohort in '{table}' is very large ({int(count):,} rows)."
        response = cli_utils.prompt(f"{msg} Continue?", override=args.allow_large_selection)
        if response == cli_utils.PromptResponse.NON_INTERACTIVE:
            errors.fatal(
                f"{msg}\nIf you want to use it anyway, pass --allow-large-selection",
                errors.ATHENA_TABLE_TOO_BIG,
            )

    return cursor.execute(f'SELECT * FROM "{table}"').output_location  # noqa: S608


class CsvMatcher:
    def __init__(self, csv_file: str, *, is_anon: bool, extra_fields: list[str] | None = None):
        self._id_pools = {
            res_type: id_handling.get_ids_from_csv(
                csv_file, res_type, is_anon=is_anon, extra_fields=extra_fields
            )
            for res_type in {"DiagnosticReport", "DocumentReference"}
        }
        self._is_anon = is_anon

        # We operate in two modes: note ID or patient ID.
        # We only match patients if they are the only kind of ID defined.
        self._only_patients = not any(self._id_pools.values())
        self._id_pools["Patient"] = id_handling.get_ids_from_csv(
            csv_file, "Patient", is_anon=is_anon, extra_fields=extra_fields
        )

        if not any(self._id_pools.values()):
            errors.fatal(
                f"No patient or note IDs found in CSV file '{csv_file}'.", errors.COHORT_NOT_FOUND
            )

    def has_resource_match(self, codebook: deid.Codebook, resource: dict) -> bool:
        match resource["resourceType"]:
            case "DiagnosticReport" | "DocumentReference":
                patient_ref = resource.get("subject", {}).get("reference")
            case _:  # pragma: no cover
                # shouldn't happen
                return False  # pragma: no cover

        patient_id = patient_ref and patient_ref.removeprefix("Patient/")

        return self.has_match(codebook, resource["resourceType"], resource["id"], patient_id)

    def has_match(
        self, codebook: deid.Codebook, res_type: str, res_id: str, patient_id: str
    ) -> bool:
        return self.get_match(codebook, res_type, res_id, patient_id) is not None

    def get_match(
        self, codebook: deid.Codebook, res_type: str, res_id: str, patient_id: str
    ) -> set[tuple] | None:
        if self._only_patients:
            res_type = "Patient"
            res_id = patient_id

        if self._is_anon:
            res_id = codebook.fake_id(res_type, res_id, caching_allowed=False)

        return self._id_pools[res_type].get(res_id)


def _define_csv_filter(csv_file: str, is_anon: bool) -> deid.FilterFunc:
    matcher = CsvMatcher(csv_file, is_anon=is_anon)

    async def check_match(codebook, res):
        return matcher.has_resource_match(codebook, res)

    return check_match


def _define_regex_filter(
    client: cfs.FhirClient, words: list[str] | None, regexes: list[str] | None
) -> deid.FilterFunc:
    patterns = []
    if regexes:
        patterns.extend(cli_utils.user_regex_to_pattern(regex).pattern for regex in regexes)
    if words:
        patterns.extend(cli_utils.user_term_to_pattern(word).pattern for word in words)

    # combine into one big compiled pattern
    patterns = re.compile("|".join(patterns), re.IGNORECASE)

    async def res_filter(codebook: deid.Codebook, resource: dict) -> bool:
        try:
            note_text = await fhir.get_clinical_note(client, resource)
            return patterns.search(note_text) is not None
        except Exception:
            return False

    return res_filter


def _combine_filters(one: deid.FilterFunc, two: deid.FilterFunc) -> deid.FilterFunc:
    async def combined(codebook: deid.Codebook, resource: dict) -> bool:
        if one and not await one(codebook, resource):
            return False
        if two and not await two(codebook, resource):
            return False
        return True

    return combined


def get_note_filter(client: cfs.FhirClient, args: argparse.Namespace) -> deid.FilterFunc:
    """Returns (patient refs to match, resource refs to match)"""
    # Confirm we don't have conflicting arguments. Which we could maybe combine, as a future
    # improvement, but is too much hassle right now)
    has_csv = bool(args.select_by_csv)
    has_anon_csv = bool(args.select_by_anon_csv)
    has_word = bool(args.select_by_word)
    has_regex = bool(args.select_by_regex)
    has_athena_table = bool(args.select_by_athena_table)
    csv_count = int(has_csv) + int(has_anon_csv) + int(has_athena_table)
    if csv_count > 1:
        errors.fatal(
            "Multiple CSV selection arguments provided. Please specify just one.",
            errors.MULTIPLE_COHORT_ARGS,
        )

    func = None

    # Currently, only support one CSV input (could relax - but should we use OR or AND?)
    if has_athena_table:
        func = _define_csv_filter(query_athena_table(args.select_by_athena_table, args), True)
    elif has_anon_csv:
        func = _define_csv_filter(args.select_by_anon_csv, True)
    elif has_csv:
        func = _define_csv_filter(args.select_by_csv, False)

    # And combine that with any word filter (all word filters are OR'd together, and then AND'd
    # with the csv filter - i.e. you can filter down your CSV file further with regexes)
    if has_word or has_regex:
        regex_func = _define_regex_filter(client, args.select_by_word, args.select_by_regex)
        func = _combine_filters(func, regex_func)

    return func
