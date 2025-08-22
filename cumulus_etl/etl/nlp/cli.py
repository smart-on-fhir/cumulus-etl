"""
Similar to a normal ETL task, but with an extra NLP focus.

Some differences:
- Runs only the NLP targeted tasks
- No completion tracking
- No bulk de-identification (i.e. no MS tool)
- Has NLP specific arguments
"""

import argparse
import re
import string

import cumulus_fhir_support as cfs
import pyathena

from cumulus_etl import cli_utils, deid, errors, fhir, id_handling, loaders
from cumulus_etl.etl import config, pipeline


def define_nlp_parser(parser: argparse.ArgumentParser) -> None:
    """Fills out an argument parser with all the ETL options."""
    parser.usage = "%(prog)s [OPTION]... INPUT OUTPUT PHI"

    pipeline.add_common_etl_args(parser)
    cli_utils.add_ctakes_override(parser)
    cli_utils.add_task_selection(parser, etl_mode=False)

    cli_utils.add_aws(parser, athena=True)

    group = parser.add_argument_group("cohort selection")
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
        "--select-by-word",
        metavar="WORD",
        action="append",
        help="only select notes that match the given word (can specify multiple times)",
    )
    group.add_argument(
        "--select-by-regex",
        metavar="REGEX",
        action="append",
        help="only select notes that match the given word regex (can specify multiple times)",
    )
    group.add_argument(
        "--select-by-athena-table",
        metavar="DB.TABLE",
        help="name of an Athena table with patient and/or note IDs",
    )
    group.add_argument(
        "--allow-large-selection",
        action="store_true",
        help="allow a larger-than-normal selection",
    )


def query_athena_table(args) -> str:
    if "." in args.select_by_athena_table:
        parts = args.select_by_athena_table.split(".", 1)
        database = parts[0]
        table = parts[-1]
    else:
        database = args.athena_database
        table = args.select_by_athena_table
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
    count = cursor.execute(f'SELECT count(*) FROM "{table}"').fetchone()[0]  # noqa: S608
    if int(count) > 20_000 and not args.allow_large_selection:
        errors.fatal(
            f"Athena cohort in '{table}' is very large ({int(count):,} rows).\n"
            "If you want to use it anyway, pass --allow-large-selection",
            errors.ATHENA_TABLE_TOO_BIG,
        )
    return cursor.execute(f'SELECT * FROM "{table}"').output_location  # noqa: S608


def define_csv_filter(csv_file: str, is_anon: bool) -> config.FilterFunc:
    dxreport_ids = id_handling.get_ids_from_csv(csv_file, "DiagnosticReport", is_anon=is_anon)
    docref_ids = id_handling.get_ids_from_csv(csv_file, "DocumentReference", is_anon=is_anon)
    patient_ids = id_handling.get_ids_from_csv(csv_file, "Patient", is_anon=is_anon)

    if not dxreport_ids and not docref_ids and not patient_ids:
        errors.fatal("No patient or note IDs found in cohort.", errors.COHORT_NOT_FOUND)

    async def res_filter(codebook: deid.Codebook, resource: dict) -> bool:
        match resource["resourceType"]:
            case "DiagnosticReport":
                id_pool = dxreport_ids
                patient_ref = resource.get("subject", {}).get("reference")
            case "DocumentReference":
                id_pool = docref_ids
                patient_ref = resource.get("subject", {}).get("reference")
            case _:  # pragma: no cover
                # shouldn't happen
                return False  # pragma: no cover

        # Check if we have an exact resource ID match (if the user defined exact IDs, we only use
        # them, and don't do any patient matching)
        if id_pool:
            res_id = resource["id"]
            if is_anon:
                res_id = codebook.fake_id(resource["resourceType"], res_id, caching_allowed=False)
            return res_id in id_pool

        # Else match on patients if no resource IDs were defined
        if not patient_ref:
            return False
        patient_id = patient_ref.removeprefix("Patient/")
        if is_anon:
            patient_id = codebook.fake_id("Patient", patient_id, caching_allowed=False)
        return patient_id in patient_ids

    return res_filter


def define_regex_filter(
    client: cfs.FhirClient, words: list[str] | None, regexes: list[str] | None
) -> config.FilterFunc:
    patterns = []
    if regexes:
        patterns.extend(cli_utils.user_regex_to_pattern(regex).pattern for regex in regexes)
    if words:
        patterns.extend(cli_utils.user_term_to_pattern(word).pattern for word in words)

    # combine into one big compiled pattern
    patterns = re.compile("|".join(patterns))

    async def res_filter(codebook: deid.Codebook, resource: dict) -> bool:
        try:
            note_text = await fhir.get_clinical_note(client, resource)
            return patterns.search(note_text) is not None
        except Exception:
            return False

    return res_filter


def get_cohort_filter(client: cfs.FhirClient, args: argparse.Namespace) -> config.FilterFunc:
    """Returns (patient refs to match, resource refs to match)"""
    # Poor man's add_mutually_exclusive_group(), which we don't use because we have additional
    # flags for the group, like "--allow-large-selection".
    has_csv = bool(args.select_by_csv)
    has_anon_csv = bool(args.select_by_anon_csv)
    has_word = bool(args.select_by_word)
    has_regex = bool(args.select_by_regex)
    has_athena_table = bool(args.select_by_athena_table)
    arg_count = (
        int(has_csv) + int(has_anon_csv) + int(has_word or has_regex) + int(has_athena_table)
    )
    if arg_count > 1:
        errors.fatal(
            "Multiple selection arguments provided. Please specify just one.",
            errors.MULTIPLE_COHORT_ARGS,
        )

    if has_athena_table:
        return define_csv_filter(query_athena_table(args), True)
    elif has_anon_csv:
        return define_csv_filter(args.select_by_anon_csv, True)
    elif has_csv:
        return define_csv_filter(args.select_by_csv, False)
    elif has_word or has_regex:
        return define_regex_filter(client, args.select_by_word, args.select_by_regex)
    else:
        return None


async def nlp_main(args: argparse.Namespace) -> None:
    async def prep_scrubber(
        client: cfs.FhirClient, _results: loaders.LoaderResults
    ) -> tuple[deid.Scrubber, dict]:
        res_filter = get_cohort_filter(client, args)
        config_args = {"ctakes_overrides": args.ctakes_overrides, "resource_filter": res_filter}
        return deid.Scrubber(args.dir_phi), config_args

    await pipeline.run_pipeline(args, prep_scrubber=prep_scrubber, nlp=True)


async def run_nlp(parser: argparse.ArgumentParser, argv: list[str]) -> None:
    """Parses an etl CLI"""
    define_nlp_parser(parser)
    args = parser.parse_args(argv)
    await nlp_main(args)
