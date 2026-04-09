"""Methods for handling document selection"""

import argparse
import string

import cumulus_fhir_support as cfs
import pyathena

from cumulus_etl import cli_utils, common, deid, errors


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
        type=cfs.FsPath,
        help="path to a .csv file with original patient and/or note IDs",
    )
    group.add_argument(
        "--select-by-anon-csv",
        metavar="FILE",
        type=cfs.FsPath,
        help="path to a .csv file with anonymized patient and/or note IDs",
    )
    group.add_argument(
        "--select-by-athena-table",
        metavar="DB.TABLE",
        help="name of an Athena table with patient and/or note IDs",
    )
    group.add_argument(
        "--reject-by-word",
        metavar="WORD",
        action="append",
        help="notes that match the given word will not be selected (can specify multiple times, "
        "will only match whole words/phrases)",
    )
    group.add_argument(
        "--reject-by-regex",
        metavar="REGEX",
        action="append",
        help="notes that match the given word regex will not be selected (can specify multiple "
        "times, will only match whole words/phrases)",
    )
    group.add_argument(
        "--allow-large-selection",
        action="store_true",
        help="skip selection size validation",
    )
    return group


def query_athena_table(table: str, args) -> cfs.FsPath:
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
    if set(table) - set(string.ascii_letters + string.digits + "_"):
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

    return cfs.FsPath(cursor.execute(f'SELECT * FROM "{table}"').output_location)  # noqa: S608


def get_refs_from_csv(
    path: cfs.FsPath, *, is_anon: bool = False, extra_fields: list[str] | None = None
) -> cfs.RefSet:
    """Returns an anonymized RefSet with data of {(value, value, ...)} for any extra fields"""
    extra_fields = extra_fields or []
    result = cfs.RefSet()

    with common.read_csv(path) as reader:
        try:
            scanner = cfs.make_note_ref_scanner(reader.fieldnames, is_anon=is_anon)
        except cfs.RefsNotFound:
            errors.fatal(
                f"No patient or note IDs found in CSV file '{path}'.", errors.COHORT_NOT_FOUND
            )

        for row in reader:
            if ref := scanner(list(row.values())):
                # Add this row's extra field data to any previously existing found refs
                existing_data = result.get_data_for_ref(ref, default=set())
                existing_data.add(tuple(row.get(field) for field in extra_fields))
                result.add_ref(ref, data=existing_data)

    return result


def get_note_filter(args: argparse.Namespace, codebook: deid.Codebook | None) -> cfs.NoteFilter:
    """Returns (patient refs to match, resource refs to match)"""

    # Currently, only support one CSV input (we could relax that - but should we use OR or AND?)
    has_csv = bool(args.select_by_csv)
    has_anon_csv = bool(args.select_by_anon_csv)
    has_athena_table = bool(args.select_by_athena_table)
    csv_count = int(has_csv) + int(has_anon_csv) + int(has_athena_table)
    if csv_count > 1:
        errors.fatal(
            "Multiple CSV selection arguments provided. Please specify just one.",
            errors.MULTIPLE_COHORT_ARGS,
        )

    if (has_athena_table or has_anon_csv) and not codebook:
        errors.fatal("A PHI folder must be provided to process anonymous IDs.", errors.ARGS_INVALID)

    if has_athena_table:
        csv_path = query_athena_table(args.select_by_athena_table, args)
        csv_refs = get_refs_from_csv(csv_path, is_anon=True)
    elif has_anon_csv:
        csv_refs = get_refs_from_csv(args.select_by_anon_csv, is_anon=True)
    elif has_csv:
        codebook = None  # don't need or want salt
        csv_refs = get_refs_from_csv(args.select_by_csv, is_anon=False)
    else:
        csv_refs = None

    return cfs.make_note_filter(
        reject_by_regex=args.reject_by_regex,
        reject_by_word=args.reject_by_word,
        salt=codebook and codebook.get_id_salt(),
        select_by_ref=csv_refs,
        select_by_regex=args.select_by_regex,
        select_by_word=args.select_by_word,
    )
