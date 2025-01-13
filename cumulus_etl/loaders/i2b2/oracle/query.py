"""Actual queries to oracle"""

from cumulus_etl.loaders.i2b2.schema import Table, ValueType

###############################################################################
# Table.patient_dimension
###############################################################################


def sql_patient() -> str:
    birth_date = format_date("BIRTH_DATE")
    death_date = format_date("DEATH_DATE")
    cols = f"PATIENT_NUM, {birth_date}, {death_date}, SEX_CD, RACE_CD, ZIP_CD"
    return f"select {cols} \n from {Table.patient.value}"  # noqa: S608


###############################################################################
# Table.provider_dimension (FUTURE)
###############################################################################


def sql_provider() -> str:
    cols_dates = format_date("IMPORT_DATE")
    cols = f"PROVIDER_ID, PROVIDER_PATH, NAME_CHAR, {cols_dates}"
    return f"select {cols} \n from {Table.provider.value}"  # noqa: S608


###############################################################################
# Table.visit_dimension
###############################################################################


def sql_visit() -> str:
    """
    :return: select visit_dimension
    """
    start_date = format_date("START_DATE")
    end_date = format_date("END_DATE")
    import_date = format_date("IMPORT_DATE")

    cols_dates = f"{start_date}, {end_date}, {import_date}, LENGTH_OF_STAY"
    cols = f"ENCOUNTER_NUM, PATIENT_NUM, LOCATION_CD, INOUT_CD, {cols_dates}"
    return f"select {cols} \n from {Table.visit.value}"  # noqa: S608


def after_start_date(start_date: str) -> str:
    """
    WHERE After Start Date
    :param start_date: START_DATE
    :return: start_date > $start_date
    """
    return f"START_DATE > '{start_date}'"


def before_end_date(end_date: str) -> str:
    """
    WHERE Before End Date
    :param end_date: END_DATE
    :return: end_date < $end_date
    """
    return f"END_DATE < '{end_date}'"


###############################################################################
# Table.concept_dimension
###############################################################################


def sql_concept() -> str:
    """
    :return: select concept_dimension
    """
    cols_dates = format_date("IMPORT_DATE")
    cols = f"CONCEPT_CD, NAME_CHAR, SOURCESYSTEM_CD, CONCEPT_BLOB, {cols_dates}"
    return f"select {cols} \n from {Table.concept.value}"  # noqa: S608


###############################################################################
# Table.observation_fact
###############################################################################


def sql_observation_fact(categories: list[str]) -> str:
    """
    :param categories: the types of fact (or "concept cd" in the database's term)
    :return: SQL for ObservationFact
    """
    # In this method, we use aliases: O for the observation_fact table and C for the concept table.
    start_date = format_date("O.START_DATE", "START_DATE")
    end_date = format_date("O.END_DATE", "END_DATE")
    import_date = format_date("O.IMPORT_DATE", "IMPORT_DATE")

    cols_patient_dim = "O.PATIENT_NUM"
    cols_provider_dim = "O.PROVIDER_ID"
    cols_visit_dim = f"O.ENCOUNTER_NUM, {start_date}, {end_date}, O.LOCATION_CD"
    cols_obs_fact = (
        f"O.CONCEPT_CD, O.INSTANCE_NUM, {import_date}, O.TVAL_CHAR, "
        f"O.VALTYPE_CD, O.VALUEFLAG_CD, O.NVAL_NUM, O.UNITS_CD, O.OBSERVATION_BLOB"
    )
    cols = f"{cols_patient_dim}, {cols_provider_dim}, {cols_visit_dim}, {cols_obs_fact}"

    matchers = [f"(concept_cd like '{category}:%')" for category in categories]

    return (
        f"select {cols} \n from {Table.observation_fact.value} O "  # noqa: S608
        f"where {' or '.join(matchers)}"
    )


def eq_val_type(val_type: ValueType) -> str:
    return f" VALTYPE_CD={val_type.value}"


###############################################################################
#
# Full/Diff comparison helper functions
#
###############################################################################
def where(expression=None) -> str:
    return "\n WHERE " + expression if expression else ""


def AND(expression: str) -> str:
    return f"\n AND ({expression})"


def OR(expression: str) -> str:
    return f"\n OR ({expression})"


def limit(count: int):
    return where(f"ROWNUM <= {count}")


def alias(expression: str, as_alias: str) -> str:
    return f"{expression} as {as_alias}"


def cast_date(column: str) -> str:
    return f"cast({column} as date)"


def cast_date_as(column: str, as_alias=None) -> str:
    as_alias = as_alias if as_alias else column
    return alias(cast_date(column), as_alias)


def to_date(column: str) -> str:
    return f"to_date({column})"


def to_char(column: str, frmt="YYYY-MM-DD") -> str:
    return f"to_char({column}, '{frmt}')"


def format_date(column: str, column_alias=None, frmt="YYYY-MM-DD") -> str:
    return alias(to_char(cast_date(column), frmt), column_alias if column_alias else column)
