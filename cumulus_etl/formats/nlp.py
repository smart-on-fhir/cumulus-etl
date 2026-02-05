import re

import pyarrow

from cumulus_etl import errors, store


class AthenaMixin:  # like most mixins, this should go first in inheritance list to take priority
    require_empty_dir = False

    def __init__(
        self,
        root: store.Root,
        dbname: str,
        *args,
        version: int,
        connection,
        schema,
        clean: bool = False,
        **kwargs,
    ):
        self._connection = connection
        self._orig_dbname = dbname
        self._schema = schema

        # We use a different output dir format for NLP - a tree format inspired by how Cumulus
        # Library uploads files to Athena's results bucket:
        #   s3://{workgroup_result_path}/{study}/{topic}/*.parquet
        # Where topic is our dbname, minus the study and with version info added
        study, table = dbname.split("__", 1)
        root = store.Root(root.joinpath(study))
        dbname = f"{table}_v{version}"

        if clean:
            # We wanna clean out all previous uploads for this table.
            # Do this before calling __init__() because that looks at the files in the folder to
            # get the first batch number to use.
            table_with_version = re.compile(rf"/{table}_v[0-9]+$")
            try:
                for folder in root.ls():
                    if table_with_version.search(folder):
                        root.rm(folder)
            except (FileNotFoundError, PermissionError):
                pass

        super().__init__(root, dbname, *args, **kwargs)

    def finalize(self) -> None:
        if self._connection:
            self._register_athena_table()

    def _athena_args(self) -> tuple[str, str]:
        """Returns pre-location and post-location sql args"""
        return "", ""  # pragma: no cover

    def _register_athena_table(self) -> None:
        cursor = self._connection.cursor()
        pre_params, post_params = self._athena_args()
        cols = self._pyarrow_schema_to_athena_cols(self._schema)
        folder = self.root.joinpath(self.dbname)

        try:
            cursor.execute(f"DROP TABLE IF EXISTS {self._orig_dbname}")
        except Exception as exc:
            if "DROP TABLE not supported for Delta Lake" in str(exc):
                errors.fatal(
                    f"The {self._orig_dbname} table is still being defined by a Glue crawler. "
                    "Please manually remove its crawler definition from your AWS Glue config. "
                    "Or use --output-format=deltalake which will still work for a little bit.",
                    errors.ATHENA_TABLE_STILL_CRAWLER,
                )
            raise

        # https://docs.aws.amazon.com/athena/latest/ug/create-table.html
        cursor.execute(f"""
            CREATE EXTERNAL TABLE {self._orig_dbname} (
                {cols}
            )
            {pre_params}
            LOCATION '{folder}/'
            {post_params}
        """)

    def _pyarrow_schema_to_athena_cols(
        self, schema: pyarrow.DataType, is_dict: bool = False, is_list: bool = False
    ) -> str:
        cols = []
        is_schema = isinstance(schema, pyarrow.Schema)
        field_count = len(schema.names) if is_schema else schema.num_fields
        for i in range(field_count):
            field = schema.field(i)
            athena_type = ""
            if pyarrow.types.is_string(field.type):
                athena_type = "STRING"
            elif pyarrow.types.is_integer(field.type):
                athena_type = "INT"
            elif pyarrow.types.is_floating(field.type):
                athena_type = "DOUBLE"
            elif pyarrow.types.is_boolean(field.type):
                athena_type = "BOOLEAN"
            elif pyarrow.types.is_timestamp(field.type):
                athena_type = "TIMESTAMP"
            elif pyarrow.types.is_list(field.type) or pyarrow.types.is_fixed_size_list(field.type):
                athena_type = "ARRAY<"
                athena_type += self._pyarrow_schema_to_athena_cols(field.type, is_list=True)
                athena_type += ">"
            elif pyarrow.types.is_struct(field.type):
                athena_type = "STRUCT<"
                athena_type += self._pyarrow_schema_to_athena_cols(field.type, is_dict=True)
                athena_type += ">"
            else:
                errors.fatal(
                    f"Unsupported pyarrow type {type(field.type)} found.", errors.ARGS_INVALID
                )

            if is_list:
                cols.append(athena_type)
            elif is_dict:
                cols.append(f"{field.name}: {athena_type}")
            else:
                cols.append(f"{field.name} {athena_type}")

        return ", ".join(cols)
