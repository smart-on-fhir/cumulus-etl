import contextlib
import io
import json
import os
from unittest import mock

import pyarrow
import pyathena

from cumulus_etl import cli, common, errors
from tests.etl import BaseEtlSimple
from tests.nlp.utils import NlpModelTestCase
from tests.s3mock import S3Mixin


class HelperMixin:
    def mock_age_results(self):
        # Set up a simple NLP model for the two default input docs
        self.mock_azure("gpt-oss-120b")
        age_model = self.load_pydantic_model("example/age.json")
        self.mock_response(content=age_model(has_mention=True, age=10))
        self.mock_response(content=age_model(has_mention=True, age=11))

    def mock_donor_results(self):
        # Set up a simple NLP model for the two default input docs
        self.mock_azure("gpt-oss-120b")
        donor_model = self.load_pydantic_model("irae/donor.json")
        irae = donor_model.model_validate(
            {
                "donor_transplant_date_mention": {"has_mention": False, "spans": []},
                "donor_type_mention": {"has_mention": False, "spans": []},
                "donor_relationship_mention": {"has_mention": False, "spans": []},
                "donor_hla_match_quality_mention": {"has_mention": False, "spans": []},
                "donor_hla_mismatch_count_mention": {"has_mention": False, "spans": []},
                "donor_serostatus_mention": {"has_mention": False, "spans": []},
                "donor_serostatus_cmv_mention": {"has_mention": False, "spans": []},
                "donor_serostatus_ebv_mention": {"has_mention": False, "spans": []},
                "recipient_serostatus_mention": {"has_mention": False, "spans": []},
                "recipient_serostatus_cmv_mention": {"has_mention": False, "spans": []},
                "recipient_serostatus_ebv_mention": {"has_mention": False, "spans": []},
            }
        )
        self.mock_response(content=irae)
        self.mock_response(content=irae)


class TestNlpCli(HelperMixin, NlpModelTestCase, BaseEtlSimple):
    async def test_multiple_tasks(self):
        self.mock_age_results()
        self.mock_donor_results()

        # Multiple NLP models will fail
        with self.assert_fatal_exit(errors.TASK_TOO_MANY):
            await self.run_etl(
                tasks=["example_nlp__nlp_gpt_oss_120b", "example_nlp__nlp_llama4_scout"]
            )

        # But same model type does work
        await self.run_etl(tasks=["example_nlp__nlp_gpt_oss_120b", "irae__nlp_donor_gpt_oss_120b"])

    async def test_non_text_docrefs_are_ignored(self):
        self.mock_age_results()

        valid = {
            "resourceType": "DocumentReference",
            "id": "valid",
            "subject": {"reference": "Patient/1"},
            "context": {"encounter": [{"reference": "Encounter/1"}]},
            "content": [
                {
                    "attachment": {
                        "contentType": "text/plain",
                        "data": "aGVsbG8=",
                    },
                }
            ],
        }
        ignored = {
            "resourceType": "DocumentReference",
            "id": "ignored",
        }

        tmpdir = self.make_tempdir()
        with common.NdjsonWriter(f"{tmpdir}/notes.ndjson") as writer:
            writer.write(valid)
            writer.write(ignored)

        task = "example_nlp__nlp_gpt_oss_120b"
        with self.assert_fatal_exit(errors.TASK_FAILED):
            await self.run_etl(input_path=tmpdir, tasks=[task], errors_to=f"{tmpdir}/errors")

        with open(f"{tmpdir}/errors/{task}/nlp-errors.ndjson", "rb") as f:
            self.assertEqual(ignored, json.load(f))
        with open(self.result_path(task), "rb") as f:
            self.assertEqual(
                json.load(f)["note_ref"],
                "DocumentReference/5feee0d19f20824ac33967633125a76e4f916a7888c60bf777a0eec5a1555114",
            )

    async def test_output_dir_with_workgroup(self):
        with self.assert_fatal_exit(errors.ARGS_CONFLICT, "Both an output folder and an Athena"):
            await self.run_etl("--athena-workgroup=xxx", tasks=["example_nlp__nlp_gpt_oss_120b"])

    async def test_dir_with_contents(self):
        os.makedirs(self.output_path)
        with open(f"{self.output_path}/hello.txt", "wb") as f:
            f.write(b"hello")
        with self.assert_fatal_exit(errors.FOLDER_NOT_EMPTY, "the --athena-workgroup flavor"):
            await self.run_etl(tasks=["example_nlp__nlp_gpt_oss_120b"])

    async def test_warn_on_parquet_output(self):
        self.mock_age_results()
        stdout = io.StringIO()
        with contextlib.redirect_stdout(stdout):
            await self.run_etl(tasks=["example_nlp__nlp_gpt_oss_120b"], output_format="parquet")
        self.assertIn("will not be registered as an Athena table", stdout.getvalue())

    async def test_deltalake_with_clean(self):
        with self.assert_fatal_exit(errors.ARGS_CONFLICT, "Cannot provide --clean"):
            await self.run_etl(
                "--clean", output_format="deltalake", tasks=["example_nlp__nlp_gpt_oss_120b"]
            )


class TestAthenaRun(HelperMixin, S3Mixin, NlpModelTestCase, BaseEtlSimple):
    def mock_nlp(self, encryption: bool = True):
        self.mock_age_results()

        # Set up a mock athena, pointing at a results folder

        def get_work_group(*, WorkGroup):
            self.assertEqual(WorkGroup, "wg")
            wg_config = {
                "WorkGroup": {
                    "Configuration": {
                        "ResultConfiguration": {
                            "OutputLocation": f"{self.bucket_url}/",
                        },
                    },
                },
            }
            if encryption:
                result_config = wg_config["WorkGroup"]["Configuration"]["ResultConfiguration"]
                result_config["EncryptionConfiguration"] = {
                    "EncryptionOption": "aws:kms",
                }
            return wg_config

        self.cursor = mock.Mock()
        self.connection = mock.Mock()
        self.connection.client.get_work_group = get_work_group
        self.connection.cursor.return_value = self.cursor

        def connect(*, region_name, work_group, schema_name):
            if not work_group:
                raise pyathena.ProgrammingError
            self.connection.region_name = region_name
            self.connection.work_group = work_group
            self.connection.schema_name = schema_name
            return self.connection

        self.patch("pyathena.connect", new=connect)

    async def run_nlp(
        self,
        *args,
        phi_path: str | None = None,
        set_workgroup: bool = True,
        set_database: bool = True,
        task: str = "example_nlp__nlp_gpt_oss_120b",
    ) -> None:
        args = [
            "nlp",
            self.input_path,
            phi_path or self.phi_path,
            f"--task={task}",
            "--provider=azure",
            *args,
        ]
        if set_workgroup:
            args.append("--athena-workgroup=wg")
        if set_database:
            args.append("--athena-database=db")
        await cli.main(args)

    async def test_deltalake_format(self):
        self.mock_nlp()
        with self.assert_fatal_exit(errors.ARGS_CONFLICT, "cannot use the deltalake output"):
            await self.run_nlp("--output-format=deltalake")

    async def test_parquet_format(self):
        self.mock_nlp()
        await self.run_nlp()  # default is parquet
        root = "mockbucket/cumulus_user_uploads/db"
        self.assertEqual(
            set(self.s3fs.find("mockbucket")),
            {
                f"{root}/codebook.id",
                f"{root}/example_nlp/nlp_gpt_oss_120b_v1/nlp_gpt_oss_120b_v1.000.parquet",
            },
        )

    async def test_ndjson_format(self):
        self.mock_nlp()
        await self.run_nlp("--output-format=ndjson")
        root = "mockbucket/cumulus_user_uploads/db"
        self.assertEqual(
            set(self.s3fs.find("mockbucket")),
            {
                f"{root}/codebook.id",
                f"{root}/example_nlp/nlp_gpt_oss_120b_v1/nlp_gpt_oss_120b_v1.000.ndjson",
            },
        )

    async def test_no_workgroup(self):
        self.mock_nlp()
        with self.assert_fatal_exit(errors.ARGS_INVALID, "No Athena workgroup provided"):
            await self.run_nlp(set_workgroup=False)

    async def test_no_database(self):
        self.mock_nlp()
        with self.assert_fatal_exit(errors.ARGS_INVALID, "No Athena database provided"):
            await self.run_nlp(set_database=False)

    async def test_unencrypted_bucket(self):
        self.mock_nlp(encryption=False)
        with self.assert_fatal_exit(errors.ATHENA_NOT_ENCRYPTED):
            await self.run_nlp()

    async def test_two_phi_folders(self):
        self.mock_nlp()
        await self.run_nlp()
        with self.assert_fatal_exit(errors.WRONG_PHI_FOLDER):
            await self.run_nlp(phi_path=f"{self.tmpdir}/phi2")

    async def test_cleaning_previous_content(self):
        self.mock_nlp()
        root = "mockbucket/cumulus_user_uploads/db/example_nlp"

        await self.run_nlp("--clean")  # clean without anything there should be a graceful no-op
        self.assertEqual(
            set(self.s3fs.find(root)),
            {f"{root}/nlp_gpt_oss_120b_v1/nlp_gpt_oss_120b_v1.000.parquet"},
        )

        # Pretend we had some previous runs too (of both a same and a different model)
        self.s3fs.copy(
            f"{root}/nlp_gpt_oss_120b_v1/nlp_gpt_oss_120b_v1.000.parquet",
            f"{root}/nlp_gpt_oss_120b_v0/nlp_gpt_oss_120b_v0.000.parquet",
        )
        self.s3fs.copy(
            f"{root}/nlp_gpt_oss_120b_v1/nlp_gpt_oss_120b_v1.000.parquet",
            f"{root}/nlp_gpt5_v1/nlp_gpt5_v1.000.parquet",
        )

        # Second normal backup, which will append to previous results
        await self.run_nlp()
        self.assertEqual(
            set(self.s3fs.find(root)),
            {
                f"{root}/nlp_gpt_oss_120b_v0/nlp_gpt_oss_120b_v0.000.parquet",
                f"{root}/nlp_gpt_oss_120b_v1/nlp_gpt_oss_120b_v1.000.parquet",
                f"{root}/nlp_gpt_oss_120b_v1/nlp_gpt_oss_120b_v1.001.parquet",
                f"{root}/nlp_gpt5_v1/nlp_gpt5_v1.000.parquet",
            },
        )

        # Now let's run with --clean - any existing same-model files should be wiped
        await self.run_nlp("--clean")
        self.assertEqual(
            set(self.s3fs.find(root)),
            {
                f"{root}/nlp_gpt_oss_120b_v1/nlp_gpt_oss_120b_v1.000.parquet",
                f"{root}/nlp_gpt5_v1/nlp_gpt5_v1.000.parquet",
            },
        )

    async def test_existing_delta_lake_error(self):
        self.mock_nlp()

        # Try a random non-delta-lake error first - this will bubble up
        self.cursor.execute.side_effect = ValueError
        with self.assertRaises(ValueError):
            await self.run_nlp()

        # Now try with the correct error message, it will be caught and handled separately
        self.cursor.execute.side_effect = ValueError("DROP TABLE not supported for Delta Lake")
        with self.assert_fatal_exit(errors.ATHENA_TABLE_STILL_CRAWLER):
            await self.run_nlp()

    async def test_column_schema(self):
        self.mock_nlp()

        # Prepare a fake schema, to exercise all the types we can convert.
        # string, int, and struct are already covered by wrapping schema.
        schema = pyarrow.struct(
            [
                pyarrow.field("float", pyarrow.float32()),
                pyarrow.field("bool", pyarrow.bool_()),
                pyarrow.field("time", pyarrow.timestamp("s")),
                pyarrow.field("list", pyarrow.list_(pyarrow.float32())),
                pyarrow.field("fixed_list", pyarrow.list_(pyarrow.float32(), 2)),
            ]
        )
        method = "cumulus_etl.etl.tasks.nlp_task.BaseModelTask.convert_pydantic_fields_to_pyarrow"
        self.patch(method, return_value=schema)

        await self.run_nlp()

        self.assertEqual(self.cursor.execute.call_count, 2)
        self.assertEqual(
            self.cursor.execute.call_args_list[0][0],
            ("DROP TABLE IF EXISTS example_nlp__nlp_gpt_oss_120b",),
        )

        # Now check the columns get translated correctly.
        # IRAE is a complicated schema, but that's useful to confirm we hit all the types.
        ctas = self.cursor.execute.call_args_list[1][0][0]
        expected = """
            CREATE EXTERNAL TABLE example_nlp__nlp_gpt_oss_120b (
                note_ref STRING,
                encounter_ref STRING,
                subject_ref STRING,
                generated_on STRING,
                task_version INT,
                system_fingerprint STRING,
                result STRUCT<float: DOUBLE, bool: BOOLEAN, time: TIMESTAMP, list: ARRAY<DOUBLE>,
                    fixed_list: ARRAY<DOUBLE>>
            )
            STORED AS PARQUET
            LOCATION 's3://mockbucket/cumulus_user_uploads/db/example_nlp/nlp_gpt_oss_120b_v1/'
            TBLPROPERTIES ("parquet.compression"="SNAPPY")
        """
        # remove all multiple whitespace
        ctas = " ".join(ctas.split())
        expected = " ".join(expected.split())
        self.assertEqual(ctas, expected)

    async def test_column_schema_unknown_type(self):
        self.mock_nlp()

        schema = pyarrow.struct(
            [
                pyarrow.field("binary", pyarrow.binary()),
            ]
        )
        method = "cumulus_etl.etl.tasks.nlp_task.BaseModelTask.convert_pydantic_fields_to_pyarrow"
        self.patch(method, return_value=schema)

        with self.assert_fatal_exit(errors.ARGS_INVALID, "Unsupported pyarrow type"):
            await self.run_nlp()
