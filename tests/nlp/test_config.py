import json

from cumulus_etl.etl.tasks.nlp_task import parse_nlp_config
from tests.utils import AsyncTestCase


class TestNlpConfig(AsyncTestCase):
    def test_missing_schema(self):
        tmpdir = self.make_tempdir()
        path = f"{tmpdir}/conf.toml"
        with open(path, "w") as f:
            f.write("""
                [[task]]
                system-prompt="yo"
                models=["gpt-oss-120b"]
            """)
        with self.assert_fatal_exit(msg="The 'response-schema' key is required"):
            parse_nlp_config("test", path)

    def test_missing_prompt(self):
        tmpdir = self.make_tempdir()
        path = f"{tmpdir}/conf.toml"
        with open(path, "w") as f:
            f.write("""
                [[task]]
                response-schema = "file.json"
                models=["gpt-oss-120b"]
            """)
        with self.assert_fatal_exit(msg="The 'system-prompt' key is required"):
            parse_nlp_config("test", path)

    def test_missing_models(self):
        tmpdir = self.make_tempdir()
        path = f"{tmpdir}/conf.toml"
        with open(path, "w") as f:
            f.write("""
                [[task]]
                system-prompt="yo"
                response-schema = "file.json"
            """)
        with self.assert_fatal_exit(msg="The 'models' key is required"):
            parse_nlp_config("test", path)

    def test_schema_complicated_path(self):
        tmpdir = self.make_tempdir()
        path = f"{tmpdir}/conf.toml"
        with open(path, "w") as f:
            f.write("""
                [[task]]
                system-prompt="yo"
                response-schema = "../file.json"
                models=["gpt-oss-120b"]
            """)
        with self.assert_fatal_exit(msg="response-schema must be a simple filename"):
            parse_nlp_config("test", path)

    def test_unknown_model(self):
        tmpdir = self.make_tempdir()
        path = f"{tmpdir}/conf.toml"
        with open(path, "w") as f:
            f.write("""
                [[task]]
                system-prompt="yo"
                response-schema = "file.json"
                models=["nope"]
            """)
        with open(f"{tmpdir}/file.json", "w") as f:
            json.dump({"title": "MyMention", "type": "object"}, f)
        with self.assert_fatal_exit(msg="Unrecognized model name 'nope'"):
            parse_nlp_config("test", path)
