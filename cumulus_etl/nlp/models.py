"""Abstraction layer for inference APIs"""

import abc
import dataclasses
import json
import os
from collections.abc import Iterable
from typing import NoReturn, Self

import boto3
import openai
from pydantic import BaseModel

from cumulus_etl import errors
from cumulus_etl.nlp.utils import cache_wrapper

_provider_name = "local"


def set_nlp_provider(provider: str | None) -> None:
    global _provider_name
    _provider_name = provider or "local"


@dataclasses.dataclass
class PromptResponse:
    """
    The response from an NLP model.

    Be cautious if removing or renaming fields, as it will likely break serialization.
    """

    answer: BaseModel
    fingerprint: str | None = None

    def to_dict(self) -> dict:
        serialized = dataclasses.asdict(self)
        serialized["answer"] = self.answer.model_dump(
            round_trip=True, exclude_unset=True, by_alias=True
        )
        return serialized

    @classmethod
    def from_dict(cls, serialized: dict, schema: type[BaseModel]) -> Self:
        answer = schema.model_validate(serialized.pop("answer"))
        return PromptResponse(answer, **serialized)


class Provider(abc.ABC):
    async def post_init_check(self) -> None:
        pass  # pragma: no cover

    @abc.abstractmethod
    async def prompt(self, system: str, user: str, schema: type[BaseModel]) -> PromptResponse:
        pass  # pragma: no cover


class BedrockProvider(Provider):
    def __init__(self, model_name: str):
        self.model_name = model_name
        self.client = boto3.client("bedrock-runtime")

    async def prompt(self, system: str, user: str, schema: type[BaseModel]) -> PromptResponse:
        # Bedrock does not make it easy to define a JSON schema.
        # See https://builder.aws.com/content/2hWA16FSt2bIzKs0Z1fgJBwu589/ for how to do it.
        # Basically, you have to define a JSON-conversion tool and ask for it to be used.
        #
        # But even then, it's only sometimes used, based on the model (e.g. gpt-oss-120b is like
        # a 50/50 shot).
        # So we handle both response types below during parsing, regardless of schema support.
        #
        # Also, some models *don't* support this approach at all, and will error out if you provide
        # the toolChoice field. So let's skip those here.
        supports_schema = self.model_name not in {"us.meta.llama4-scout-17b-instruct-v1:0"}

        extra_args = {}
        if supports_schema:
            extra_args = {
                "toolConfig": {
                    "tools": [
                        {
                            "toolSpec": {
                                "name": "to_json",
                                "description": "convert to JSON",
                                "inputSchema": {"json": schema.model_json_schema()},
                            },
                        }
                    ],
                    "toolChoice": {"tool": {"name": "to_json"}},
                },
            }

        response = self.client.converse(
            modelId=self.model_name,
            system=[{"text": system}],
            messages=[{"role": "user", "content": [{"text": user}]}],
            inferenceConfig={"temperature": 0},
            **extra_args,
        )

        stop_reason = response.get("stopReason")
        if stop_reason not in {"end_turn", "tool_use"}:
            raise ValueError(f"did not complete, with stop reason: {stop_reason}")

        # There could be multiple content blocks returned (e.g. gpt-oss gives a reasoning block)
        for content in response["output"]["message"]["content"]:
            if "toolUse" in content:
                raw_json = content["toolUse"]["input"]
                # Sometimes (e.g. with claude sonnet 4.5) we get a wrapper field of "parameter"
                if len(raw_json) == 1 and "parameter" in raw_json:
                    raw_json = raw_json["parameter"]
                answer = schema.model_validate(raw_json)
                break
            if "text" in content:
                # Do a little work to find the JSON in the middle of a text response.
                # Usually, it is formatted with some markdown (at least in llama4 and gpt-oss).
                text = content["text"]
                pieces = text.split("```")
                if len(pieces) == 3:
                    text = pieces[1].removeprefix("json")
                # This might raise a validation error, but that's caught in upper layers.
                answer = schema.model_validate_json(text)
                break
        else:
            raise ValueError("no response content found")

        return PromptResponse(answer)


class OpenAIProvider(Provider):
    def __init__(self, model_name: str, client: openai.AsyncOpenAI):
        self.model_name = model_name
        self.client = client

    async def post_init_check(self) -> None:
        try:
            models = self.client.models.list()
            names = {model.id async for model in models}
        except openai.APIError as exc:
            errors.fatal(f"NLP server is unreachable: {exc}", errors.SERVICE_MISSING)

        if self.model_name not in names:
            errors.fatal(
                f"NLP server does not have model ID '{self.model_name}'.",
                errors.SERVICE_MISSING,
            )

    async def prompt(self, system: str, user: str, schema: type[BaseModel]) -> PromptResponse:
        # Some older models don't support taking a schema directly - so treat them specially.
        supports_schema = self.model_name not in {"gpt-35-turbo-0125"}

        response = await self.client.chat.completions.parse(
            model=self.model_name,
            messages=[
                {"role": "system", "content": system},
                {"role": "user", "content": user},
            ],
            seed=12345,  # arbitrary, just specifying it for reproducibility
            temperature=0,  # minimize temp, also for reproducibility
            timeout=120,  # in seconds
            response_format=schema if supports_schema else {"type": "json_object"},
        )

        choice = response.choices[0]
        if supports_schema:
            parsed = choice.message.parsed
        else:
            print(choice.message.content)
            parsed = schema.model_validate_json(choice.message.content)

        if choice.finish_reason != "stop" or not parsed:
            raise ValueError(f"did not complete, with finish reason: {choice.finish_reason}")

        return PromptResponse(
            answer=parsed,
            fingerprint=response.system_fingerprint,
        )


class AzureProvider(OpenAIProvider):
    def __init__(self, model_name: str):
        super().__init__(model_name, openai.AsyncAzureOpenAI(api_version="2024-10-21"))


class VllmProvider(OpenAIProvider):
    def __init__(self, compose_id, model_name: str, env_stem: str, port: int):
        url = os.environ.get(f"CUMULUS_{env_stem}_URL")  # set by compose.yaml
        url = url or f"http://localhost:{port}/v1"  # offer non-docker fallback
        super().__init__(model_name, openai.AsyncOpenAI(base_url=url, api_key=""))
        self.compose_id = compose_id

    async def post_init_check(self) -> None:
        try:
            await super().post_init_check()
        except SystemExit as exc:
            if exc.code == errors.SERVICE_MISSING:
                errors.fatal(  # add a little suggestion
                    f"Try running 'docker compose up {self.compose_id} --wait'.\n"
                    "Or pass --provider to specify a cloud NLP provider.",
                    errors.SERVICE_MISSING,
                )
            raise


class Model:
    AZURE_ID = None  # model name in MS Azure
    BEDROCK_ID = None  # model name in AWS Bedrock
    COMPOSE_ID = None  # docker service name in compose.yaml
    VLLM_INFO = None  # tuple of vLLM model name, env var stem use for URL, plus default port

    AZURE_ENV = ("AZURE_OPENAI_API_KEY", "AZURE_OPENAI_ENDPOINT")

    @staticmethod
    def _env_defined(env_keys: Iterable[str]) -> bool:
        return all(os.environ.get(key) for key in env_keys)

    def raise_unsupported_error(self) -> NoReturn:
        errors.fatal(
            f"{self.__class__.__name__} does not support the '{_provider_name}' provider.",
            errors.ARGS_INVALID,
        )

    def __init__(self):
        if _provider_name == "azure":
            if not self.AZURE_ID:
                self.raise_unsupported_error()
            if not self._env_defined(self.AZURE_ENV):
                errors.fatal(
                    "Missing Azure environment variables. "
                    "Set both AZURE_OPENAI_API_KEY & AZURE_OPENAI_ENDPOINT.",
                    errors.ARGS_INVALID,
                )
            self.provider = AzureProvider(self.AZURE_ID)

        elif _provider_name == "bedrock":
            if not self.BEDROCK_ID:
                self.raise_unsupported_error()
            self.provider = BedrockProvider(self.BEDROCK_ID)

        else:
            if not self.COMPOSE_ID:
                self.raise_unsupported_error()
            self.provider = VllmProvider(
                self.COMPOSE_ID, self.VLLM_INFO[0], self.VLLM_INFO[1], self.VLLM_INFO[2]
            )

    # override to add your own checks
    async def post_init_check(self) -> None:
        await self.provider.post_init_check()

    async def prompt(
        self,
        system: str,
        user: str,
        *,
        schema: type[BaseModel],
        cache_dir: str,
        cache_namespace: str,
        note_text: str,
    ) -> PromptResponse:
        return await cache_wrapper(
            cache_dir,
            cache_namespace,
            note_text,
            lambda x: PromptResponse.from_dict(json.loads(x), schema),  # from file
            lambda x: json.dumps(x.to_dict()),  # to file
            self.provider.prompt,
            system,
            user,
            schema,
        )


class Gpt35Model(Model):  # deprecated, do not use in new code (doesn't support JSON schemas)
    AZURE_ID = "gpt-35-turbo-0125"


class Gpt4Model(Model):
    AZURE_ID = "gpt-4"


class Gpt4oModel(Model):
    AZURE_ID = "gpt-4o"


class Gpt5Model(Model):
    AZURE_ID = "gpt-5"


class GptOss120bModel(Model):
    AZURE_ID = "gpt-oss-120b"
    BEDROCK_ID = "openai.gpt-oss-120b-1:0"
    COMPOSE_ID = "gpt-oss-120b"
    VLLM_INFO = ("openai/gpt-oss-120b", "GPT_OSS_120B", 8086)


class Llama4ScoutModel(Model):
    AZURE_ID = "Llama-4-Scout-17B-16E-Instruct"
    BEDROCK_ID = "us.meta.llama4-scout-17b-instruct-v1:0"
    COMPOSE_ID = "llama4-scout"
    VLLM_INFO = ("nvidia/Llama-4-Scout-17B-16E-Instruct-FP4", "LLAMA4_SCOUT", 8087)


class ClaudeSonnet45Model(Model):
    BEDROCK_ID = "us.anthropic.claude-sonnet-4-5-20250929-v1:0"
