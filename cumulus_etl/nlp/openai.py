"""Abstraction layer for Hugging Face's inference API"""

import os
from collections.abc import Iterable

import openai
from openai.types import chat
from pydantic import BaseModel

from cumulus_etl import errors


class OpenAIModel:
    AZURE_ID = None  # model name in MS Azure
    BEDROCK_ID = None  # model name in AWS Bedrock
    COMPOSE_ID = None  # docker service name in compose.yaml
    VLLM_INFO = None  # tuple of vLLM model name, env var stem use for URL, plus default port

    AZURE_ENV = ("AZURE_OPENAI_API_KEY", "AZURE_OPENAI_ENDPOINT")
    BEDROCK_ENV = ("BEDROCK_OPENAI_API_KEY", "BEDROCK_OPENAI_ENDPOINT")

    @staticmethod
    def _env_defined(env_keys: Iterable[str]) -> bool:
        return all(os.environ.get(key) for key in env_keys)

    def __init__(self):
        self.is_vllm = False

        if self.AZURE_ID and self._env_defined(self.AZURE_ENV):
            self.model_name = self.AZURE_ID
            self.client = openai.AsyncAzureOpenAI(api_version="2024-10-21")

        elif self.BEDROCK_ID and self._env_defined(self.BEDROCK_ENV):
            self.model_name = self.BEDROCK_ID
            self.client = openai.AsyncOpenAI(
                base_url=os.environ["BEDROCK_OPENAI_ENDPOINT"],
                api_key=os.environ["BEDROCK_OPENAI_API_KEY"],
            )

        elif self.COMPOSE_ID:
            self.model_name = self.VLLM_INFO[0]
            url = os.environ.get(f"CUMULUS_{self.VLLM_INFO[1]}_URL")  # set by compose.yaml
            url = url or f"http://localhost:{self.VLLM_INFO[2]}/v1"  # offer non-docker fallback
            self.client = openai.AsyncOpenAI(base_url=url, api_key="")
            self.is_vllm = True

        else:
            errors.fatal(
                "Missing Azure or Bedrock environment variables. "
                "Set AZURE_OPENAI_API_KEY & AZURE_OPENAI_ENDPOINT or "
                "BEDROCK_OPENAI_API_KEY & BEDROCK_OPENAI_ENDPOINT.",
                errors.ARGS_INVALID,
            )

    # override to add your own checks
    async def post_init_check(self) -> None:
        try:
            models = self.client.models.list()
            names = {model.id async for model in models}
        except openai.APIError as exc:
            message = f"NLP server is unreachable: {exc}."
            if self.is_vllm:
                message += f"\nTry running 'docker compose up {self.COMPOSE_ID} --wait'."
            errors.fatal(message, errors.SERVICE_MISSING)

        if self.model_name not in names:
            errors.fatal(
                f"NLP server does not have model ID '{self.model_name}'.",
                errors.SERVICE_MISSING,
            )

    async def prompt(self, system: str, user: str, schema: BaseModel) -> chat.ParsedChatCompletion:
        return await self._parse_prompt(system, user, schema)

    async def _parse_prompt(self, system: str, user: str, schema) -> chat.ParsedChatCompletion:
        return await self.client.chat.completions.parse(
            model=self.model_name,
            messages=[
                {"role": "system", "content": system},
                {"role": "user", "content": user},
            ],
            seed=12345,  # arbitrary, just specifying it for reproducibility
            temperature=0,  # minimize temp, also for reproducibility
            timeout=120,  # in seconds
            response_format=schema,
        )


class Gpt35Model(OpenAIModel):  # deprecated, do not use in new code (doesn't support JSON schemas)
    AZURE_ID = "gpt-35-turbo-0125"

    # 3.5 doesn't support a pydantic JSON schema, so we do some work to keep it using the same API
    # as the rest of our code.
    async def prompt(self, system: str, user: str, schema: BaseModel) -> chat.ParsedChatCompletion:
        response = await self._parse_prompt(system, user, {"type": "json_object"})
        parsed = schema.model_validate_json(response.choices[0].message.content)
        response.choices[0].message.parsed = parsed
        return response


class Gpt4Model(OpenAIModel):
    AZURE_ID = "gpt-4"


class Gpt4oModel(OpenAIModel):
    AZURE_ID = "gpt-4o"


class Gpt5Model(OpenAIModel):
    AZURE_ID = "gpt-5"


class GptOss120bModel(OpenAIModel):
    AZURE_ID = "gpt-oss-120b"
    BEDROCK_ID = "openai.gpt-oss-120b-1:0"
    COMPOSE_ID = "gpt-oss-120b"
    VLLM_INFO = ("openai/gpt-oss-120b", "GPT_OSS_120B", 8086)


class Llama4ScoutModel(OpenAIModel):
    AZURE_ID = "Llama-4-Scout-17B-16E-Instruct"
    BEDROCK_ID = "meta.llama4-scout-17b-instruct-v1:0"
    COMPOSE_ID = "llama4-scout"
    VLLM_INFO = ("nvidia/Llama-4-Scout-17B-16E-Instruct-FP8", "LLAMA4_SCOUT", 8087)
