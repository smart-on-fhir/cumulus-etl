"""Abstraction layer for Hugging Face's inference API"""

import abc
import os

import openai
from openai.types import chat
from pydantic import BaseModel

from cumulus_etl import errors


class OpenAIModel(abc.ABC):
    USER_ID = None  # name in compose file or brand name
    MODEL_NAME = None  # which model to request via the API

    @abc.abstractmethod
    def make_client(self) -> openai.AsyncOpenAI:
        """Creates an NLP client"""

    def __init__(self):
        self.client = self.make_client()

    # override to add your own checks
    @classmethod
    async def pre_init_check(cls) -> None:
        pass

    # override to add your own checks
    async def post_init_check(self) -> None:
        try:
            models = self.client.models.list()
            names = {model.id async for model in models}
        except openai.APIError:
            errors.fatal(
                f"NLP server '{self.USER_ID}' is unreachable.\n"
                f"If it's a local server, try running 'docker compose up {self.USER_ID} --wait'.",
                errors.SERVICE_MISSING,
            )

        if self.MODEL_NAME not in names:
            errors.fatal(
                f"NLP server '{self.USER_ID}' is using an unexpected model setup.",
                errors.SERVICE_MISSING,
            )

    async def prompt(self, system: str, user: str, schema: BaseModel) -> chat.ParsedChatCompletion:
        return await self.client.chat.completions.parse(
            model=self.MODEL_NAME,
            messages=[
                {"role": "system", "content": system},
                {"role": "user", "content": user},
            ],
            seed=12345,  # arbitrary, just specifying it for reproducibility
            temperature=0,  # minimize temp, also for reproducibility
            timeout=120,  # in seconds
            response_format=schema,
        )


class AzureModel(OpenAIModel):
    USER_ID = "Azure"

    @classmethod
    async def pre_init_check(cls) -> None:
        await super().pre_init_check()

        messages = []
        if not os.environ.get("AZURE_OPENAI_API_KEY"):
            messages.append("The AZURE_OPENAI_API_KEY environment variable is not set.")
        if not os.environ.get("AZURE_OPENAI_ENDPOINT"):
            messages.append("The AZURE_OPENAI_ENDPOINT environment variable is not set.")

        if messages:
            errors.fatal("\n".join(messages), errors.ARGS_INVALID)

    def make_client(self) -> openai.AsyncOpenAI:
        return openai.AsyncAzureOpenAI(api_version="2024-06-01")


class Gpt35Model(AzureModel):
    MODEL_NAME = "gpt-35-turbo-0125"


class Gpt4Model(AzureModel):
    MODEL_NAME = "gpt-4"


class Gpt4oModel(AzureModel):
    MODEL_NAME = "gpt-4o"


class Gpt5Model(AzureModel):
    MODEL_NAME = "gpt-5"


class LocalModel(OpenAIModel, abc.ABC):
    @property
    @abc.abstractmethod
    def url(self) -> str:
        """The OpenAI compatible URL to talk to (where's the server?)"""

    def make_client(self) -> openai.AsyncOpenAI:
        return openai.AsyncOpenAI(base_url=self.url, api_key="")


class GptOss120bModel(LocalModel):
    USER_ID = "gpt-oss-120b"
    MODEL_NAME = "openai/gpt-oss-120b"

    @property
    def url(self) -> str:
        return os.environ.get("CUMULUS_GPT_OSS_120B_URL") or "http://localhost:8086/v1"


class Llama4ScoutModel(LocalModel):
    USER_ID = "llama4-scout"
    MODEL_NAME = "nvidia/Llama-4-Scout-17B-16E-Instruct-FP8"

    @property
    def url(self) -> str:
        return os.environ.get("CUMULUS_LLAMA4_SCOUT_URL") or "http://localhost:8087/v1"
