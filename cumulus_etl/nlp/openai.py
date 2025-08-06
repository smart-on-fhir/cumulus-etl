"""Abstraction layer for Hugging Face's inference API"""

import abc
import os

import openai

from cumulus_etl import errors


class OpenAIModel(abc.ABC):
    COMPOSE_ID = None
    MODEL_NAME = None

    @property
    @abc.abstractmethod
    def url(self) -> str:
        """The OpenAI compatible URL to talk to (where's the server?)"""

    @property
    @abc.abstractmethod
    def api_key(self) -> str:
        """The API key to use (empty string for local servers)"""

    def __init__(self):
        self.client = openai.AsyncClient(base_url=self.url, api_key=self.api_key)

    async def check(self) -> None:
        try:
            models = self.client.models.list()
            names = {model.id async for model in models}
        except openai.APIError:
            errors.fatal(
                f"NLP server '{self.COMPOSE_ID}' is unreachable.\n"
                f"Try running 'docker compose up {self.COMPOSE_ID} --wait'.",
                errors.SERVICE_MISSING,
            )

        if self.MODEL_NAME not in names:
            errors.fatal(
                f"NLP server '{self.COMPOSE_ID}' is using an unexpected model setup.",
                errors.SERVICE_MISSING,
            )

    async def prompt(self, system: str, user: str) -> str:
        response = await self.client.responses.create(
            model=self.MODEL_NAME,
            instructions=system,
            input=user,
            temperature=0,
        )
        return response.output_text.strip()


class LocalModel(OpenAIModel, abc.ABC):
    @property
    def api_key(self) -> str:
        return ""


class Llama2Model(LocalModel):
    COMPOSE_ID = "llama2"
    MODEL_NAME = "meta-llama/Llama-2-13b-chat-hf"

    @property
    def url(self) -> str:
        # 8000 and 8080 are both used as defaults in ctakesclient (cnlp & ctakes respectively).
        # 8086 is used as a joking reference to Hugging Face (HF = 86).
        return os.environ.get("CUMULUS_LLAMA2_URL") or "http://localhost:8086/v1"
