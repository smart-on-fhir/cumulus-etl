import os
from unittest import mock

import httpx
import openai
import pydantic
from openai.types import chat

from tests.etl import TaskTestCase


class OpenAITestCase(TaskTestCase):
    """Sets up some OpenAI mocks for you"""

    MODEL_ID = None

    def setUp(self):
        super().setUp()
        self.mock_client = mock.MagicMock()
        self.mock_create = mock.AsyncMock()
        self.mock_client.chat.completions.parse = self.mock_create
        self.mock_client.models.list = self.mock_model_list(self.MODEL_ID)
        self.mock_client_factory = self.patch("openai.AsyncOpenAI")
        self.mock_client_factory.return_value = self.mock_client

        self.responses = []

    def mock_azure(self):
        self.patch_dict(os.environ, {"AZURE_OPENAI_API_KEY": "?", "AZURE_OPENAI_ENDPOINT": "?"})
        self.mock_azure_factory = self.patch("openai.AsyncAzureOpenAI")
        self.mock_azure_factory.return_value = self.mock_client

    def mock_bedrock(self):
        self.patch_dict(os.environ, {"BEDROCK_OPENAI_API_KEY": "?", "BEDROCK_OPENAI_ENDPOINT": "?"})

    @staticmethod
    def mock_model_list(models: str | list[str] = "", *, error: bool = False):
        async def async_list():
            if error:
                raise openai.APIConnectionError(request=httpx.Request("GET", "blarg"))

            nonlocal models
            if isinstance(models, str):
                models = [models]
            for model in models:
                mock_answer = mock.MagicMock()
                mock_answer.id = model
                yield mock_answer

        return async_list

    def default_content(self) -> pydantic.BaseModel:
        class EmptyModel(pydantic.BaseModel):
            pass

        return EmptyModel()

    def mock_response(
        self,
        *,
        finish_reason: str = "stop",
        content: pydantic.BaseModel | None = None,
        parsed: bool = True,
    ) -> None:
        content = content or self.default_content()
        message_args = (
            {"parsed": content} if parsed else {"content": content.model_dump_json(by_alias=True)}
        )
        message = chat.ParsedChatCompletionMessage(**message_args, role="assistant")

        self.responses.append(
            chat.ParsedChatCompletion(
                id="test-id",
                choices=[chat.ParsedChoice(finish_reason=finish_reason, index=0, message=message)],
                created=1723143708,
                model="test-model",
                object="chat.completion",
                system_fingerprint="test-fp",
            ),
        )
        self.mock_create.side_effect = self.responses
