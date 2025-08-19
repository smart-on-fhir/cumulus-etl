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
        mock_client_factory = self.patch("openai.AsyncOpenAI")
        mock_client_factory.return_value = self.mock_client

        # Also set up azure mocks, which have a different entry point
        self.patch_dict(os.environ, {"AZURE_OPENAI_API_KEY": "?", "AZURE_OPENAI_ENDPOINT": "?"})
        mock_azure_factory = self.patch("openai.AsyncAzureOpenAI")
        mock_azure_factory.return_value = self.mock_client

        self.responses = []

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
        self, *, finish_reason: str = "stop", content: pydantic.BaseModel | None = None
    ) -> None:
        content = content or self.default_content()

        self.responses.append(
            chat.ParsedChatCompletion(
                id="test-id",
                choices=[
                    chat.ParsedChoice(
                        finish_reason=finish_reason,
                        index=0,
                        message=chat.ParsedChatCompletionMessage(parsed=content, role="assistant"),
                    ),
                ],
                created=1723143708,
                model="test-model",
                object="chat.completion",
                system_fingerprint="test-fp",
            ),
        )
        self.mock_create.side_effect = self.responses
