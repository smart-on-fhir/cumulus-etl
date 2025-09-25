import os
from unittest import mock

import httpx
import openai
import pydantic
from openai.types import chat, completion_usage

from tests.etl import TaskTestCase


class NlpModelTestCase(TaskTestCase):
    """Sets up some NLP model mocks for you"""

    MODEL_ID = None

    def setUp(self):
        super().setUp()
        self.mock_local(self.MODEL_ID)
        self.responses = []

    def mock_provider(self, provider: str) -> None:
        self.patch("cumulus_etl.nlp.models._provider_name", new=provider)

    def mock_openai(self, model_id: str) -> None:
        self.mock_client = mock.AsyncMock()
        self.mock_create = mock.AsyncMock()
        self.mock_client.chat.completions.parse = self.mock_create
        self.mock_client_factory = self.patch("openai.AsyncOpenAI")
        self.mock_client_factory.return_value = self.mock_client
        self.mock_client.models.list = self.mock_model_list(model_id)

    def mock_azure(self, model_id: str, batching: bool = False) -> None:
        self.mode = "azure"
        self.mock_provider(self.mode)
        self.mock_openai(model_id)
        self.patch_dict(os.environ, {"AZURE_OPENAI_API_KEY": "?", "AZURE_OPENAI_ENDPOINT": "?"})
        self.mock_azure_factory = self.patch("openai.AsyncAzureOpenAI")
        self.mock_azure_factory.return_value = self.mock_client

    def mock_bedrock(self) -> None:
        self.mode = "bedrock"
        self.mock_provider(self.mode)
        self.mock_client = mock.MagicMock()
        self.mock_create = mock.MagicMock()
        self.mock_client_factory = self.patch("cumulus_etl.nlp.models.boto3.client")
        self.mock_client_factory.return_value = self.mock_client
        self.mock_client.converse = self.mock_create

    def mock_local(self, model_id: str) -> None:
        self.mode = "local"
        self.mock_provider(self.mode)
        self.mock_openai(model_id)

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

    def add_response(self, response: dict) -> None:
        self.responses.append(response)
        self.mock_create.side_effect = self.responses

    def mock_response(
        self,
        *,
        finish_reason: str | None = None,
        content: pydantic.BaseModel | dict | str | None = None,
        usage: tuple[int, int, int, int] | None = None,  # input, cache-read, cache-write, output
    ) -> None:
        content = content or self.default_content()

        if self.mode == "bedrock":
            if isinstance(content, str):
                default_stop = "end_turn"
                message = {"text": content}
            else:
                default_stop = "tool_use"
                if isinstance(content, pydantic.BaseModel):
                    content = content.model_dump(by_alias=True)
                message = {"toolUse": {"input": content}}
            response = {
                "stopReason": finish_reason or default_stop,
                "output": {"message": {"content": [message]}},
            }
            if usage:
                response["usage"] = {
                    "inputTokens": usage[0],
                    "cacheReadInputTokens": usage[1],
                    "cacheWriteInputTokens": usage[2],
                    "outputTokens": usage[3],
                }
            self.add_response(response)
        else:
            if isinstance(content, str):
                message_args = {"content": content}
            else:
                message_args = {"parsed": content}
            message = chat.ParsedChatCompletionMessage(**message_args, role="assistant")

            self.add_response(
                chat.ParsedChatCompletion(
                    id="test-id",
                    choices=[
                        chat.ParsedChoice(
                            finish_reason=finish_reason or "stop", index=0, message=message
                        )
                    ],
                    usage=completion_usage.CompletionUsage(
                        completion_tokens=usage[3] if usage else 0,
                        prompt_tokens=(usage[0] + usage[1]) if usage else 0,
                        total_tokens=0,
                        prompt_tokens_details=completion_usage.PromptTokensDetails(
                            cached_tokens=usage[1] if usage else 0
                        ),
                    ),
                    created=1723143708,
                    model="test-model",
                    object="chat.completion",
                    system_fingerprint="test-fp",
                ),
            )
