"""Abstraction layer for inference APIs"""

import abc
import argparse
import asyncio
import dataclasses
import datetime
import json
import logging
import os
import pathlib
import tempfile
from collections.abc import AsyncIterator, Iterable
from typing import NoReturn, Self

import boto3
import openai
import rich
import rich.text
from openai.types.chat import ParsedChatCompletion
from pydantic import BaseModel

from cumulus_etl import common, errors, nlp

_provider_name = "local"
_azure_deployment = None
_use_batch_mode = False


def set_nlp_config(args: argparse.Namespace) -> None:
    global _provider_name
    _provider_name = args.provider or "local"

    global _azure_deployment
    _azure_deployment = args.azure_deployment

    global _use_batch_mode
    _use_batch_mode = args.batch


@dataclasses.dataclass(kw_only=True)
class Prompt:
    system: str
    user: str
    schema: type[BaseModel]
    cache_dir: str
    cache_namespace: str
    cache_checksum: str


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


@dataclasses.dataclass(kw_only=True)
class TokenStats:
    new_input_tokens: int = 0
    cache_read_input_tokens: int = 0
    cache_written_input_tokens: int = 0
    output_tokens: int = 0


@dataclasses.dataclass(kw_only=True)
class TokenPrices:
    date: datetime.date  # when these prices were last updated
    # These values are in dollars per 1,000-tokens (e.g. $0.0165/1000-tokens)
    new_input_tokens: float
    cache_read_input_tokens: float = 0
    cache_written_input_tokens: float = 0
    output_tokens: float
    multiplier: float = 1


class Provider(abc.ABC):
    def __init__(self):
        self.stats = TokenStats()
        self.supports_batches = False

    async def post_init_check(self) -> None:
        pass  # pragma: no cover

    @abc.abstractmethod
    async def prompt(self, system: str, user: str, schema: type[BaseModel]) -> PromptResponse:
        pass  # pragma: no cover

    # Called when all reads are done - provider can finish up any in-progress batches, etc
    async def finish(self) -> None:
        pass  # pragma: no cover


class BedrockProvider(Provider):
    def __init__(
        self,
        model_name: str,
        *,
        supports_cache: bool = True,
        supports_schema: bool = True,
    ):
        super().__init__()
        self.model_name = model_name
        self.supports_cache = supports_cache
        self.supports_schema = supports_schema
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
        extra_args = {}
        if self.supports_schema:
            extra_args = {
                "toolConfig": {
                    "tools": [
                        {
                            "toolSpec": {
                                "name": "to_json",
                                "description": "convert to JSON",
                                "inputSchema": {"json": schema.model_json_schema()},
                            },
                        },
                    ],
                    "toolChoice": {"tool": {"name": "to_json"}},
                },
            }
            if self.supports_cache:
                extra_args["toolConfig"]["tools"].append({"cachePoint": {"type": "default"}})

        system_prompts = [{"text": system}]
        if self.supports_cache:
            system_prompts.append({"cachePoint": {"type": "default"}})

        response = self.client.converse(
            modelId=self.model_name,
            system=system_prompts,
            messages=[{"role": "user", "content": [{"text": user}]}],
            inferenceConfig={"temperature": 0},
            **extra_args,
        )

        usage = response.get("usage", {})
        self.stats.cache_read_input_tokens += usage.get("cacheReadInputTokens", 0)
        self.stats.cache_written_input_tokens += usage.get("cacheWriteInputTokens", 0)
        self.stats.new_input_tokens += usage.get("inputTokens", 0)
        self.stats.output_tokens += usage.get("outputTokens", 0)

        stop_reason = response.get("stopReason")
        if stop_reason not in {"end_turn", "tool_use"}:
            raise ValueError(f"did not complete, with stop reason: {stop_reason}")

        # There could be multiple content blocks returned (e.g. gpt-oss gives a reasoning block)
        for content in response["output"]["message"]["content"]:
            if "toolUse" in content:
                raw_json = content["toolUse"]["input"]
                # Sometimes (e.g. with claude sonnet 4.5) we get a wrapper field of "parameter"
                # or "$PARAMETER_NAME" :shrug: - for now, just look for those names in particular.
                # If we see a wider variety, we can try to skip any single wrapper field, but I
                # want to keep the option of studies that have only one top level field for now.
                top_keys = set(raw_json)
                if len(top_keys) == 1 and {"parameter", "$PARAMETER_NAME"} & top_keys:
                    raw_json = raw_json.popitem()[1]
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
    # From https://platform.openai.com/docs/api-reference/batch/create
    AZURE_MAX_BATCH_COUNT = 50_000
    AZURE_MAX_BATCH_BYTES = 200_000_000  # 200 MB

    def __init__(
        self,
        model_name: str,
        client: openai.AsyncOpenAI,
        *,
        max_batch_count: int | None,
        supports_schema: bool,
        deployment: str | None = None,
    ):
        super().__init__()
        self.model_name = model_name
        self.client = client
        self.supports_batches = max_batch_count is not None
        self.max_batch_count = self.AZURE_MAX_BATCH_COUNT
        if self.supports_batches:
            self.max_batch_count = min(self.max_batch_count, max_batch_count)
        self.supports_schema = supports_schema
        self.deployment = deployment or model_name
        self.batch_filename = None

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

    @staticmethod
    def pydantic_to_response_format(schema: type[BaseModel]):
        # Same thing that the openai library does, but done manually here, because the batching
        # code uses this too, and needs to do it manually like this.

        # OpenAI has some extra formatting requirements on schemas, that would be difficult
        # to reproduce, but they don't expose those very cleanly. (We want its internal helper
        # method `to_strict_json_schema()`, but best I see exposed is `pydantic_function_tool`,
        # which has what we want inside it.)
        formatted_schema = openai.pydantic_function_tool(schema)["function"]["parameters"]

        return {
            "type": "json_schema",
            "json_schema": {
                "schema": formatted_schema,
                "name": schema.__name__,
                "strict": True,
            },
        }

    def _prompt_args(self, system: str, user: str, schema: type[BaseModel]) -> dict:
        if self.supports_schema:
            response_format = self.pydantic_to_response_format(schema)
        else:
            response_format = {"type": "json_object"}

        return {
            "model": self.deployment,
            "messages": [
                {"role": "system", "content": system},
                {"role": "user", "content": user},
            ],
            "seed": 12345,  # arbitrary, just specifying it for reproducibility
            "temperature": 0,  # minimize temp, also for reproducibility
            "timeout": 120,  # in seconds
            "response_format": response_format,
        }

    def _process_completion_result(
        self, response: ParsedChatCompletion, schema: type[BaseModel], batched: bool = False
    ) -> PromptResponse:
        if response.usage:
            cached_tokens = 0
            if response.usage.prompt_tokens_details:
                cached_tokens = response.usage.prompt_tokens_details.cached_tokens or 0
            self.stats.cache_read_input_tokens += cached_tokens
            self.stats.new_input_tokens += response.usage.prompt_tokens - cached_tokens
            self.stats.output_tokens += response.usage.completion_tokens

        choice = response.choices[0]
        if self.supports_schema and choice.message.parsed:
            parsed = choice.message.parsed
        else:
            parsed = schema.model_validate_json(choice.message.content)

        if choice.finish_reason != "stop":
            raise ValueError(f"did not complete, with finish reason: {choice.finish_reason}")

        return PromptResponse(
            answer=parsed,
            fingerprint=response.system_fingerprint,
        )

    async def prompt(self, system: str, user: str, schema: type[BaseModel]) -> PromptResponse:
        prompt_args = self._prompt_args(system, user, schema)
        response = await self.client.chat.completions.parse(**prompt_args)
        return self._process_completion_result(response, schema)

    async def add_to_batch(self, prompt: Prompt) -> str | None:
        batch_id = None

        # Check if we can early exit, because it's in cache already
        cached = nlp.cache_read(prompt.cache_dir, prompt.cache_namespace, prompt.cache_checksum)
        if cached is not None:
            return None

        # Format new row
        row = {
            "custom_id": prompt.cache_checksum,
            "method": "POST",
            "url": "/v1/chat/completions",
            "body": self._prompt_args(prompt.system, prompt.user, prompt.schema),
        }
        row = json.dumps(row) + "\n"

        # Confirm we won't go past the size limit if we add this new row; send current batch if so
        if self.batch_filename:
            new_size = os.path.getsize(self.batch_filename) + len(row)
            new_lines = common.read_local_line_count(self.batch_filename) + 1
            if new_size > self.AZURE_MAX_BATCH_BYTES or new_lines > self.AZURE_MAX_BATCH_COUNT:
                # New row wouldn't fit, send old one first
                batch_id = await self._send_batch(prompt.cache_dir, prompt.cache_namespace)

        # Are we continuing a WIP file, or do we need to create a new one?
        if self.batch_filename:
            tmp_file = open(self.batch_filename, "a", encoding="utf8")
        else:
            tmp_dir = common.get_temp_dir("nlp-batches")
            tmp_file = tempfile.NamedTemporaryFile("wt", dir=tmp_dir, delete=False, suffix=".jsonl")
            self.batch_filename = tmp_file.name

        # Add our new row
        with tmp_file:
            tmp_file.write(row)

        return batch_id

    async def _send_batch(self, cache_dir: str, cache_namespace: str) -> str:
        prompt_file = await self.client.files.create(
            file=pathlib.Path(self.batch_filename),
            purpose="batch",
        )

        batch = await self.client.batches.create(
            completion_window="24h",
            endpoint="/v1/chat/completions",
            input_file_id=prompt_file.id,
        )

        os.unlink(self.batch_filename)
        self.batch_filename = None

        # Save this batch ID for resuming later
        metadata = nlp.cache_metadata_read(cache_dir, cache_namespace)
        batch_key = f"batches-{_provider_name}"
        metadata[batch_key] = [*metadata.get(batch_key, []), batch.id]
        nlp.cache_metadata_write(cache_dir, cache_namespace, metadata)

        return batch.id

    async def finish_batch(self, cache_dir: str, cache_namespace: str) -> str | None:
        if self.batch_filename:
            return await self._send_batch(cache_dir, cache_namespace)
        return None

    async def wait_for_batch(
        self, batch_id: str, *, schema: type[BaseModel], cache_dir: str, cache_namespace: str
    ) -> None:
        # Poll the batches until completion
        batch = await self.client.batches.retrieve(batch_id=batch_id)
        # You can see the list of valid statuses here:
        # https://platform.openai.com/docs/guides/batch/2-uploading-your-batch-input-file
        while batch.status in {"validating", "in_progress", "finalizing"}:
            await asyncio.sleep(60 * 5)  # check every five minutes
            batch = await self.client.batches.retrieve(batch_id=batch_id)

        if batch.status != "completed":
            logging.warning(f"Batch did not complete, got status: '{batch.status}'")
            # Don't exit function - process the errors and output that we do have, and remove the
            # batch from our metadata cache like normal

        # Check for errors
        if batch.error_file_id:
            error_file = await self.client.files.content(file_id=batch.error_file_id)
            async for line in await error_file.aiter_lines():
                try:
                    line = json.loads(line)
                    # Yes, error.message.error.message is really where this is kept.
                    msg = line.get("error", {}).get("message", {}).get("error", {}).get("message")
                    if msg:
                        logging.warning(f"Error from NLP: {msg}")
                except json.JSONDecodeError:
                    logging.warning(f"Could not process error message: '{line}'")

        # Get the results!
        if batch.output_file_id:
            contents = await self.client.files.content(file_id=batch.output_file_id)

            async for line in await contents.aiter_lines():
                line = json.loads(line)
                # If an error happens, it seems to come in via the error_file_id above.
                # But just for safety, we'll also check here.
                if line.get("error") and (error := line["error"].get("message")):
                    logging.warning(f"Error from NLP: {error}")
                    continue
                status = line.get("response", {}).get("status_code", 200)
                if status >= 300:
                    logging.warning(f"Unexpected status code from NLP: {status}")
                    continue
                checksum = line.get("custom_id")
                body = line.get("response", {}).get("body")
                if not body or not checksum:
                    logging.warning("Unexpected response from NLP: missing data")
                    continue

                # Write each valid response to cache
                result = ParsedChatCompletion.model_validate(body)
                response = self._process_completion_result(result, schema)
                nlp.cache_write(
                    cache_dir, cache_namespace, checksum, json.dumps(response.to_dict())
                )

        # We could clean up files here, but the batch, input file, and output file are cleaned
        # automatically after 30 days, so let's not bother deleting them ourselves, in case we need
        # to do some manual recovery.

        # Drop this batch ID from resume list
        metadata = nlp.cache_metadata_read(cache_dir, cache_namespace)
        batch_key = f"batches-{_provider_name}"
        batch_ids = metadata.get(batch_key, [])
        if batch.id in batch_ids:
            batch_ids.remove(batch.id)
        metadata[batch_key] = batch_ids
        nlp.cache_metadata_write(cache_dir, cache_namespace, metadata)


class AzureProvider(OpenAIProvider):
    def __init__(self, model_name: str, **kwargs):
        super().__init__(
            model_name,
            openai.AsyncAzureOpenAI(api_version="2024-10-21"),
            deployment=_azure_deployment,
            **kwargs,
        )


class VllmProvider(OpenAIProvider):
    def __init__(self, compose_id, model_name: str, env_stem: str, port: int):
        url = os.environ.get(f"CUMULUS_{env_stem}_URL")  # set by compose.yaml
        url = url or f"http://localhost:{port}/v1"  # offer non-docker fallback
        super().__init__(
            model_name,
            openai.AsyncOpenAI(base_url=url, api_key=""),
            supports_schema=True,
            max_batch_count=None,
        )
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
    ##################################
    # Fields for subclasses to fill in
    ##################################

    AZURE_ID = None  # model name in MS Azure
    AZURE_PRICES: TokenPrices | None = None
    AZURE_BATCHES = True  # turns on batch support
    AZURE_SCHEMA = True  # turns on JSON schema support

    BEDROCK_ID = None  # model name in AWS Bedrock
    BEDROCK_PRICES: TokenPrices | None = None
    BEDROCK_CACHE = True  # turns on caching
    BEDROCK_SCHEMA = True  # turns on JSON schema support

    COMPOSE_ID = None  # docker service name in compose.yaml
    VLLM_INFO = None  # tuple of vLLM model name, env var stem use for URL, plus default port

    #########################################
    # End of fields for subclasses to fill in
    #########################################

    AZURE_ENV = ("AZURE_OPENAI_API_KEY", "AZURE_OPENAI_ENDPOINT")

    @staticmethod
    def _env_defined(env_keys: Iterable[str]) -> bool:
        return all(os.environ.get(key) for key in env_keys)

    def raise_unsupported_error(self) -> NoReturn:
        errors.fatal(
            f"{self.__class__.__name__} does not support the '{_provider_name}' provider.",
            errors.ARGS_INVALID,
        )

    def __init__(self, *, max_batch_count: int | None = None):
        self.prices = None
        self.max_batch_count = max_batch_count

        if _provider_name == "azure":
            if not self.AZURE_ID:
                self.raise_unsupported_error()
            if not self._env_defined(self.AZURE_ENV):
                errors.fatal(
                    "Missing Azure environment variables. "
                    "Set both AZURE_OPENAI_API_KEY & AZURE_OPENAI_ENDPOINT.",
                    errors.ARGS_INVALID,
                )
            self.provider = AzureProvider(
                self.AZURE_ID,
                max_batch_count=self.max_batch_count if self.AZURE_BATCHES else None,
                supports_schema=self.AZURE_SCHEMA,
            )
            self.prices = self.AZURE_PRICES

        elif _provider_name == "bedrock":
            if not self.BEDROCK_ID:
                self.raise_unsupported_error()
            self.provider = BedrockProvider(
                self.BEDROCK_ID,
                supports_cache=self.BEDROCK_CACHE,
                supports_schema=self.BEDROCK_SCHEMA,
            )
            self.prices = self.BEDROCK_PRICES

        else:
            if not self.COMPOSE_ID:
                self.raise_unsupported_error()
            self.provider = VllmProvider(
                self.COMPOSE_ID, self.VLLM_INFO[0], self.VLLM_INFO[1], self.VLLM_INFO[2]
            )

        if self.provider.supports_batches and _use_batch_mode:
            # Currently, both Azure and Bedrock charge 50% for batch mode.
            self.prices.multiplier = 0.5

    @property
    def stats(self) -> TokenStats:
        return self.provider.stats

    # override to add your own checks
    async def post_init_check(self) -> None:
        await self.provider.post_init_check()

    async def prompt(self, prompt: Prompt) -> PromptResponse:
        return await nlp.cache_wrapper(
            prompt.cache_dir,
            prompt.cache_namespace,
            prompt.cache_checksum,
            lambda x: PromptResponse.from_dict(json.loads(x), prompt.schema),  # from file
            lambda x: json.dumps(x.to_dict()),  # to file
            self.provider.prompt,
            prompt.system,
            prompt.user,
            prompt.schema,
        )

    async def prompt_via_batches_if_possible(
        self,
        *,
        schema: type[BaseModel],
        cache_dir: str,
        cache_namespace: str,
        prompt_iter: AsyncIterator[Prompt],
    ) -> None:
        """Will resume any previous batches, run new notes via batches, and save all in cache"""
        if not _use_batch_mode:
            return
        if not self.provider.supports_batches:
            errors.fatal(
                f"Model {self.provider.model_name} does not support batching.",
                errors.NLP_BATCHING_UNSUPPORTED,
            )

        # First, resume any previously started batches
        metadata = nlp.cache_metadata_read(cache_dir, cache_namespace)
        batch_ids = metadata.get(f"batches-{_provider_name}")
        if batch_ids:
            rich.print(" Resuming previously created batches.")
            await self._wait_for_batches(
                batch_ids, schema=schema, cache_dir=cache_dir, cache_namespace=cache_namespace
            )

        # Then, regardless of whether there were existing ones, make new batches for any new
        # notes we now see.
        batch_ids = set()
        async for prompt in prompt_iter:
            batch_ids.add(await self.provider.add_to_batch(prompt))
        batch_ids.add(await self.provider.finish_batch(cache_dir, cache_namespace))
        batch_ids.discard(None)  # drop any None's from calls that didn't create a batch
        if batch_ids:
            rich.print(" Waiting for batches to finish (can be resumed if interrupted).")
            await self._wait_for_batches(
                batch_ids,
                cache_dir=cache_dir,
                cache_namespace=cache_namespace,
                schema=schema,
            )

    async def _wait_for_batches(
        self, batch_ids: set[str], *, schema: type[BaseModel], cache_dir: str, cache_namespace: str
    ) -> None:
        status_box = rich.text.Text()

        count = len(batch_ids)

        def update_text() -> None:
            plural = "" if count == 1 else "es"
            status_box.plain = (
                f"Waiting for {count} batch{plural} to finish… (may take up to a day)"
            )

        async def run_batch(batch_id: str) -> None:
            nonlocal count

            await self.provider.wait_for_batch(
                batch_id,
                schema=schema,
                cache_dir=cache_dir,
                cache_namespace=cache_namespace,
            )

            count -= 1
            update_text()

        with rich.get_console().status(status_box):
            update_text()
            coroutines = [run_batch(batch_id) for batch_id in batch_ids]
            await asyncio.gather(*coroutines)

        count = len(batch_ids)
        batch_plural = "" if count == 1 else "es"
        rich.print(f" Waited for {count} batch{batch_plural}.")


class Gpt35Model(Model):  # deprecated, do not use in new code (doesn't support many features)
    AZURE_ID = "gpt-35-turbo-0125"
    AZURE_PRICES = TokenPrices(
        # https://azure.microsoft.com/en-us/pricing/details/cognitive-services/openai-service/
        # "gpt-35-turbo-0125"
        date=datetime.date(2025, 10, 15),
        new_input_tokens=0.00055,
        output_tokens=0.00165,
    )
    AZURE_BATCHES = False
    AZURE_SCHEMA = False


class Gpt4Model(Model):
    AZURE_ID = "gpt-4"
    AZURE_PRICES = TokenPrices(
        # https://azure.microsoft.com/en-us/pricing/details/cognitive-services/openai-service/
        # "GPT-4" with 32K context
        date=datetime.date(2025, 10, 15),
        new_input_tokens=0.06,
        output_tokens=0.12,
    )
    AZURE_BATCHES = False


class Gpt4oModel(Model):
    AZURE_ID = "gpt-4o"
    AZURE_PRICES = TokenPrices(
        # https://azure.microsoft.com/en-us/pricing/details/cognitive-services/openai-service/
        # "GPT-4o-2024-1120 Global"
        date=datetime.date(2025, 10, 15),
        new_input_tokens=0.0025,
        cache_read_input_tokens=0.00125,
        output_tokens=0.01,
    )


class Gpt5Model(Model):
    AZURE_ID = "gpt-5"
    AZURE_PRICES = TokenPrices(
        # https://azure.microsoft.com/en-us/pricing/details/cognitive-services/openai-service/
        # "GPT-5 2025-08-07 Global"
        date=datetime.date(2025, 10, 15),
        new_input_tokens=0.00125,
        cache_read_input_tokens=0.00013,
        output_tokens=0.01,
    )
    AZURE_BATCHES = False


class GptOss120bModel(Model):
    AZURE_ID = "gpt-oss-120b"
    AZURE_BATCHES = False
    # AZURE_PRICES = TokenPrices(...)  # TODO: find online
    BEDROCK_ID = "openai.gpt-oss-120b-1:0"
    BEDROCK_CACHE = False
    BEDROCK_PRICES = TokenPrices(
        # https://aws.amazon.com/bedrock/pricing/ for us-east-1
        date=datetime.date(2025, 10, 15),
        new_input_tokens=0.00015,
        output_tokens=0.0006,
    )
    COMPOSE_ID = "gpt-oss-120b"
    VLLM_INFO = ("openai/gpt-oss-120b", "GPT_OSS_120B", 8086)


class Llama4ScoutModel(Model):
    AZURE_ID = "Llama-4-Scout-17B-16E-Instruct"
    AZURE_BATCHES = False
    # AZURE_PRICES = TokenPrices(...)  # TODO: find online
    BEDROCK_ID = "us.meta.llama4-scout-17b-instruct-v1:0"
    BEDROCK_CACHE = False
    BEDROCK_SCHEMA = False
    BEDROCK_PRICES = TokenPrices(
        # https://aws.amazon.com/bedrock/pricing/ for us-east-1
        date=datetime.date(2025, 10, 15),
        new_input_tokens=0.00017,
        output_tokens=0.00066,
    )
    COMPOSE_ID = "llama4-scout"
    VLLM_INFO = ("nvidia/Llama-4-Scout-17B-16E-Instruct-FP4", "LLAMA4_SCOUT", 8087)


class ClaudeSonnet45Model(Model):
    BEDROCK_ID = "us.anthropic.claude-sonnet-4-5-20250929-v1:0"
    BEDROCK_PRICES = TokenPrices(
        # https://aws.amazon.com/bedrock/pricing/ for us-east-1
        date=datetime.date(2025, 10, 15),
        new_input_tokens=0.0033,
        cache_read_input_tokens=0.00033,
        cache_written_input_tokens=0.004125,
        output_tokens=0.0165,
    )
