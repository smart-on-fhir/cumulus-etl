"""Abstraction layer for Hugging Face's inference API"""

import os
import urllib.parse

import httpx

from cumulus_etl import http


def get_hugging_face_url() -> str:
    # 8000 and 8080 are both used as defaults in ctakesclient (cnlp & ctakes respectively). So let's use 86 for H.F.
    return os.environ.get("CUMULUS_HUGGING_FACE_URL") or "http://localhost:8086/"


async def llama2_prompt(
    system_prompt: str, user_prompt: str, *, client: httpx.AsyncClient = None
) -> str:
    """
    Prompts a llama2 chat model and provides its response.

    :param system_prompt: this is the instruction to the model ("You are a ...")
    :param user_prompt: this is the question / input ("How big is the sun?")
    :param client: the httpx client to use (optional)
    :returns: the llama2 response
    """
    # From Llama2 docs (https://huggingface.co/meta-llama/Llama-2-7b-hf):
    #   To get the expected features and performance for the chat versions, a specific formatting needs to be followed,
    #   including the INST and <<SYS>> tags, BOS and EOS tokens, and the whitespaces and breaklines in between
    #   (we recommend calling strip() on inputs to avoid double-spaces). See our reference code in github for details:
    #   https://github.com/facebookresearch/llama/blob/main/llama/generation.py#L212.
    #
    # Also see https://huggingface.co/blog/llama2#how-to-prompt-llama-2
    whole_prompt = f"""<s>[INST] <<SYS>>
{system_prompt.strip()}
<</SYS>>

{user_prompt.strip()} [/INST]"""

    response = await hf_prompt(whole_prompt, client=client)
    text = response[0]["generated_text"]
    # llama2 gives back the prompt too, but we don't need it
    text = text.removeprefix(whole_prompt).strip()
    return text


async def hf_prompt(prompt: str | dict, *, client: httpx.AsyncClient = None) -> list | dict:
    """
    Prompts a Hugging Face model and provides its response.

    :param prompt: this is the raw input to the model
    :param client: the httpx client to use (optional)
    :returns: the raw model response
    """
    headers = {}
    # Check for a token using the same env name as the huggingface/text-generation-inference docker
    # TODO: disabled for now to prevent accidentally sending PHI - do we even want the ability to hit the cloud?
    # if hf_token := os.environ.get("HUGGING_FACE_HUB_TOKEN"):
    #     headers["Authorization"] = f"Bearer {hf_token}"

    # Prompt and answer format will be model-specific - don't assume too much here
    client = client or httpx.AsyncClient()
    response = await http.request(
        client,
        "POST",
        get_hugging_face_url(),
        headers=headers,
        json={
            "inputs": prompt,
            "options": {
                "wait_for_model": True,
            },
            "parameters": {
                # Maybe max_new_tokens should be configurable, but let's hope a universal value is fine for now.
                # When bumping this, consider whether you should bump the task version of any tasks that call this.
                "max_new_tokens": 1000,
            },
        },
    )
    return response.json()


async def hf_info(*, client: httpx.AsyncClient = None) -> dict:
    """
    Returns a Hugging Face model info dictionary.

    This is suitable for logging along with generated results, to identify provenance.
    """
    url = urllib.parse.urljoin(get_hugging_face_url(), "info")
    client = client or httpx.AsyncClient()
    response = await http.request(client, "GET", url)
    return response.json()
