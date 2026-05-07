"""Support code for NLP servers"""

from .extract import TransformerModel, ctakes_extract, ctakes_httpx_client, list_polarity
from .models import (
    ClaudeSonnet45Model,
    Gpt4Model,
    Gpt4oModel,
    Gpt51Model,
    Gpt54Model,
    Gpt54MiniModel,
    Gpt54NanoModel,
    Gpt35Model,
    GptOss120bModel,
    Llama4ScoutModel,
    Model,
    Prompt,
    TokenStats,
    set_nlp_config,
)
from .selection import (
    add_note_selection,
    get_note_filter,
    get_refs_from_csv,
    query_athena_table,
)
from .utils import (
    cache_checksum,
    cache_metadata_read,
    cache_metadata_write,
    cache_read,
    cache_wrapper,
    cache_write,
    get_note_date,
    get_note_encounter_id,
    get_note_info,
    get_note_subject_ref,
)
from .watcher import (
    check_ctakes,
    check_negation_cnlpt,
    check_term_exists_cnlpt,
    restart_ctakes_with_bsv,
)

ALL_MODELS = [
    ClaudeSonnet45Model,
    Gpt4Model,
    Gpt4oModel,
    Gpt51Model,
    Gpt54Model,
    Gpt54MiniModel,
    Gpt54NanoModel,
    Gpt35Model,
    GptOss120bModel,
    Llama4ScoutModel,
]
