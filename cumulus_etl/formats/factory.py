"""Create a Format instance"""

from typing import Type

from .base import Format
from .deltalake import DeltaLakeFormat
from .ndjson import NdjsonFormat
from .parquet import ParquetFormat


def get_format_class(name: str) -> Type[Format]:
    """
    Returns a Format class of the named type for the target output path.
    """
    classes = {
        "deltalake": DeltaLakeFormat,
        "ndjson": NdjsonFormat,
        "parquet": ParquetFormat,
    }
    try:
        return classes[name]
    except KeyError as exc:
        raise ValueError(f"Unknown output format name {name}.") from exc