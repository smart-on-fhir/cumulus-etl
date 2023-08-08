"""Create a Format instance"""

from .base import Format
from .deltalake import DeltaLakeFormat
from .ndjson import NdjsonFormat


def get_format_class(name: str) -> type[Format]:
    """
    Returns a Format class of the named type for the target output path.
    """
    classes = {
        "deltalake": DeltaLakeFormat,
        "ndjson": NdjsonFormat,
    }
    try:
        return classes[name]
    except KeyError as exc:
        raise ValueError(f"Unknown output format name {name}.") from exc
