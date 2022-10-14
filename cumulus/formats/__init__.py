"""Classes that know _how_ to write out results to the target folder"""

from .json_tree import JsonTreeFormat
from .ndjson import NdjsonFormat
from .parquet import ParquetFormat
