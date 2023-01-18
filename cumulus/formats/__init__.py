"""Classes that know _how_ to write out results to the target folder"""

from .base import Format
from .deltalake import DeltaLakeFormat
from .ndjson import NdjsonFormat
from .parquet import ParquetFormat
