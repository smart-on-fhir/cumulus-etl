"""Classes that know _how_ to write out results to the target folder"""

from .base import Format
from .batch import Batch
from .factory import get_format_class
