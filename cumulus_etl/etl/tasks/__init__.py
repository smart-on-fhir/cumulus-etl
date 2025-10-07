"""Task support for the ETL workflow"""

from .base import EntryIterator, EtlTask, OutputTable
from .nlp_task import BaseModelTask, BaseModelTaskWithSpans, BaseNlpTask
