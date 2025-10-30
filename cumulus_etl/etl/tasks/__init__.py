"""Task support for the ETL workflow"""

from .base import EntryBundle, EntryIterator, EtlTask, OutputTable
from .nlp_task import BaseModelTask, BaseModelTaskWithSpans, BaseNlpTask, NoteDetails
