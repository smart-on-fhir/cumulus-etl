"""
Helpers for implementing completion-tracking.

Completion tracking allows downstream consumers to know when ETL runs are
"complete enough" for their purposes.

For example, the `core` study may want to not expose Encounters whose
Conditions have not yet been loaded. These metadata tables allow that.

Although these metadata tables aren't themselves tasks, they need a
lot of the same information that tasks need. This module provides that.
"""

from .schema import (
    COMPLETION_ENCOUNTERS_TABLE,
    COMPLETION_TABLE,
    completion_encounters_output_args,
    completion_encounters_schema,
    completion_format_args,
    completion_schema,
)
