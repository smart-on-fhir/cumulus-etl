import logging
import os
from collections.abc import Iterable

import mlflow

from cumulus_etl.etl.tasks import base
from cumulus_etl.etl.tasks import nlp_types


class MlflowTrackingMixin:
    """
    Mixin that adds MLflow experiment tracking to any BaseModelTask subclass.

    Lifecycle
    ---------
    - __init__     : initializes the predictions accumulator
    - process_note : Accumulates per-note input/output rows (NOTE: These contain PHI)
    - finish_task  : logs all params, metrics, and artifacts in a single run

    What is logged
    --------------
    Parameters
        task_version, model_id, response_schema (JSON), system_prompt (truncated)

    Metrics
        notes.{seen,considered,with_text,with_results,yield_rate}
        tokens.{new_input,cache_read,cache_written,output,total}
        cost.estimated_usd  (only when model.prices is set)

    Artifacts
        prompts/system_prompt.txt
        prompts/user_prompt.txt
        predictions.json

    """

    # This mixin assumes that the task class it's mixed into has
    # - a `name` attribute defining the task name (including study prefix, the task, the model)
    # - a `task_version` attribute defining the task task_version
    # Combining these should give us a unique identifier for the task run, which we can use as the MLflow experiment name.
    @property
    def mlflow_run(self) -> str:
        return f"{self.name}_{self.task_version}"

    def _make_prediction_row(self, details: nlp_types.NoteDetails, result: dict) -> dict:
        """
        Build a single row for the predictions table.

        Deliberately omits raw note text by default — clinical text must NOT
        leave the PHI-safe environment unless the MLflow server has been
        specifically approved for that data.  Override in a subclass to add
        columns if your tracking environment is PHI-approved.
        """
        return {
            "note_ref": details.note_ref,
            # Serialize just the structured result, not the surrounding metadata
            "result": str(result.get("result", "")),
        }

    #########
    # Task-specific overrides
    #
    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        # Accumulates rows for log_table
        # Initialized here so the list is always safe to append to in
        # process_note regardless of whether a run is active.
        self._mlflow_predictions: dict = {
            "notes": [],
            "responses": [],
            "results": [],
        }

    async def process_note(self, details: nlp_types.NoteDetails) -> base.EntryBundle | None:
        """
        Delegates to the real implementation and records a summary of each note's result.

        Override in a study-specific subclass if you need richer columns —
        for example, adding NLP label counts.  Always call super() first.
        """
        result = await super().process_note(details)

        if result:
            self._mlflow_predictions["notes"].append(details.note_text)
            self._mlflow_predictions["responses"].append(str(result.get("result", "")))
            self._mlflow_predictions["results"].append(result)

        return result

    def finish_task(self) -> None:
        super().finish_task()  # prints existing rich tables

        try:
            self._log_to_mlflow()
        except Exception as exc:
            logging.warning("MLflow logging failed (non-fatal): %s", exc, exc_info=True)

    def _log_to_mlflow(self) -> None:
        mlflow_tracking_uri = os.environ.get("MLFLOW_TRACKING_URI")
        if not mlflow_tracking_uri:
            logging.warning(
                "Missing MLFlow environment variables. "
                "Set MLFLOW_TRACKING_URI to track experiments. "
                "Skipping MLflow logging for this run."
            )
            return
        mlflow.set_tracking_uri(mlflow_tracking_uri)

        mlflow.set_experiment(self.mlflow_experiment_name)

        with mlflow.start_run(run_name=self.mlflow_run):
            self._log_params()
            self._log_metrics()
            self._log_artifacts()

    def _log_params(self) -> None:
        mlflow.log_params(
            {
                "task": self.name,
                "task_version": self.task_version,
                "model_id": self.client_class.CONFIG_ID,
                "system_prompt": self.get_system_prompt(),
                "response_schema": self.response_format.model_json_schema(),
            }
        )

    def _log_metrics(self) -> None:
        # --- Note throughput ---
        ns = self.note_stats
        yield_rate = ns.with_results / ns.with_text if ns.with_text else 0.0
        mlflow.log_metrics(
            {
                "notes.seen": ns.seen,
                "notes.considered": ns.considered,
                "notes.with_text": ns.with_text,
                "notes.with_results": ns.with_results,
                "notes.yield_rate": round(yield_rate, 4),
            }
        )

        # --- Token usage ---
        stats = self.model.stats
        mlflow.log_metrics(
            {
                "tokens.new_input": stats.new_input_tokens,
                "tokens.cache_read": stats.cache_read_input_tokens,
                "tokens.cache_written": stats.cache_written_input_tokens,
                "tokens.output": stats.output_tokens,
                "tokens.total": (
                    stats.new_input_tokens + stats.cache_read_input_tokens + stats.output_tokens
                ),
            }
        )

        # --- Cost estimate (only when the model exposes pricing) ---
        if prices := self.model.prices:
            cost = (
                (
                    stats.new_input_tokens * prices.new_input_tokens
                    + stats.cache_read_input_tokens * prices.cache_read_input_tokens
                    + stats.cache_written_input_tokens * prices.cache_written_input_tokens
                    + stats.output_tokens * prices.output_tokens
                )
                / 1_000
                * prices.multiplier
            )
            mlflow.log_metric("cost.estimated_usd", round(cost, 6))

    def _log_artifacts(self) -> None:
        # Full prompts as text files — easier to diff across runs than params
        mlflow.log_text(self.get_system_prompt(), "prompts/system_prompt.txt")
        if self.user_prompt:
            mlflow.log_text(self.user_prompt, "prompts/user_prompt.txt")

        # Per-note prediction table (opt-in, PHI-safe by default)
        if self._mlflow_predictions:
            mlflow.log_table(
                self._mlflow_predictions,
                artifact_file="predictions.json",
            )
