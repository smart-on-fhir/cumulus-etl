import logging
import os
import time
from datetime import UTC, datetime

import mlflow


class MlflowTrackingMixin:
    """
    Mixin that adds MLflow experiment tracking to any BaseModelTask subclass.

    Lifecycle
    ---------
    - __init__:     initializes the predictions accumulator and enables openai autologging
                    NOTE: THIS LOGS PHI TO MLFLOW.
    - finish_task:  logs all params, metrics, and artifacts in a single run

    What is logged
    --------------
    Parameters
    - task_version, model_id, response_schema (JSON), system_prompt,
      runtime.start, runtime.end  (human-readable form of the epoch metrics)

    Metrics
    - notes.{seen, considered, with_text, with_results, yield_rate},
      tokens.{new_input, cache_read, cache_written, output, total, cache_hit_rate},
      cost.estimated_usd  (only when model.prices is set), and
      runtime.{start, end, time_taken_seconds, time_taken_per_note_seconds, tokens_per_second}

    Artifacts
    - prompts/system_prompt.txt
    - prompts/user_prompt.txt

    Traces
    - All OpenAI API calls are automatically captured by mlflow.openai.autolog() and
      attached to the run.
    """

    @property
    def mlflow_run_name(self) -> str:
        return self._mlflow_run_name or f"{self.name}_{self.task_version}"

    #########
    # Task-specific overrides
    #
    def __init__(self, *args, mlflow_run_name: str | None = None, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self._mlflow_run_name = mlflow_run_name
        self._mlflow_start_time: float = time.time()
        # Patch the OpenAI client globally — traces are captured into whatever
        # run is active at call time, so the run must be started before any
        # OpenAI calls happen (see prepare_task).
        mlflow.openai.autolog()

    async def prepare_task(self) -> bool:
        result = await super().prepare_task()
        if not result:
            return result

        mlflow_tracking_uri = os.environ.get("MLFLOW_TRACKING_URI")
        if mlflow_tracking_uri:
            mlflow.set_tracking_uri(mlflow_tracking_uri)
            mlflow.set_experiment(self.mlflow_experiment_name)
            mlflow.start_run(run_name=self.mlflow_run_name)

        return result

    def finish_task(self) -> None:
        """
        Finishes the task per-normal, then logs an MLflow run with all params, metrics, and artifacts.
        """
        super().finish_task()  # prints existing rich tables

        try:
            self._log_to_mlflow()
        except Exception as exc:
            logging.warning("MLflow logging failed (non-fatal): %s", exc, exc_info=True)

    def _log_to_mlflow(self) -> None:
        if not mlflow.active_run():
            logging.warning(
                "Missing MLFlow environment variables. "
                "Set MLFLOW_TRACKING_URI to track experiments. "
                "Skipping MLflow logging for this run."
            )
            return

        self._mlflow_end_time: float = time.time()

        try:
            self._log_params()
            self._log_metrics()
            self._log_artifacts()
        finally:
            mlflow.end_run()

    def _log_params(self) -> None:
        mlflow.log_params(
            {
                "task": self.name,
                "task_version": self.task_version,
                "model_id": self.client_class.CONFIG_ID,
                "mlflow_run_name": self.mlflow_run_name,
                "system_prompt": self.get_system_prompt(),
                "response_schema": self.response_format.model_json_schema(),
                "runtime.start": datetime.fromtimestamp(
                    self._mlflow_start_time, tz=UTC
                ).isoformat(),
                "runtime.end": datetime.fromtimestamp(self._mlflow_end_time, tz=UTC).isoformat(),
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
        total_tokens = stats.new_input_tokens + stats.cache_read_input_tokens + stats.output_tokens
        total_input_tokens = stats.new_input_tokens + stats.cache_read_input_tokens
        cache_hit_rate = (
            stats.cache_read_input_tokens / total_input_tokens if total_input_tokens else 0.0
        )
        mlflow.log_metrics(
            {
                "tokens.new_input": stats.new_input_tokens,
                "tokens.cache_read": stats.cache_read_input_tokens,
                "tokens.cache_written": stats.cache_written_input_tokens,
                "tokens.output": stats.output_tokens,
                "tokens.total": total_tokens,
                "tokens.cache_hit_rate": round(cache_hit_rate, 4),
            }
        )

        # --- Runtime ---
        elapsed = self._mlflow_end_time - self._mlflow_start_time
        mlflow.log_metrics(
            {
                "runtime.time_taken_seconds": round(elapsed, 3),
                "runtime.time_taken_per_note_seconds": round(elapsed / ns.with_results, 3)
                if ns.with_results
                else 0.0,
                "runtime.tokens_per_second": round(total_tokens / elapsed, 1) if elapsed else 0.0,
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
