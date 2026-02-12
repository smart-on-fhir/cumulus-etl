import contextlib
import threading

import rich.progress

from cumulus_etl import common

TaskID = rich.progress.TaskID


class Progress(rich.progress.Progress):
    def __init__(self):
        console = rich.get_console()

        columns = [
            rich.progress.TextColumn("[progress.description]{task.description}"),
            rich.progress.BarColumn(),
            rich.progress.TaskProgressColumn(),
            rich.progress.TimeElapsedColumn(),
        ]

        self._thread = None

        super().__init__(*columns, console=console)

    @contextlib.contextmanager
    def show_indeterminate_task(self, description: str):
        task = self.add_task(description, total=None)
        yield
        self.update(task, completed=1, total=1)

    def add_task(self, description: str, *args, **kwargs) -> TaskID:
        task_id = super().add_task(description, *args, **kwargs)

        self._stop_thread()
        if not self.console.is_interactive:
            self._thread = _RefreshThread(progress=self, task_id=task_id)
            self._thread.start()

        return task_id

    def update(self, task_id: TaskID, *args, completed=None, total=None, **kwargs) -> None:
        super().update(task_id, *args, total=total, completed=completed, **kwargs)

        if self._thread and task_id == self._thread.task_id and completed == total:
            self._stop_thread()

    def stop(self, *args, **kwargs) -> None:
        super().stop(*args, **kwargs)
        self._stop_thread()

    def _stop_thread(self) -> None:
        if self._thread:
            self._thread.stop()
            self._thread = None


class _RefreshThread(threading.Thread):
    def __init__(self, progress: Progress, task_id: TaskID) -> None:
        self.progress = progress
        self.task_id = task_id
        self.done = threading.Event()
        super().__init__(daemon=True)

    def _find_task(self) -> rich.progress.Task | None:
        for task in self.progress.tasks:
            if task.id == self.task_id:
                return task
        return None  # pragma: no cover

    def stop(self) -> None:
        self.done.set()

    def _print(self) -> None:
        task = self._find_task()
        if not task:
            return  # pragma: no cover

        elapsed = common.human_time_offset(int(task.elapsed))
        if elapsed == "0s":  # haven't really started, don't need time/percent update
            rich.print(f"{task.description}…")
        elif task.total:  # bar is slowly filling up, we can do percentages
            rich.print(f"{task.description}… ({task.percentage:.0f}%, {elapsed} so far)")
        else:  # indeterminate bar
            rich.print(f"{task.description}… ({elapsed} so far)")

    def _delay(self) -> int:
        task = self._find_task()
        if not task or task.elapsed < 60 * 60:
            return 60
        else:
            # after an hour, only print every two minutes
            return 120  # pragma: no cover

    def run(self) -> None:
        self._print()
        while not self.done.wait(self._delay()):
            if self.done.is_set():
                break  # pragma: no cover
            self._print()
