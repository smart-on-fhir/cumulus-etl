import asyncio
from collections.abc import Awaitable, Callable, Iterable
from typing import TypeVar

from cumulus_etl import errors

Item = TypeVar("Item")


def _drain_queue(queue: asyncio.Queue) -> None:
    """
    Used to empty the queue if we are early exiting.

    Once we depend on Python 3.13, we can just use Queue.shutdown() instead.
    """
    while not queue.empty():
        queue.get_nowait()
        queue.task_done()


async def _worker(
    queue: asyncio.Queue, shutdown: asyncio.Event, processor: Callable[[int, Item], Awaitable[None]]
) -> None:
    while True:
        index, item = await queue.get()
        try:
            await processor(index, item)
        except Exception:
            shutdown.set()  # flag other tasks to stop
            raise
        finally:
            queue.task_done()
            if shutdown.is_set():
                _drain_queue(queue)


async def _reader(queue: asyncio.Queue, shutdown: asyncio.Event, iterable: Iterable[Item]) -> None:
    for index, item in enumerate(iterable):
        await queue.put((index, item))
        if shutdown.is_set():
            _drain_queue(queue)
            break


async def peek_ahead_processor(
    iterable: Iterable[Item],
    processor: Callable[[int, Item], Awaitable[None]],
    *,
    peek_at: int,
) -> None:
    """Processes items in sequence, but always looks at some in parallel"""
    queue = asyncio.Queue(peek_at)
    shutdown = asyncio.Event()  # poor substitute for Python 3.13's wonderful Queue.shutdown()
    reader = asyncio.create_task(_reader(queue, shutdown, iterable), name="peek-ahead-reader")
    tasks = [
        asyncio.create_task(_worker(queue, shutdown, processor), name=f"peek-ahead-worker-{i}")
        for i in range(peek_at)
    ]

    try:
        await reader  # read all the input, processing as we go
        await queue.join()  # finish up final batch of workers
    finally:
        # Close out the tasks
        for task in tasks:
            task.cancel()

        results = await asyncio.gather(*tasks, return_exceptions=True)

        # Check for an early exit in the tasks
        for result in results:
            if result and not isinstance(result, asyncio.CancelledError):
                errors.fatal(str(result), errors.INLINE_TASK_FAILED)
