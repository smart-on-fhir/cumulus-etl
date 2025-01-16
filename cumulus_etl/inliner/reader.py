import asyncio

from cumulus_etl import errors


async def peek_ahead_iterator(iterable, *, peek_at: int):
    """
    This method will run several coroutines from `iterable` at a time.

    Results will be yielded as they come in, out of order but with the original index.

    :param iterable: a sequence of coroutines to run
    :param peek_at: how many to load into memory and run at once
    :returns: the result of each coroutine, in arbitrary order
    """
    coroutine_iter = iter(iterable)
    coroutines_remaining = True
    running_tasks = set()

    while coroutines_remaining or running_tasks:
        # Refill our tasks to wait upon, from the incoming list of coroutines
        if coroutines_remaining:
            try:
                while len(running_tasks) < peek_at:
                    running_tasks.add(asyncio.create_task(next(coroutine_iter)))
            except StopIteration:
                coroutines_remaining = False
                if not running_tasks:
                    break

        # Wait on all running tasks for at least one to finish. Returns two sets of futures.
        done, pending = await asyncio.wait(running_tasks, return_when=asyncio.FIRST_COMPLETED)

        # Start yielding back results as they come in
        for item in done:
            yield item.result()
        running_tasks = pending


async def _worker(worker_id: int, reader, queue: asyncio.Queue, processor):
    while True:
        index, resource = await queue.get()

        try:
            await processor(index, resource)
        except Exception as exc:
            # something went wrong, this is a fatal error, so abort read/write loop
            errors.fatal(exc, errors.INLINE_TASK_FAILED)

        queue.task_done()


async def _reader(queue: asyncio.Queue, iterable):
    for index, item in enumerate(iterable):
        await queue.put((index, item))


async def peek_ahead_processor(iterable, processor, *, peek_at: int):
    queue = asyncio.Queue(peek_at)
    reader = asyncio.create_task(_reader(queue, iterable))
    tasks = [asyncio.create_task(_worker(i, reader, queue, processor)) for i in range(peek_at)]

    try:
        await reader  # read all of the input, processing as we go
        await queue.join()  # finish up final batch of workers
    finally:
        # Close out the tasks
        for task in tasks:
            task.cancel()

    # TODO: return exceptions?
    await asyncio.gather(*tasks, return_exceptions=True)
