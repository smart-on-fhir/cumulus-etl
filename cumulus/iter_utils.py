"""Iteration helpers"""

import asyncio
from typing import Any, AsyncIterable, AsyncIterator, Coroutine, Iterable, List, TypeVar, Union

T = TypeVar("T")


async def _batch_slice(iterable: AsyncIterable[Union[List[T], T]], n: int) -> AsyncIterator[T]:
    """
    Returns the first n elements of iterable, flattening lists, but including an entire list if we would end in middle.

    For example, list(_batch_slice([1, [2.1, 2.1], 3], 2)) returns [1, 2.1, 2.2]

    Note that this will only flatten elements that are actual Python lists (isinstance list is True)
    """
    count = 0
    async for item in iterable:
        if isinstance(item, list):
            for x in item:
                yield x
            count += len(item)
        else:
            yield item
            count += 1

        if count >= n:
            return


async def _async_chain(first: T, rest: AsyncIterator[T]) -> AsyncIterator[T]:
    """An asynchronous version of itertools.chain([first], rest)"""
    yield first
    async for x in rest:
        yield x


async def batch_iterate(iterable: AsyncIterable[Union[List[T], T]], size: int) -> AsyncIterator[AsyncIterator[T]]:
    """
    Yields sub-iterators, each roughly {size} elements from iterable.

    Sub-iterators might be less, if we have reached the end.
    Sub-iterators might be more, if a list is encountered in the source iterable.
    In that case, all elements of the list are included in the same sub-iterator batch, which might put us over size.
    See the comments for the EtlTask.group_field class attribute for why we support this.

    The whole iterable is never fully loaded into memory. Rather we load only one element at a time.

    Example:
        for batch in _batch_iterate([1, [2.1, 2.2], 3, 4, 5], 2):
            print(list(batch))

    Results in:
        [1, 2.1, 2.2]
        [3, 4]
        [5]
    """
    if size < 1:
        raise ValueError("Must iterate by at least a batch of 1")

    # aiter() and anext() were added in python 3.10
    # pylint: disable=unnecessary-dunder-call

    true_iterable = iterable.__aiter__()  # get a real once-through iterable (we want to iterate only once)
    while True:
        iter_slice = _batch_slice(true_iterable, size)
        try:
            peek = await iter_slice.__anext__()
        except StopAsyncIteration:
            return  # we're done!
        yield _async_chain(peek, iter_slice)


async def peek_ahead_iterator(iterable: Iterable[Coroutine[T, Any, Any]], *, peek_at: int) -> AsyncIterator[T]:
    """
    This method will run several coroutines from `iterable` at a time, yielding results as they come in.

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
