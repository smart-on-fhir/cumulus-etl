"""Iteration utilities to support batching values."""

from collections.abc import AsyncIterable, AsyncIterator, Iterable
from typing import TypeVar

Item = TypeVar("Item")
Atom = Item | list[Item] | None  # one single item (or group of items that can't be broken up)
AtomStreams = Atom | tuple[Atom, ...]  # separated streams of atoms (which should be kept separated)
ItemBatch = list[Item]
ItemBatchStreams = tuple[ItemBatch, ...]  # separated lists of items


async def _batch_slice(iterable: AsyncIterable[AtomStreams], n: int) -> ItemBatchStreams | None:
    """
    Returns the first n elements of iterable, flattening lists.

    But include an entire list if we would end in middle.
    For example, list(_batch_slice([1, [2.1, 2.1], 3], 2)) returns [1, 2.1, 2.2]

    Note that this will only flatten elements that are actual Python lists (isinstance list is True)
    """
    count = 0
    slices = None

    async for atom_streams in iterable:
        if not isinstance(atom_streams, tuple):
            atom_streams = (atom_streams,)  # ensure that we are always dealing with a streams tuple

        if slices is None:  # Create a tuple of empty lists that we will fill with slice data
            slices = tuple([] for _ in range(len(atom_streams)))

        for index, atom in enumerate(atom_streams):
            if isinstance(atom, list):
                slices[index].extend(atom)
                count += len(atom)
            elif atom is not None:
                slices[index].append(atom)
                count += 1

        if count >= n:
            break

    return slices


async def _force_aiter(iterable: AsyncIterable[Item] | Iterable[Item]) -> AsyncIterator[Item]:
    """A version of aiter() that also promotes sync iterables into async iterables"""
    if hasattr(iterable, "__aiter__"):
        async for item in iterable:
            yield item
    else:
        for item in iterable:
            yield item


async def batch_iterate_streams(
    iterable: AsyncIterable[AtomStreams] | Iterable[AtomStreams], size: int
) -> AsyncIterator[ItemBatchStreams]:
    """
    Yields batches of items, each roughly {size} elements from iterable.

    This is similar to itertools.batched() from Python 3.12, with a few major changes:
    - this is fully async, accepting async (or sync) iteratables, and returning an async iterable
    - lists are returned, rather than tuples
    - batches might be larger than the batch size, if a list is encountered inside the source
      iterable. In that case, all elements of the list are included in the same sub-iterator batch,
      which might put us over size. See the comments for the EtlTask.group_field class attribute
      for why we support this.
    - this accepts an iterator of tuples, where all the items in the tuple count against the batch
      size and lists inside the tuple get expanded like above. This allows batching parallel streams
      of data, like we do in ETL tasks that generate multiple output tables (see MedicationRequest).
      All elements in a tuple will be in the same batch. The return value is always given in tuple
      form, even if the input was not tupled.

    The whole iterable is never fully loaded into memory. Rather we load only one element at a time.

    Example of sub-lists:
        async for batch in _batch_iterate_streams([1, [2.1, 2.2], 3, 4, 5], 2):
            print(batch)
        # ([1, 2.1, 2.2],)
        # ([3, 4],)
        # ([5],)

    Example of multiple streams, tupled together:
        items = [(1, "a"), ([2.1, 2.2], "b"), (None, ["c", "d"])]
        async for batch in _batch_iterate_streams(items, 3):
            print(batch)
        # ([1, 2.1, 2.2], ["a", "b"]),
        # ([], ["c", "d"]),
    """
    if size < 1:
        raise ValueError("Must iterate by at least a batch of 1")

    # get a real once-through iterable (we want to iterate only once)
    true_iterable = _force_aiter(iterable)
    while batches := await _batch_slice(true_iterable, size):
        yield batches


async def batch_iterate(
    iterable: AsyncIterable[Atom] | Iterable[Atom], size: int
) -> AsyncIterator[ItemBatch]:
    """
    A version of batch_iterate_streams that simplifies the common "one stream" case.

    That is, this does not support accepting an iterator of tuples.

    Read its documentation for the full explanation.

    Example of sub-lists:
        async for batch in _batch_iterate([1, [2.1, 2.2], 3, 4, 5], 2):
            print(batch)
        # [1, 2.1, 2.2]
        # [3, 4]
        # [5]
    """
    async for batch in batch_iterate_streams(iterable, size):
        yield batch[0]
