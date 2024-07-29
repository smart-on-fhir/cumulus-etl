"""Iteration utilities to support batching values."""

from collections.abc import AsyncIterable, AsyncIterator
from typing import TypeVar

Item = TypeVar("Item")
Atom = Item | list[Item] | None  # one single item (or group of items that can't be broken up)
AtomStreams = Atom | tuple[Atom, ...]  # separated streams of atoms (which should be kept separated)
ItemBatches = tuple[list[Item], ...]  # separated lists of items


async def _batch_slice(iterable: AsyncIterable[AtomStreams], n: int) -> ItemBatches | None:
    """
    Returns the first n elements of iterable, flattening lists, but including an entire list if we would end in middle.

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


async def batch_iterate(
    iterable: AsyncIterable[AtomStreams], size: int
) -> AsyncIterator[ItemBatches]:
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

    # get a real once-through iterable (we want to iterate only once)
    true_iterable = aiter(iterable)
    while batches := await _batch_slice(true_iterable, size):
        yield batches
