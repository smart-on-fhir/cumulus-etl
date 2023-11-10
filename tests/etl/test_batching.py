"""Tests for etl/tasks/batching.py"""

from collections.abc import AsyncIterator

import ddt

from cumulus_etl.etl.tasks import batching

from tests.utils import AsyncTestCase


@ddt.ddt
class TestBatching(AsyncTestCase):
    """Test case for basic batching methods"""

    @ddt.data(
        ([], 2, []),
        (  # batch boundary in middle of list group
            [1, [2.1, 2.2], 3, 4],
            2,
            [
                ([1, 2.1, 2.2],),
                ([3, 4],),
            ],
        ),
        (  # clean batch boundaries
            [[1.1, 1.2], [2.1, 2.2], 3, 4],
            2,
            [
                ([1.1, 1.2],),
                ([2.1, 2.2],),
                ([3, 4],),
            ],
        ),
        (  # mix of boundaries, including ending early
            [1, [2.1, 2.2], 3, 4, 5],
            2,
            [
                ([1, 2.1, 2.2],),
                ([3, 4],),
                ([5],),
            ],
        ),
        (  # larger group size acts similarly
            [1, [2.1, 2.2], 3, 4],
            3,
            [
                ([1, 2.1, 2.2],),
                ([3, 4],),
            ],
        ),
        (  # tiny group size works correctly
            [1, [2.1, 2.2], 3],
            1,
            [
                ([1],),
                ([2.1, 2.2],),
                ([3],),
            ],
        ),
        (  # multiple streams of input values get separated correctly (batch 2)
            [(1, "a"), ([2.1, 2.2], "b"), (None, ["c", "d"]), (None, "e"), (3, [])],
            2,
            [
                ([1], ["a"]),
                ([2.1, 2.2], ["b"]),
                ([], ["c", "d"]),
                ([3], ["e"]),
            ],
        ),
        (  # multiple streams of input values get separated correctly (batch 3)
            [(1, "a"), ([2.1, 2.2], "b"), (None, ["c", "d"])],
            3,
            [
                ([1, 2.1, 2.2], ["a", "b"]),
                ([], ["c", "d"]),
            ],
        ),
        (  # zero group size is invalid
            [1, 2, 3],
            0,
            ValueError,
        ),
        (  # negative group size is invalid
            [1, 2, 3],
            -1,
            ValueError,
        ),
    )
    @ddt.unpack
    async def test_batch_iterate(self, values, batch_size, expected):
        """Check a bunch of edge cases for the batch_iterate helper"""

        # Tiny little convenience method to be turn sync lists into async iterators.
        async def async_iter() -> AsyncIterator:
            for x in values:
                yield x

        async def gather_results() -> list:
            return [x async for x in batching.batch_iterate(async_iter(), batch_size)]

        if isinstance(expected, type) and issubclass(expected, BaseException):
            with self.assertRaises(expected):
                await gather_results()
        else:
            collected = await gather_results()
            self.assertEqual(expected, collected)
