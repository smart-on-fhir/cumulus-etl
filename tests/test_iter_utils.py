"""Tests for iter_utils.py"""

import asyncio
import os
import shutil
import tempfile
from typing import AsyncIterator, List
from unittest import mock

import ddt

from cumulus import iter_utils
from cumulus.etl import config, tasks

from tests.ctakesmock import CtakesMixin
from tests import i2b2_mock_data
from tests.utils import AsyncTestCase


@ddt.ddt
class TestBatchIterate(AsyncTestCase):
    """Test cases for batch_iterate"""

    @staticmethod
    async def async_iter(values: List) -> AsyncIterator:
        """Turns a sync list into an async iterator."""
        for x in values:
            yield x

    async def assert_batches_equal(self, expected: List, values: List, batch_size: int) -> None:
        """Handles converting all the async code into synchronous lists for ease of testing."""
        collected = []
        async for batch in iter_utils.batch_iterate(self.async_iter(values), batch_size):
            batch_list = []
            async for item in batch:
                batch_list.append(item)
            collected.append(batch_list)
        self.assertEqual(expected, collected)

    @ddt.data(
        # expected (list or exception), values, batch_size
        ([], [], 2),  # basic no-op
        (  # don't cut off a group
            [
                [1, 2.1, 2.2],
                [3, 4],
            ],
            [1, [2.1, 2.2], 3, 4],
            2,
        ),
        (  # batch falls on edges of groups
            [
                [1.1, 1.2],
                [2.1, 2.2],
                [3, 4],
            ],
            [[1.1, 1.2], [2.1, 2.2], 3, 4],
            2,
        ),
        (  # trailing incomplete group
            [
                [1, 2.1, 2.2],
                [3, 4],
                [5],
            ],
            [1, [2.1, 2.2], 3, 4, 5],
            2,
        ),
        (  # larger batch size
            [
                [1, 2.1, 2.2],
                [3, 4],
            ],
            [1, [2.1, 2.2], 3, 4],
            3,
        ),
        (  # tiny batch size
            [
                [1],
                [2.1, 2.2],
                [3],
            ],
            [1, [2.1, 2.2], 3],
            1,
        ),
        (ValueError, [1, 2, 3], 0),  # invalid batch size
        (ValueError, [1, 2, 3], -1),  # invalid batch size
    )
    @ddt.unpack
    async def test_batch_iterate(self, expected, values, batch_size):
        """Check a bunch of edge cases for the batch_iterate helper"""
        if issubclass(expected, BaseException):
            with self.assertRaises(expected):
                await self.assert_batches_equal([], values, batch_size)
        else:
            await self.assert_batches_equal(expected, values, batch_size)


@ddt.ddt
class TestPeekAheadIterator(AsyncTestCase):
    """Test cases for peek_ahead_iterator"""

    @staticmethod
    async def waiter(event: asyncio.Event, value: int) -> int:
        await event.wait()
        return value

    async def test_peek_ahead_limit(self):
        """Verify that we only peek so far ahead, rather than running all coroutines immediately"""
        event0 = asyncio.Event()
        event1 = asyncio.Event()
        coroutines = (
            self.waiter(event0, 0),
            self.waiter(event1, 1),
        )

        event1.set()  # 1 will start ready to go
        iterator = iter_utils.peek_ahead_iterator(coroutines, peek_at=1)

        with self.assertRaises(asyncio.TimeoutError):
            await asyncio.wait_for(iterator.__anext__(), 0.1)

        await coroutines[1]  # just for cleanliness, so python doesn't warn about an ignored coroutine

    async def test_peek_ahead_iterator(self):
        """Verify that we iterate through coroutines in the order they resolve"""
        event0 = asyncio.Event()
        event1 = asyncio.Event()
        event2 = asyncio.Event()
        event3 = asyncio.Event()
        coroutines = (
            self.waiter(event0, 0),
            self.waiter(event1, 1),
            self.waiter(event2, 2),
            self.waiter(event3, 3),
        )

        # 1 and 2 will start ready to go
        event1.set()
        event2.set()

        iterator = iter_utils.peek_ahead_iterator(coroutines, peek_at=2)

        self.assertEqual(1, await iterator.__anext__())
        self.assertEqual(2, await iterator.__anext__())
        event3.set()  # last entry
        self.assertEqual(3, await iterator.__anext__())
        event0.set()  # first entry, finished last
        self.assertEqual(0, await iterator.__anext__())

        with self.assertRaises(StopAsyncIteration):
            await iterator.__anext__()
