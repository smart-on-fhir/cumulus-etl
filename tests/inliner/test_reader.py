"""Tests for inliner/reader.py"""

import asyncio

from cumulus_etl import errors
from cumulus_etl.inliner import reader
from tests import utils


class TestReader(utils.AsyncTestCase):
    @staticmethod
    def get_task_names() -> set[str]:
        return {
            task.get_name()
            for task in asyncio.all_tasks()
            if task.get_name().startswith("peek-ahead-")
        }

    async def test_parallelism(self):
        events = [asyncio.Event() for _i in range(5)]
        called = [asyncio.Event() for _i in range(5)]
        finished = [asyncio.Event() for _i in range(5)]

        async def processor(index: int, event) -> None:
            called[index].set()
            await event.wait()
            finished[index].set()

        peek_task = asyncio.create_task(reader.peek_ahead_processor(events, processor, peek_at=2))

        # Wait for first two worker calls to be made (and confirm the last worker isn't called)
        await asyncio.wait_for(called[0].wait(), 1)
        await asyncio.wait_for(called[1].wait(), 1)
        self.assertFalse(called[2].is_set())
        self.assertFalse(called[3].is_set())
        self.assertFalse(called[4].is_set())
        self.assertFalse(finished[0].is_set())
        self.assertFalse(finished[1].is_set())
        self.assertFalse(finished[2].is_set())
        self.assertFalse(finished[3].is_set())
        self.assertFalse(finished[4].is_set())
        self.assertFalse(peek_task.done())

        # Confirm we have the full set of expected tasks
        self.assertEqual(
            self.get_task_names(),
            {"peek-ahead-reader", "peek-ahead-worker-0", "peek-ahead-worker-1"},
        )

        # Release one worker, and confirm we then grab the next one.
        events[0].set()
        await asyncio.wait_for(finished[0].wait(), 1)
        await asyncio.wait_for(called[2].wait(), 1)
        self.assertFalse(called[3].is_set())
        self.assertFalse(called[4].is_set())
        self.assertFalse(finished[1].is_set())
        self.assertFalse(finished[2].is_set())
        self.assertFalse(finished[3].is_set())
        self.assertFalse(finished[4].is_set())
        self.assertFalse(peek_task.done())

        # Confirm that the read worker has finished (it reads 2 ahead of the consumers, so it
        # will have been able to read all 5 inputs now)
        async def no_more_reader():
            while "peek-ahead-reader" in self.get_task_names():
                await asyncio.sleep(0)

        await asyncio.wait_for(no_more_reader(), 1)
        self.assertEqual(self.get_task_names(), {"peek-ahead-worker-0", "peek-ahead-worker-1"})

        # Release the next worker
        events[2].set()
        await asyncio.wait_for(finished[2].wait(), 1)
        await asyncio.wait_for(called[3].wait(), 1)
        self.assertFalse(called[4].is_set())
        self.assertFalse(finished[1].is_set())
        self.assertFalse(finished[3].is_set())
        self.assertFalse(finished[4].is_set())
        self.assertFalse(peek_task.done())

        # Release the remaining workers
        events[1].set()
        events[3].set()
        events[4].set()
        await asyncio.wait_for(finished[1].wait(), 1)
        await asyncio.wait_for(finished[3].wait(), 1)
        await asyncio.wait_for(finished[4].wait(), 1)
        await peek_task
        self.assertTrue(peek_task.done())

        # Confirm all tasks have finished
        self.assertEqual(self.get_task_names(), set())

    async def test_worker_exception(self):
        async def processor(_index: int, _item: int) -> None:
            raise ValueError("boom")

        with self.assert_fatal_exit(errors.INLINE_TASK_FAILED):
            await reader.peek_ahead_processor([1, 2, 3, 4], processor, peek_at=2)
