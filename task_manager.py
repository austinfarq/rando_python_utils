import asyncio
from typing import Awaitable


class AsyncTaskManager:
    def __init__(self):
        self._tasks = set()

    def create(self, aw: Awaitable):
        task = asyncio.ensure_future(aw)
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)
        return task

    def __await__(self):
        pass