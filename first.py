from __future__ import annotations
import asyncio
from typing import (
    Awaitable,
    AsyncIterator,
    Any,
    Callable,
    TypeVar,
    Generic,
    Optional, Union
)

T = TypeVar("T")
U = TypeVar("U")


def aiofilter(fn: Callable[[T], bool] = lambda x: x is not None):
    def _inner_filter(blob: T):
        if fn(blob):
            return blob
    return _inner_filter


def stream_from_queue(
    queue: asyncio.Queue[Union[T, U]], sentinel: Optional[U] = None
) -> AioStream[T]:
    async def _inner():
        while True:
            blob = await queue.get()
            if blob is sentinel:
                return

            yield blob
    return AioStream(_inner())


class AioStream(Generic[T]):
    def __init__(self, stream: AsyncIterator[T]):
        self._stream = stream

    def __or__(self, fn: Callable[[T], U]) -> AioStream[U]:
        return AioStream(map_agen(self, fn))

    def __aiter__(self):
        return self

    def __anext__(self):
        return self._stream.__anext__()


from functools import singledispatch
import inspect

async def map_agen(
    agen: AsyncIterator[T],
    fn: Callable[[T], U], 
) -> AsyncIterator[U]:
    if inspect.iscoroutinefunction(fn):
        gen = _map_gen_async(agen, fn)
    else:
        gen = _map_agen_sync(agen, fn)
    
    async for blob in gen:
        yield blob



async def _map_agen_sync(
    agen: AsyncIterator[T],
    fn: Callable[[T], U], 
) -> AsyncIterator[U]:
    async for blob in agen:
        yield fn(blob)


async def gen_anext(agen: AsyncIterator[T]):
    result = await anext(agen)
    return agen, result


async def _map_gen_async(
    agen: AsyncIterator[T],
    fn: Callable[[T], Awaitable[U]], 
) -> AsyncIterator[U]:
    all_tasks = set()
    generator_done = False
    import itertools as it
    while True:
        aws = all_tasks
        if not generator_done:
            aws = it.chain.from_iterable([
                [asyncio.create_task(gen_anext(agen))], aws
            ])
        if not aws:
            break
        done, _ = await asyncio.wait(aws, return_when=asyncio.FIRST_COMPLETED)

        for fut in done:
            if fut.cancelled():
                continue
            
            exception = fut.exception()
            if exception:
                if isinstance(exception, StopAsyncIteration):
                    generator_done = True
                    continue
                yield exception
                continue

            result = fut.result() 
            if isinstance(result, tuple) and result[0] is agen:
                all_tasks.add(
                    asyncio.create_task(fn(result[1]))
                )
            else:
                all_tasks.discard(fut)
                yield result


async def main():
    async def example():
        for i in range(10):
            await asyncio.sleep(0)
            yield i
    import random
    async def scale(num):
        await asyncio.sleep(random.random())
        return num * 10

    # AioStream(example()) | scale | aiofilter(lambda x: x % 2 == 0)
    queue = asyncio.Queue()
    obj = object()
    x = stream_from_queue(queue, sentinel=obj) | scale
    for i in range(10):
        queue.put_nowait(i)
        print(i)
    queue.put_nowait(obj)
    print([b async for b in x])




if __name__ == "__main__":
    asyncio.run(main())