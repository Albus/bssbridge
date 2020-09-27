from contextlib import asynccontextmanager
from typing import Optional
from asyncio.selector_events import BaseSelectorEventLoop
import asyncio
import pydantic


@asynccontextmanager
async def background_thread_loop(timeout: Optional[pydantic.StrictInt] = 10,
                                 daemon: bool = True, debug: bool = True) -> BaseSelectorEventLoop:
    from threading import Thread

    def run_forever(loop: BaseSelectorEventLoop):
        asyncio.set_event_loop(loop)
        try:
            loop.run_forever()
        except asyncio.CancelledError:
            pass
        finally:
            for task in asyncio.Task.all_tasks():
                task.cancel()
            loop.run_until_complete(loop.shutdown_asyncgens())
            loop.stop()
            loop.close()

    new_loop = asyncio.new_event_loop()
    new_loop.set_debug(enabled=debug)
    try:
        new_thread = Thread(target=run_forever, kwargs={'loop': new_loop}, daemon=daemon)
        new_thread.start()
        yield new_loop
    finally:
        new_loop.call_soon_threadsafe(new_loop.stop)
        new_thread.join(timeout=timeout)


class ThreadWithLoop:
    _queue: asyncio.Queue
    _consumer_task: asyncio.Task
    _loop: asyncio.BaseEventLoop

    def _run_async(self, coro) -> None:
        future = asyncio.run_coroutine_threadsafe(coro, self._loop)
        future.result()

    def __init__(self, loop) -> None:
        self._loop = loop
        self._run_async(self._init())

    def put(self, item=...) -> None:
        self._run_async(self._put(item))

    def join(self) -> None:
        self._run_async(self._join())

    def close(self) -> None:
        self.put()
        self.join()

    @property
    def queue_len(self) -> int:
        return self._queue.qsize()

    async def _init(self):
        self._queue = asyncio.Queue()
        self._consumer_task = self._loop.create_task(self._consumer())

    async def _consumer(self):
        """async def _consumer(self):
            while True:
                message = await self._queue.get()
                if message is ...:
                    break
                else:
                    print(f"Processing {message!r}...")"""
        raise NotImplemented

    async def _join(self):
        return await asyncio.shield(self._consumer_task)

    async def _put(self, item):
        return await asyncio.shield(self._queue.put(item))
