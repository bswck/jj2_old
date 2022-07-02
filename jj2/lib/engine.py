from __future__ import annotations

import abc
import asyncio
import inspect
import typing
import weakref

if typing.TYPE_CHECKING:
    from jj2.lib import Protocol


ON_FLAG = '__on__'


class LoopRunner:
    def __init__(self, coro=None):
        try:
            loop = asyncio.get_event_loop()
        except RuntimeError:
            loop = asyncio.new_event_loop()
        self.loop = loop
        self.tasks = weakref.WeakSet()  # note: remove after 3.9+
        self.coro = coro

    def task(self, coro, name=None):
        task = asyncio.Task(coro, loop=self.loop, name=name)
        self.tasks.add(weakref.ref(task))
        return task

    def done(self, _future):
        self.loop.stop()

    def shutdown(self):
        for task in self.tasks:
            task.cancel()

    async def run_async(self, *args, **kwargs):
        await self.coro(*args, **kwargs)

    def run(self, *args, **kwargs):
        future = asyncio.ensure_future(self.run_async(*args, **kwargs), loop=self.loop)
        future.add_done_callback(self.done)
        try:
            self.loop.run_forever()
        except KeyboardInterrupt:
            return
        else:
            if not future.cancelled():
                return future.result()
        finally:
            future.remove_done_callback(self.done)


class Engine(abc.ABC):
    protocol_class: type[Protocol]
    handler_prefix = 'on_'
    _handlers: dict

    def __init__(self, **config):
        self.config = config
        self.protocols = {}
        self.runner = LoopRunner(self.run)
        self._setup_fn = None

    @property
    def loop(self):
        return self.runner.loop

    def start(self, *args, **kwargs):
        self.runner.run(*args, **kwargs)

    def shutdown(self):
        self.runner.shutdown()

    def setup(self, fn):
        self._setup_fn = fn
        return fn

    def dispatch(self, protocol, event: str, payload=None):
        dispatch = self.get_dispatcher(event)
        if callable(dispatch):
            if payload:
                dispatch(protocol, payload)
            else:
                dispatch(protocol)

    def get_dispatcher(self, event):
        return getattr(self, f'dispatch_{event}', None)

    def call_handlers(self, event, *args, **kwargs):
        handlers = self._handlers.get(event, [])
        return asyncio.gather(*(handler(self, *args, **kwargs) for handler in handlers))

    def register_protocol(self, key, protocol):
        self.protocols.setdefault(key, []).append(protocol)
        return protocol

    async def run(self, *args, **kwargs):
        if callable(self._setup_fn):
            await self._setup_fn()

    @classmethod
    def on(cls, event, condition, fn, override=False):
        cls._handlers.setdefault(event, []).append((condition, fn))
        if override and condition:
            cls._handlers[event].pop((None, fn), None)

    def __init_subclass__(cls):
        cls._handlers = {}

        for name, fn in inspect.getmembers(cls, callable):
            if name.startswith(cls.handler_prefix):
                event = name[len(cls.handler_prefix):]
                cls.on(event, None, fn)
            for flags in getattr(fn, ON_FLAG, []):
                cls.on(**flags)


class Client(Engine, metaclass=abc.ABCMeta):
    @abc.abstractmethod
    async def connect(self, host, port) -> Protocol:
        pass


class Server(Engine, metaclass=abc.ABCMeta):
    @abc.abstractmethod
    async def run_server(self, port) -> Protocol:
        pass


def on(event, condition=None, override=None):
    def _on_decorator(fn):
        on_flags = getattr(fn, ON_FLAG, [])
        on_flags.append(dict(event=event, condition=condition, override=override))
        return fn
    return _on_decorator
