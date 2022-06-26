from __future__ import annotations

import functools
import heapq
import inspect
import sys
import traceback
from typing import Any, ClassVar

from jj2.lib import Payload

TAKES_ASSOCIATED_CLASS = '__takes_associated_class__'
TAKES_PREVIOUS_VALUE_FLAG = '__takes_previous_value__'
HANDLES_WITH_REPLY_FLAG = '__handles_with_reply__'


@functools.total_ordering
class _PrioritizedHandler:
    def __init__(self, fn, associated_class, priority):
        self.fn = fn
        self.associated_class = associated_class
        self.priority = priority

    def __lt__(self, other):
        return -self.priority < -other.priority

    def __eq__(self, other):
        return -self.priority == -other.priority

    @property
    def takes_associated_class(self):
        return getattr(self.fn, TAKES_ASSOCIATED_CLASS, self.associated_class is not None)

    @property
    def takes_previous_value(self):
        return getattr(self.fn, TAKES_PREVIOUS_VALUE_FLAG, False)

    def __call__(self, previous_value, args, kwargs):
        if self.takes_associated_class:
            args = (self.associated_class, *args)
        if self.takes_previous_value:
            args = (previous_value, *args)
        return self.fn(*args, **kwargs)


class Protocol:
    feeds: ClassVar[Any]

    _lookup: dict
    _registry: dict
    _cases: dict

    _children: list

    def __init__(self, parent=None, **config):
        self.parent = parent
        self._config = {}
        self.configure(**config)
        self.registry = {}
        self.children = {}

    def configure(self, **config):
        self._config.update(config)
        registry = self._registry.copy()
        for registrar, conditions in self._registry.items():
            for key, condition in conditions.items():
                value = self._config.get(key)
                if callable(condition):
                    ok = condition(value)
                else:
                    ok = condition == value
                if not ok:
                    del registry[registrar]
        for child_cls in self._children:
            if child_cls not in self.children:
                self.children[child_cls] = child_cls(self, **self._config)

    @classmethod
    def register(cls, registered=None, **config_conditions):
        if registered is None:
            return functools.partial(cls.register, **config_conditions)
        if issubclass(registered, Payload):
            for key, value in config_conditions.items():
                cls._registry.setdefault(registered, {}).update({key: value})
            return registered
        raise TypeError(
            'invalid registrar type: expected Payload subclass as a Protocol registrar'
        )

    @classmethod
    def handles_with_reply(
            cls, payload_cls, associated_cls,
            conditions=None, priority=0,
            **config_conditions
    ):
        if isinstance(payload_cls, (tuple, list)):
            assigners = []
            for a_payload_cls in payload_cls:
                assigners.append(
                    cls.handles_with_reply(
                        a_payload_cls, associated_cls,
                        conditions=conditions,
                        priority=priority,
                        **config_conditions
                    )
                )

            def _map_fn(fn):
                for assign in assigners:
                    assign(fn)
                return fn
            return _map_fn

        if isinstance(payload_cls, str):
            impl = cls._lookup.get(payload_cls)
            if impl is None:
                raise TypeError(f'undefined payload feeds: {payload_cls!r}')
            payload_cls = impl

        def _assign_associated_class(fn):
            (
                cls._cases
                .setdefault(payload_cls, {})
                .setdefault(fn, {})
                .update({associated_cls: conditions})
            )
            return fn

        conditions = {'priority': priority, 'config': {}, 'schema': {}}

        for condition_dict, condition_args in (
            (conditions['config'], config_conditions or {}),
            (conditions['schema'], conditions or {})
        ):
            for key, condition in condition_args.items():
                condition_dict.update({key: condition})

        return _assign_associated_class

    @classmethod
    def handles(cls, payload_cls, conditions=None, priority=0, **config_conditions):
        return cls.handles_with_reply(
            payload_cls=payload_cls,
            associated_cls=None,
            config_conditions=config_conditions,
            conditions=conditions,
            priority=priority
        )

    def handle(self, payload: Payload):
        case = self._cases.get(type(payload))
        if case is None:
            self.on_unknown_case(payload)
            return

        handlers = []

        for fn, handlers in case.items():
            for associated_class, conditions in handlers.items():
                for condition_type, data in (('config', self._config), ('schema', payload.data())):
                    for key, condition in conditions[condition_type].items():
                        value = data.get(key)
                        if callable(condition):
                            ok = condition(value)
                        else:
                            ok = condition == value
                        if ok:
                            if issubclass(fn, Protocol):
                                fn = self.children[fn].handle
                            handler = _PrioritizedHandler(
                                fn, associated_class,
                                priority=conditions['priority']
                            )
                            heapq.heappush(handlers, handler)

        self.call_handlers(payload, handlers)

    def call_handlers(self, payload: Payload, handlers):
        functools.reduce(functools.partial(self.call_handler, payload=payload), handlers)

    def call_handler(self, value, handler, payload):
        # noinspection PyBroadException
        try:
            new_value = handler(self, value)
        except Exception:
            new_value = value
            self.on_error(payload)
        return new_value

    def on_unknown_case(self, payload):
        return

    def on_error(self, payload):
        print(
            f'FATAL: exception caught during handling {payload.feeds} payload.',
            file=sys.stderr
        )
        traceback.print_exc()

    def __init_subclass__(cls, extends=None):
        cls._lookup = {}
        cls._registry = {}
        cls._cases = {}

        cls._children = []

        if isinstance(extends, type) and issubclass(extends, Protocol):
            cls._lookup.update(extends._lookup)
            cls._registry.update(extends._registry)
            cls._cases.update(extends._cases)
            extends._children.append(cls)

        for name, member in inspect.getmembers(cls):
            # Avoid unsafe properties when creating Protocol subclasses!
            handles_with_reply_kwargs = getattr(member, HANDLES_WITH_REPLY_FLAG, {})
            if handles_with_reply_kwargs:
                handles_with_reply_kwargs.setdefault('associated_cls', None)
                cls.handles_with_reply(**handles_with_reply_kwargs)(member)


def takes_associated_class(fn):
    setattr(fn, TAKES_ASSOCIATED_CLASS, True)
    return fn


def takes_previous_value(fn):
    setattr(fn, TAKES_PREVIOUS_VALUE_FLAG, True)
    return fn


def handles_with_reply(
    payload_cls,
    reply_cls,
    conditions=None,
    priority=0,
    **config_conditions
):
    def _handles_decorator(fn):
        setattr(
            fn,
            HANDLES_WITH_REPLY_FLAG,
            dict(
                payload_cls=payload_cls,
                reply_cls=reply_cls,
                conditions=conditions,
                priority=priority,
                **config_conditions
            )
        )
        return fn
    return _handles_decorator


def handles(payload_cls, conditions=None, priority=0, **config_conditions):
    return handles_with_reply(
        payload_cls=payload_cls,
        reply_cls=None,
        conditions=conditions,
        priority=priority,
        **config_conditions
    )
