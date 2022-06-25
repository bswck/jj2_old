from __future__ import annotations

import functools
import heapq
import sys
import traceback
from typing import Any, ClassVar

from jj2.lib.payload import PayloadSchema


TAKES_RECIPIENT_FLAG = '__takes_recipient__'
TAKES_PREV_VALUE_FLAG = '__takes_previous_value__'


@functools.total_ordering
class _PrioritizedHandler:
    def __init__(self, fn, recipient, priority):
        self.fn = fn
        self.recipient = recipient
        self.priority = priority

    def __lt__(self, other):
        return self.priority < other.priority

    def __eq__(self, other):
        return self.priority == other.priority

    @property
    def takes_recipient(self):
        return getattr(self.fn, TAKES_RECIPIENT_FLAG, False)

    @property
    def takes_previous_value(self):
        return getattr(self.fn, TAKES_PREV_VALUE_FLAG, False)

    def __call__(self, previous_value, args, kwargs):
        if self.takes_recipient:
            args = (self.recipient, *args)
        if self.takes_previous_value:
            args = (previous_value, *args)
        return self.fn(*args, **kwargs)


class Protocol:
    identity: ClassVar[Any]

    _lookup: dict
    _registry: dict
    _cases: dict
    _handlers: dict
    _unresolved_payloads: list

    def __init__(self, **config):
        self._config = {}
        self.ok_payloads = []
        self.configure(**config)

    def configure(self, **config):
        self._config.update(config)
        registry = self._registry.copy()
        for registrar, conditions in self._registry.items():
            for key, condition in conditions.items():
                if not self.condition_check(condition, self._config.get(key)):
                    del registry[registrar]
        self.ok_payloads = list(filter(lambda payload: payload in self._cases, registry))

    @classmethod
    def register(cls, registered=None, **config_conditions):
        if registered is None:
            return functools.partial(cls.register, **config_conditions)
        if issubclass(registered, (PayloadSchema, Protocol)):
            for key, value in config_conditions.items():
                cls._registry.setdefault(registered, {}).update({key: value})
            return registered
        raise TypeError(
            'invalid registrar type: expected Payload or Protocol as a Protocol registrar'
        )

    @classmethod
    def on(cls, payload, target, config_conditions, priority=0, **conditions):
        if isinstance(payload, str):
            found = cls._lookup.get(payload)
            if found is None:
                raise TypeError(f'undefined payload identity: {payload!r}')
            payload = found

        def _assign_target(fn):
            cls._cases[payload].setdefault(fn, {}).update({target: bulk_condition})
            return fn

        bulk_condition = {'priority': priority, 'config': {}, 'schema': {}}
        for cond_dict, conds in (
            (bulk_condition['config'], config_conditions),
            (bulk_condition['schema'], conditions)
        ):
            for key, condition in conds.items():
                cond_dict.update({key: condition})

        return _assign_target

    def handle(self, payload: PayloadSchema):
        case = self._cases.get(type(payload))
        if case is None:
            self.on_unknown_case(payload)
            return
        handlers = []
        for fn, handlers in case.items():
            for target, bulk_condition in handlers.items():
                for cond_type, data in (('config', self._config), ('schema', payload.data())):
                    for key, condition in bulk_condition[cond_type].items():
                        if self.condition_check(condition, data.get(key)):
                            handler = _PrioritizedHandler(fn, target, bulk_condition['priority'])
                            heapq.heappush(handlers, handler)
        self.call_handlers(payload, handlers)

    def call_handlers(self, payload: PayloadSchema, handlers):
        functools.reduce(functools.partial(self.call_handler, payload=payload), handlers)

    def call_handler(self, value, handler, payload):
        # noinspection PyBroadException
        try:
            new_value = handler(value)
        except Exception:
            new_value = value
            self.on_error(payload)
        return new_value

    def on_unknown_case(self, payload):
        return

    def on_error(self, payload):
        print(
            f'FATAL: exception caught during handling {payload.identity} payload.',
            file=sys.stderr
        )
        traceback.print_exc()

    @staticmethod
    def condition_check(condition, value):
        if callable(condition):
            return condition(value)
        return condition == value


def takes_recipient(fn):
    setattr(fn, TAKES_RECIPIENT_FLAG, True)
    return fn


def takes_previous_value(fn):
    setattr(fn, TAKES_PREV_VALUE_FLAG, True)
    return fn
