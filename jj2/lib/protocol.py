from __future__ import annotations

import enum
import functools
import heapq
import inspect
import sys
import traceback
from typing import Any, ClassVar

from construct import ConstructError

from jj2.lib import Payload


TAKES_RESPONSE_CLASS = '__takes_response_class__'
TAKES_PREVIOUS_VALUE_FLAG = '__takes_previous_value__'
HANDLER_FLAG = '__handles__'


class Priority(enum.IntEnum):
    DAEMON = 0
    NORMAL = 1
    IMPORTANT = 2
    URGENT = 3


@functools.total_ordering
class _PrioritizedHandler:
    def __init__(self, function, response_cls, priority, takes_protocol=False):
        self.fn = function
        self.response_cls = response_cls
        self.priority = priority
        self.takes_protocol = takes_protocol

    def __lt__(self, other):
        return -self.priority < -other.priority

    def __eq__(self, other):
        return -self.priority == -other.priority

    @property
    def takes_response_class(self):
        return getattr(self.fn, TAKES_RESPONSE_CLASS, self.response_cls is not None)

    @property
    def takes_previous_value(self):
        return getattr(self.fn, TAKES_PREVIOUS_VALUE_FLAG, False)

    def __call__(self, previous_value, protocol, payload):
        args = []
        if self.takes_protocol:
            args.append(protocol)
        args.append(payload)
        if self.takes_response_class:
            args.append(self.response_cls)
        if self.takes_previous_value:
            args.append(previous_value)
        return self.fn(*args)


def _condition_check(value, condition):
    if callable(condition):
        return condition(value)
    return condition == value


class If:
    def __init__(self, check):
        self._check = check
        self.checks_payload = False

    @classmethod
    def configured(cls, **config_conditions):
        def _check(proto, _payload):
            for key, cond in config_conditions.items():
                value = proto.config.get(key)
                if not _condition_check(value, cond):
                    return False
            return True

        return cls(_check)

    @classmethod
    def has(cls, **schema_conditions):
        def _check(_proto, payload):
            for key, cond in schema_conditions.items():
                value = payload.data().get(key)
                if not _condition_check(value, cond):
                    return False
            return True

        self = cls(_check)
        self.checks_payload = True
        return self

    @classmethod
    def returns_false(cls, check):
        return cls(lambda proto, payload: not If(check).check(proto, payload))

    @classmethod
    def returns_true(cls, check):
        return cls(check)

    def check(self, proto, payload):
        if callable(self._check):
            return self._check(proto, payload)
        return bool(self._check)

    def __and__(self, other):
        return type(self)(
            lambda proto, payload: self.check(proto, payload) and other.check(proto, payload)
        )

    def __or__(self, other):
        return type(self)(
            lambda proto, payload: self.check(proto, payload) or other.check(proto, payload)
        )


class Protocol:
    feeds: ClassVar[Any]
    payload_cls: type[Payload]

    _lookup: dict
    _registry: dict
    _handlers: dict

    _children: list

    def __init__(self, protocol=None, **config):
        self.protocol = protocol
        self._config = {}
        self.children = {}
        self.registry = []
        self.configure(**config)
        self._aborted = False

    @property
    def config(self):
        return self._config.copy()

    def configure(self, **config):
        self._config.update(config)
        registry = []

        for payload_cls, condition in self._registry.items():
            abort_on_check_failure = payload_cls is ALL_PAYLOADS
            if condition is None:
                check = True
            else:
                check = condition.check(self, None)

            if check:
                payload_cls.on_register(protocol_cls=self, protocol_supported=True)
            else:
                payload_cls.on_register(protocol_cls=self, protocol_supported=False)
                if abort_on_check_failure:
                    self._aborted = True
                    break

        for child_cls in self._children:
            if child_cls not in self.children:
                self.children[child_cls] = child_cls(self, **self._config)

        self.registry = registry

    @classmethod
    def register(cls, registered=None, condition=None):
        if registered is None:
            return functools.partial(cls.register, condition=condition)
        if issubclass(registered, Payload):
            checks_payload = False
            if condition:
                checks_payload = condition.checks_payload
            if checks_payload:
                raise ValueError('cannot check payload schema in protocol registry filter')
            cls._registry[registered] = condition
            return registered
        raise TypeError(
            'invalid registrar type: expected Payload subclass as a Protocol registrar'
        )

    @classmethod
    def handles(
            cls,
            payload_cls: type[Payload],
            condition: If | None = None,
            response_cls: type[Payload] | None = None,
            priority: Priority = Priority.NORMAL,
            bidirectional: bool = False,
    ):
        def _handles_fn(fn):
            (cls.register_handler, cls.register_bidirectional_handler)[bidirectional](
                fn,
                payload_cls=payload_cls,
                response_cls=response_cls,
                priority=priority,
                condition=condition,
            )
            return fn

        return _handles_fn

    def handle_data(self, serialized, context=None, **options):
        try:
            options.setdefault('length', len(serialized))
            payload = self.payload_cls.load(serialized, context=context, **options)
        except NotImplementedError:
            payload = None

        if payload:
            self.handle(payload)

    def handle(self, payload: Payload):
        if self._aborted:
            return

        if self not in payload._supports_protocols:
            return self.on_unknown_case(payload)

        handlers = []

        for conditional_cases in (
            self._handlers.get(type(payload), {}),
            self._handlers.get(ALL_PAYLOADS, {})
        ):
            for condition, cases in conditional_cases.items():
                if condition is None:
                    check = True
                else:
                    check = condition.check(self, payload)  # type: ignore
                if check:
                    for case in cases:
                        function = case.pop('function')
                        if isinstance(function, type) and issubclass(function, Protocol):
                            function = self.children[function].handle
                            case['takes_protocol'] = False
                        case['function'] = function
                        handler = _PrioritizedHandler(**case)
                        heapq.heappush(handlers, handler)

        self.call_handlers(payload, handlers)

    def submit_all(self, *payloads):
        for payload in payloads:
            self.submit(payload)

    def submit(self, payload):
        if self.protocol is None:
            raise NotImplementedError
        self.protocol.submit(payload)

    @classmethod
    def register_handler(
            cls,
            function,
            payload_cls,
            response_cls=None,
            priority=Priority.NORMAL,
            condition=None,
            takes_protocol=True,
    ):
        if response_cls is ALL_PAYLOADS:
            raise ValueError('cannot establish relation to all payload types')
        (
            cls._handlers
            .setdefault(payload_cls, {})
            .setdefault(condition, [])
            .append(dict(
                function=function,
                response_cls=response_cls,
                priority=priority,
                takes_protocol=takes_protocol,
            ))
        )

    @classmethod
    def register_bidirectional_handler(cls, **kwargs):
        payload_cls = kwargs.pop('payload_cls')
        response_cls = kwargs.pop('response_cls')

        for (
            payload_class, inject_class
        ) in ((payload_cls, response_cls), (response_cls, payload_cls)):
            kwargs['payload_cls'] = payload_class
            kwargs['response_cls'] = inject_class
            cls.register_handler(**kwargs)

    def call_handlers(self, payload: Payload, handlers):
        value = None
        for handler in handlers:
            value = self.call_handler(value, handler, payload)

    def call_handler(self, value, handler, payload):
        # noinspection PyBroadException
        try:
            new_value = handler(
                protocol=self,
                payload=payload,
                previous_value=value
            )
        except Exception:
            new_value = value
            self.on_error(payload)
        return new_value

    def on_unknown_case(self, payload):
        pass

    def on_error(self, payload=None, msg=None):
        if msg is None and payload:
            msg = f'handling {type(payload).__name__} payload'
        else:
            msg = f'{type(self).__name__} running'
        print(
            f'FATAL: exception caught during {msg}.',
            file=sys.stderr
        )
        traceback.print_exc()

    def __init_subclass__(cls, extends=None):
        cls._lookup = {}
        cls._registry = {}
        cls._handlers = {}

        cls._children = []

        if isinstance(extends, type) and issubclass(extends, Protocol):
            cls._lookup.update(extends._lookup)
            cls._registry.update(extends._registry)
            extends._children.append(cls)

        for name, function in inspect.getmembers(cls):
            # Avoid unsafe properties when creating Protocol subclasses!
            for relation_kwargs in getattr(function, HANDLER_FLAG, []):
                if relation_kwargs:
                    relation_kwargs['function'] = function
                    cls.register_handler(**relation_kwargs)


def takes_response_class(fn):
    setattr(fn, TAKES_RESPONSE_CLASS, True)
    return fn


def takes_previous_value(fn):
    setattr(fn, TAKES_PREVIOUS_VALUE_FLAG, True)
    return fn


def handles(
    payload_cls,
    condition=None,
    response=None,
    priority=Priority.NORMAL,
    takes_protocol=True,
):
    def _handles_decorator(fn):
        handles_list = getattr(fn, HANDLER_FLAG, [])
        if not handles_list:
            setattr(fn, HANDLER_FLAG, handles_list)
        handles_list.append(
            dict(
                payload_cls=payload_cls,
                condition=condition,
                response_cls=response,
                priority=priority,
                takes_protocol=takes_protocol
            )
        )
        return fn
    return _handles_decorator


ALL_PAYLOADS = type('_all_payloads', (), {})()
