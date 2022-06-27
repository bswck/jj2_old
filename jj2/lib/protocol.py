from __future__ import annotations

import functools
import heapq
import inspect
from typing import Any, ClassVar

from jj2.lib import Payload


TAKES_PROVIDED_CLASS = '__takes_provided_class__'
TAKES_PREVIOUS_VALUE_FLAG = '__takes_previous_value__'
RELATION_FLAG = '__relation__'


@functools.total_ordering
class _PrioritizedHandler:
    def __init__(self, function, provide_cls, priority, takes_protocol=False):
        self.fn = function
        self.provide_cls = provide_cls
        self.priority = priority
        self.takes_protocol = takes_protocol

    def __lt__(self, other):
        return -self.priority < -other.priority

    def __eq__(self, other):
        return -self.priority == -other.priority

    @property
    def takes_provided_class(self):
        return getattr(self.fn, TAKES_PROVIDED_CLASS, self.provide_cls is not None)

    @property
    def takes_previous_value(self):
        return getattr(self.fn, TAKES_PREVIOUS_VALUE_FLAG, False)

    def __call__(self, previous_value, protocol, payload):
        args = []
        if self.takes_protocol:
            args.append(protocol)
        args.append(payload)
        if self.takes_provided_class:
            args.append(self.provide_cls)
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

    _lookup: dict
    _registry: dict
    _relations: dict

    _children: list

    def __init__(self, parent=None, **config):
        self.parent = parent
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
            if condition.check(self, None):
                payload_cls._mark(self, supported=True)
            else:
                payload_cls._mark(self, supported=False)
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
            if condition is None:
                condition = If(True)
            if condition.checks_payload:
                raise ValueError('cannot check payload schema in protocol registry filter')
            cls._registry[registered] = condition
            return registered
        raise TypeError(
            'invalid registrar type: expected Payload subclass as a Protocol registrar'
        )

    @classmethod
    def relation(
            cls,
            payload_cls,
            condition=None,
            provide_cls=None,
            priority=0,
            bidirectional=False,
    ):
        def _relation_fn(fn):
            (cls.register_relation, cls.register_bidirectional_relation)[bidirectional](
                fn,
                payload_cls=payload_cls,
                provide_cls=provide_cls,
                priority=priority,
                condition=condition,
            )
            return fn

        return _relation_fn

    def handle(self, payload: Payload):
        if self._aborted:
            return

        if self not in payload._supports_protocols:
            return self.on_unknown_case(payload)

        handlers = []

        conditional_cases = {
            **self._relations.get(ALL_PAYLOADS, {}),
            **self._relations.get(type(payload), {})
        }

        for condition, cases in conditional_cases.items():
            if condition.check(self, payload):
                for case in cases:
                    function = case.pop('function')
                    if isinstance(function, type) and issubclass(function, Protocol):
                        function = self.children[function].handle
                        case['takes_protocol'] = False
                    case['function'] = function
                    handler = _PrioritizedHandler(**case)
                    heapq.heappush(handlers, handler)

        self.call_handlers(payload, handlers)

    @classmethod
    def register_relation(
            cls,
            function,
            payload_cls,
            provide_cls=None,
            priority=0,
            condition=None,
            takes_protocol=True,
    ):
        if provide_cls is ALL_PAYLOADS:
            raise ValueError('cannot establish relation to all payload types')
        (
            cls._relations
            .setdefault(payload_cls, {})
            .setdefault(condition or If(True), [])
            .append(dict(
                function=function,
                provide_cls=provide_cls,
                priority=priority,
                takes_protocol=takes_protocol,
            ))
        )

    @classmethod
    def register_bidirectional_relation(cls, **kwargs):
        payload_cls = kwargs.pop('payload_cls')
        provide_cls = kwargs.pop('provide_cls')

        for (
            payload_class, provide_class
        ) in ((payload_cls, provide_cls), (provide_cls, payload_cls)):
            kwargs['payload_cls'] = payload_class
            kwargs['provide_cls'] = provide_class
            cls.register_relation(**kwargs)

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
        return

    def on_error(self, payload):
        raise
        #
        # print(
        #     f'FATAL: exception caught during handling {type(payload).__name__} payload.',
        #     file=sys.stderr
        # )
        # traceback.print_exc()

    def __init_subclass__(cls, extends=None):
        cls._lookup = {}
        cls._registry = {}
        cls._relations = {}

        cls._children = []

        if isinstance(extends, type) and issubclass(extends, Protocol):
            cls._lookup.update(extends._lookup)
            cls._registry.update(extends._registry)
            extends._children.append(cls)

        for name, function in inspect.getmembers(cls):
            # Avoid unsafe properties when creating Protocol subclasses!
            relation_kwargs = getattr(function, RELATION_FLAG, {})
            if relation_kwargs:
                relation_kwargs['function'] = function
                cls.register_relation(**relation_kwargs)


def takes_provided_class(fn):
    setattr(fn, TAKES_PROVIDED_CLASS, True)
    return fn


def takes_previous_value(fn):
    setattr(fn, TAKES_PREVIOUS_VALUE_FLAG, True)
    return fn


def relation(
    payload_cls,
    condition=None,
    provide_cls=None,
    priority=0,
    takes_protocol=True,
):
    def _handles_decorator(fn):
        setattr(
            fn,
            RELATION_FLAG,
            dict(
                payload_cls=payload_cls,
                condition=condition,
                provide_cls=provide_cls,
                priority=priority,
                takes_protocol=takes_protocol
            )
        )
        return fn
    return _handles_decorator


ALL_PAYLOADS = type('_all_payloads', (), {})()
