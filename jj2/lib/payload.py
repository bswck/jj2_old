from __future__ import annotations

import abc
import reprlib
import weakref
from typing import ClassVar, Any


class Payload(abc.ABC):
    event: str | None = None
    feeds: str | None = None
    _supports_protocols: weakref.WeakSet

    def __init__(self, **data):
        self._data = data
        self.serialized = None
        self.deserialized_from = None

    def data(self, deserialization=False):
        return self._data

    def on_data_update(self):
        self.serialized = None
        self.deserialized_from = None

    def feed(self, changes):
        self._data.update(changes)
        self.on_data_update()

    def refresh(self, context):
        if self.deserialized_from:
            self.deserialize(self.deserialized_from, context)

    def serialize(self, context=None, **kwargs):
        context = context or {}
        if self.serialized is None:
            self.serialized = self._serialize(context)
        return self.serialized

    def deserialize(self, serialized, context, **kwargs):
        context = context or {}
        data = self._deserialize(serialized, context, **kwargs)
        if data is not None:
            self.feed(data)
            self.deserialized_from = serialized
        return self

    @classmethod
    def load(cls, serialized, context=None, **options):
        return cls().deserialize(
            serialized=serialized,
            context=context,
            **options
        )

    @classmethod
    def from_dict(cls, data):
        return cls(**data)

    @classmethod
    def on_register(cls, protocol_cls=None, payload_cls=None, protocol_supported=True):
        if payload_cls:
            cls._supports_protocols = payload_cls._supports_protocols
        if protocol_cls:
            if protocol_supported:
                cls._supports_protocols.add(protocol_cls)
            else:
                cls._supports_protocols.discard(protocol_cls)
        return protocol_supported

    @abc.abstractmethod
    def _serialize(self, context, **kwargs):
        pass

    @abc.abstractmethod
    def _deserialize(self, serialized, context, **kwargs):
        pass

    def __init_subclass__(cls, has_feed=True):
        if has_feed and cls.feeds is None:
            cls.feeds = cls.__name__
        cls._supports_protocols = weakref.WeakSet()

    @reprlib.recursive_repr()
    def __repr__(self):
        data = self.data()
        return (
            type(self).__name__
            + ((
                '(' + ', '.join(f'{key!s}={value!r}' for key, value in data.items()) + ')'
            ) if data else '()')
        )


class AbstractPayload(Payload, abc.ABC, has_feed=False):
    impls: ClassVar[dict]
    impl_spec: tuple[type[Payload], Any]
    feeds = None
    has_default_implementation = None
    impl_feeder = True

    def data(self, deserialization=True, to_impl=False):
        if to_impl:
            return self._impl_data()
        return super().data(deserialization=deserialization)

    def _impl_data(self, deserialization=False):
        return self._data

    def serialize(
            self,
            context=None,
            standalone=False,
            **kwargs
    ):
        context = context or {}
        implements_cls = impl_key = None
        impl_spec = getattr(self, 'impl_spec', None)
        if not standalone and impl_spec and self.impl_feeder:
            implements_cls, impl_key = impl_spec
        self.serialized = super().serialize(context)
        if implements_cls:
            implements = implements_cls.from_dict({self.feeds: self.serialized})
            implements._set_impl_key(impl_key, context)
            serialized = implements.serialize(**kwargs, context=context)
            return serialized
        return self.serialized

    def deserialize(self, serialized, context=None, **kwargs):
        super().deserialize(serialized, context)
        context = context or {}
        impl_class = self.pick(context)
        if impl_class is None:
            impl = self
        else:
            impl = impl_class().deserialize(
                serialized=self.data(deserialization=True, to_impl=True),
                context=context,
                **kwargs
            )
            if self.impl_feeder:
                self.feed({impl.feeds: impl.deserialized_from})
        return impl

    def pick(self, context):
        impl = self.impls.get(self._get_impl_key(context))
        if impl is None and not self.has_default_implementation:
            raise NotImplementedError(
                f'not implemented for {self.feeds or "(no payload feeds field))"!r} '
                f'(context: {context})'
            )
        return impl

    def _get_impl_key(self, context) -> type[Payload] | None:
        return

    def _set_impl_key(self, key, context):
        return

    def __init_subclass__(cls, has_feed=True):
        super().__init_subclass__(has_feed=has_feed)
        cls.impls = {}

    @classmethod
    def register(cls, value):
        def _register_impl(payload_cls):
            cls.impls[value] = payload_cls
            payload_cls.impl_spec = (cls, value)
            payload_cls.on_register(payload_cls=cls)
            return payload_cls
        return _register_impl

