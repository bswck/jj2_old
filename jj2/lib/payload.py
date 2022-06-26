from __future__ import annotations

import abc
import reprlib
from typing import ClassVar


class Payload(abc.ABC):
    feeds = None

    def __init__(self, **data):
        self._data = data
        self.serialize_cache = None
        self.deserialized_from = None

    def data(self, deserialization=False):
        return self._data

    def on_data_update(self):
        self.serialize_cache = None
        self.deserialized_from = None

    def feed(self, changes):
        self._data.update(changes)
        self.on_data_update()

    def refresh(self, context):
        if self.deserialized_from:
            self.deserialize(self.deserialized_from, context)

    def serialize(self, context):
        if self.serialize_cache is None:
            self.serialize_cache = self._serialize(context)
        return self.serialize_cache

    def deserialize(self, buffer, context):
        data = self._deserialize(buffer, context)
        self.feed(data)
        self.deserialized_from = buffer
        return self

    @abc.abstractmethod
    def _serialize(self, context):
        pass

    @abc.abstractmethod
    def _deserialize(self, buffer, context):
        pass

    def __init_subclass__(cls, has_feed=True):
        if has_feed and cls.feeds is None:
            cls.feeds = cls.__name__

    @reprlib.recursive_repr()
    def __repr__(self):
        data = self.data()
        return (
            type(self).__name__
            + ((
                '(' + ', '.join(f'{key!s}={value!r}' for key, value in data.items()) + ')'
            ) if data else '')
        )

    @classmethod
    def from_buffer(cls, buffer, context=None):
        return cls().deserialize(buffer, context or {})


class AbstractPayload(Payload, abc.ABC, has_feed=False):
    impls: ClassVar[dict]
    feeds = None
    has_default_implementation = None
    impl_feeder = True

    def data(self, deserialization=False, to_impl=False):
        if to_impl:
            return self._impl_data()
        return super().data(deserialization=deserialization)

    def _impl_data(self, deserialization=False):
        return self._data

    def serialize(self, context):
        if self.serialize_cache is None:
            impl = self.pick(context)
            if impl:
                self.serialize_cache = impl(**self.data(to_impl=True)).serialize(context)
            else:
                super().serialize(context)
        return self.serialize_cache

    def deserialize(self, buffer, context):
        super().deserialize(buffer, context)
        impl_class = self.pick(context)
        if impl_class is None:
            impl = self
        else:
            impl = impl_class().deserialize(self.data(deserialization=True, to_impl=True), context)
            if self.impl_feeder:
                self.feed({impl.feeds: impl.deserialized_from})
        return impl

    def pick(self, context):
        impl = self._pick(context)
        if impl is None and not self.has_default_implementation:
            raise NotImplementedError(
                f'not implemented for {self.feeds or "(no payload feeds))"!r} '
                f'(context: {context})'
            )
        return impl

    def _pick(self, context) -> type[Payload] | None:
        return

    def __init_subclass__(cls, has_feed=True):
        super().__init_subclass__(has_feed=has_feed)
        cls.impls = {}

    @classmethod
    def register(cls, condition):
        def _register_impl(payload_cls):
            cls.impls[condition] = payload_cls
            return payload_cls
        return _register_impl
