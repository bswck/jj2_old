from __future__ import annotations

import abc
from typing import ClassVar


class Payload(abc.ABC):
    identity = None

    def __init__(self, **data):
        self._raw_data = data
        self._data = None
        self._cached_serialized = None

    def data(self):
        self._data = self._raw_data
        return self._data

    def on_data_update(self):
        self._cached_serialized = None

    def feed(self, changes):
        self._raw_data.update(changes)

    def serialize(self, context):
        if self._cached_serialized is None:
            self._cached_serialized = self._serialize(context)
        return self._cached_serialized

    def deserialize(self, buffer, context):
        data = self._deserialize(buffer, context)
        self.feed(data)
        return self

    @abc.abstractmethod
    def _serialize(self, context):
        pass

    @abc.abstractmethod
    def _deserialize(self, buffer, context):
        pass

    def __init_subclass__(cls, has_identity=True):
        if has_identity and cls.identity is None:
            cls.identity = cls.__name__

    @classmethod
    def from_serialized(cls, buffer, context=None):
        return cls().deserialize(buffer, context or {})


class AbstractPayload(Payload, abc.ABC, has_identity=False):
    impls: ClassVar[dict]
    identity = None

    def __init__(self, data):
        super().__init__(data)

    def data(self, for_impl=False):
        return super().data()

    def serialize(self, context):
        impl = self.pick(context)
        return impl(**self.data(for_impl=True)).serialize(context)

    def deserialize(self, buffer, context):
        super().deserialize(buffer, context)
        impl = self.pick(context)
        impl.deserialize(self.data(for_impl=True), context)
        self.feed({impl.identity: impl.data()})
        return impl

    @abc.abstractmethod
    def pick(self, context):
        pass

    def __init_subclass__(cls, has_identity=False):
        super().__init_subclass__(has_identity=has_identity)
        cls.impls = {}

    @classmethod
    def register(cls, condition):
        def _register_impl(schema_cls):
            cls.impls[condition] = schema_cls
            return schema_cls
        return _register_impl

