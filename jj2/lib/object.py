from __future__ import annotations

import functools
import inspect
from collections import UserDict


OBJECT_PROPERTY_FLAG = '__object_property__'
MISSING = type('_missing_value_marker', (), {})()


class Object(UserDict, dict):
    def __init__(self, protocol=None, **data):
        super().__init__()
        self._protocol = None
        self.protocol = protocol
        for key, value in data.items():
            if isinstance(getattr(type(self), key), (Lazy, Property)):
                setattr(self, key, value)
            else:
                raise TypeError(
                    f'{type(self).__name__}() got an '
                    f'unexpected keyword argument {key!r}'
                )

    @property
    def protocol(self):
        return self._protocol

    @protocol.setter
    def protocol(self, protocol):
        self._protocol = protocol
        self.initialize_properties()

    def initialize_properties(self):
        for name, prop in inspect.getmembers(type(self)):
            try:
                object_property = object.__getattribute__(
                    prop, OBJECT_PROPERTY_FLAG
                )
            except AttributeError:
                object_property = None
            if object_property:
                setattr(self, name, object_property(self.protocol))
            elif isinstance(prop, Property):
                self.data.setdefault(
                    prop.key,
                    (prop.initial, None)[prop.initial is MISSING]
                )

    def __getitem__(self, item):
        try:
            return self.data[item]
        except KeyError as exc:
            try:
                return getattr(self, item)
            except AttributeError:
                raise exc from None

    def get(self, key, default=None):
        try:
            return getattr(self, key)
        except AttributeError:
            return super().get(key, default)


class Property:
    def __init__(
            self,
            initial=MISSING,
            name=None,
            on_get=None,
            on_update=None,
            on_error=None,
            collection=False,
    ):
        self.key = name
        self.owner = None
        self.initial = initial
        self.collection = collection

        self._on_get = on_get
        self._on_update = on_update
        self._on_error = on_error

    def __set_name__(self, owner, name):
        self._check_owner(owner)
        self.key = name

    def __get__(self, instance, owner):
        if instance is None:
            return self
        self._check_owner(owner)
        value =  instance.data[self.key]
        self._call_on_get(instance, value)
        return value

    def __set__(self, instance, value):
        self._call_on_update(instance, value)
        if self.collection:
            value = type(value)(
                map(functools.partial(self._map_object, instance), value)
            )
        else:
            value = self._map_object(instance, value)
        instance.data[self.key] = value

    def attribute(self, attribute):
        return _AttributeChain(self, attribute)

    def __getattr__(self, item):
        return self.attribute(item)

    @property
    def is_object_property(self):
        return getattr(self, OBJECT_PROPERTY_FLAG, None) is not None

    def _map_object(self, protocol, obj):
        if isinstance(obj, Object):
            if obj.protocol is None:
                obj = obj.copy()
                obj.protocol = protocol
        return obj

    def _check_owner(self, owner):
        if self.owner is None:
            self.owner = owner
        if owner is not self.owner:
            raise ValueError(
                f'{type(self).__name__!r} object linked to more than '
                f'1 context class'
            )

    def _call_on_get(self, instance, value):
        if not callable(self._on_get):
            return
        # noinspection PyBroadException
        try:
            self._on_get(instance, value)
        except Exception as exc:
            self._call_on_error(exc)

    def _call_on_update(self, instance, value):
        if not callable(self._on_update):
            return
        try:
            self._on_update(instance, value)
        except Exception as exc:
            self._call_on_error(exc)

    def _call_on_error(self, exc):
        if not callable(self._on_error):
            raise
        self._on_error(exc)

    def on_get(self, cb):
        self._on_get = cb
        return cb

    def on_update(self, cb):
        if self.is_object_property:
            raise ValueError('on_update() does not work for object properties')
        self._on_update = cb
        return cb

    def on_error(self, cb):
        if self.is_object_property:
            raise ValueError('on_error() does not work for object properties')
        self._on_error = cb
        return cb

    @classmethod
    def object(cls, object_cls):
        self = cls()
        setattr(self, OBJECT_PROPERTY_FLAG, object_cls)
        return self


class _AttributeChain:
    def __init__(self, prop, attribute=None):
        self.property = prop
        self.chain = [attribute] if attribute else []

    def __getattr__(self, item):
        self.chain.append(item)
        return self

    def resolve(self, instance, owner):
        return functools.reduce(
            getattr, self.chain, self.property.__get__(instance, owner)
        )

    def __repr__(self):
        return f'{self.property.key}.{".".join(self.chain)}'


class Lazy:
    def __init__(self, chain, initial=MISSING, read_only=True, mapper=None):
        if mapper and not read_only:
            raise ValueError('mapper works only for read-only properties')

        if isinstance(chain, Property):
            chain = _AttributeChain(chain)
            read_only = mapper is not None

        self.chain = chain
        self.key = None
        self.initial = initial
        self._mapper = mapper
        self.read_only = read_only

    def _map(self, instance, value):
        if callable(self._mapper):
            return self._mapper(instance, value)
        return value

    def mapper(self, cb):
        self.read_only = True
        self._mapper = cb
        return cb

    def __set_name__(self, owner, name):
        self.key = name

    def __get__(self, instance, owner):
        if instance is None:
            return self
        return self._map(instance, self.chain.resolve(instance, owner))

    def __set__(self, instance, value):
        if self.read_only:
            raise ValueError(
                f'{self.chain} pointed from '
                f'{type(instance).__name__} is read-only'
            )
        self.chain.property.__set__(instance, value)
