from collections import UserDict


class Context(UserDict):
    def __init__(self, protocol):
        super().__init__(protocol.config)
        self.protocol = protocol

    def __getattr__(self, item):
        try:
            return self.data[item]
        except KeyError:
            raise AttributeError(f'{type(self).__name__!r} object has no attribute {item!r}')


class Item:
    def __init__(
            self,
            initial=None,
            name=None,
            on_get=None,
            on_change=None,
            on_error=None,
    ):
        self.key = name
        self.owner = None
        self.initial = initial
        self._on_get = on_get
        self._on_change = on_change
        self._on_error = on_error

    def __set_name__(self, owner, name):
        self._check_owner(owner)
        self.key = name

    def __get__(self, instance, owner):
        self._check_owner(owner)
        self._call_on_get(instance)
        if self.initial:
            instance.data.setdefault(self.key, self.initial)
        return instance.data[self.key]

    def __set__(self, instance, value):
        self._call_on_change(instance, value)
        instance.data[instance] = value

    def _check_owner(self, owner):
        if self.owner is None:
            self.owner = owner
        if owner is not self.owner:
            raise ValueError(f'{type(self).__name__!r} object linked to more than 1 context class')

    def _call_on_get(self, instance):
        if not callable(self._on_get):
            return
        # noinspection PyBroadException
        try:
            self._on_get(instance)
        except Exception as exc:
            self._call_on_error(exc)

    def _call_on_change(self, instance, value):
        if not callable(self._on_change):
            return
        try:
            self._on_change(instance, value)
        except Exception as exc:
            self._call_on_error(exc)

    def _call_on_error(self, exc):
        if not callable(self._on_error):
            raise
        self._on_error(exc)

    def on_get(self, cb):
        self._on_get = cb
        return cb

    def on_change(self, cb):
        self._on_change = cb
        return cb

    def on_error(self, cb):
        self._on_error = cb
        return cb
