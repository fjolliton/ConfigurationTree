#!/usr/bin/env python3

from typing import Any, Optional, Union, Callable

import json
from enum import Enum
from collections import namedtuple
from weakref import ref as weakref

from storage import Storage
from utils import Printer


class StoreError(RuntimeError):
    pass


class NullEntryPoint(StoreError):
    pass


class DetachedRoot(StoreError):
    pass


class Kind(Enum):
    """
    The kind of record.

    Each kind is represented by a single character.
    """

    node = b'@'  # A configuration node.
    leaf = b'='  # A JSON-encodable value (a leaf).

    def __repr__(self):
        return '<{}>'.format(self.name)


class Store:

    __slots__ = ['__storage', '__current_root', '__root', '__volatile']

    # `volatile` is experimental.. and doesn't work perfectly.. yet.
    def __init__(self, storage: Storage, volatile: bool=False, alternate_root: Optional[int]=None) -> None:
        assert isinstance(storage, Storage)
        self.__storage = storage
        self.__current_root = self.__storage.get_current() if alternate_root is None else alternate_root  # type: int
        self.__root = self.__current_root  # type: Union[int, Item]
        self.__volatile = volatile

    @classmethod
    def open(cls, filename: str, create_if_missing: bool=False, volatile: bool=False) -> 'Store':
        """
        """
        assert isinstance(filename, str)
        storage = Storage.open(filename, create_if_missing=create_if_missing)
        return cls(storage, volatile=volatile)

    @classmethod
    def open_in_memory(cls, volatile: bool=False) -> 'Store':
        """
        """
        # For testing
        storage = Storage.open_in_memory()
        return cls(storage, volatile=volatile)

    def diff(self):
        # FIXME: The diff could be vastly optimized by knowing if a
        # key actually contains changes (the Node could indicates its
        # key that changed -including reparenting!-)
        # FIXME: Convert to a generator, with:
        # yield ('enter', path, a, b)
        # yield ('removed', path, old_value)  # Merge 3 as (None|old_value, None|new_value)?
        # yield ('changed', path, old_value, new_value)
        # yield ('added', path, new_value)
        # yield ('leave', path)
        # and let the caller do the formatting.
        current = Store(self.__storage, alternate_root=self.__current_root)
        def fmt(p):
            return '.'.join(p)
        def val(l):
            return json.dumps(l.value)
        def rec(p, a, b):
            yield ('enter', p, a, b)
            if isinstance(a, Node):
                if isinstance(b, Node):
                    ka = a.keys()
                    kb = b.keys()
                    for removed_key in ka - kb:
                        yield ('removed', p + [removed_key], a.get(removed_key))
                    for new_key in kb - ka:
                        yield ('added', p + [new_key], b.get(new_key))
                    for common_key in ka & kb:
                        yield from rec(p + [common_key], a.get(common_key), b.get(common_key))
                elif isinstance(b, Leaf):
                    yield ('changed', p, a, b)
                else:
                    raise TypeError
            elif isinstance(a, Leaf):
                if isinstance(b, Node):
                    yield ('changed', p, a, b)
                elif isinstance(b, Leaf):
                    if a.value != b.value:
                        yield ('changed', p, a, b)
                    else:
                        # Identical
                        pass
                else:
                    raise TypeError
            else:
                raise TypeError
            yield ('leave', p, a, b)
        assert isinstance(current.root, Node)
        assert isinstance(self.root, Node)
        yield from rec([], current.__root, self.__root)

    def _reset(self) -> None:
        self.__root = self.__current_root

    @property
    def volatile(self) -> bool:
        return self.__volatile

    # TODO
    def detach_root(self) -> None:
        value = self.__root
        value._detach()
        self.__root = None
        return value

    def _load(self, offset: int) -> 'Item':
        assert isinstance(offset, int)
        record = self.__storage.load(offset)
        if len(record) < 2:
            raise StoreError('Record at offset {} is too short'.format(offset))
        try:
            kind = Kind(record[:1])
        except:
            raise
        try:
            data = json.loads(record[1:].decode('utf-8'))
        except:
            raise StoreError('Unable to decode JSON')
        if kind is Kind.node:
            if (not isinstance(data, dict)
                or not all(isinstance(key, str) for key in data.keys())
                or not all(isinstance(value, int) for value in data.values())):
                raise StoreError('Node malformed')
            return Node(data, self, offset)
        elif kind is Kind.leaf:
            return Leaf(data, self, offset)
        else:
            raise RuntimeError('Unexpected kind ({!r})'.format(kind))

    def _record(self, kind: Kind, value: Any) -> int:
        data = json.dumps(value, separators=(',', ':'), sort_keys=True).encode('utf-8')
        return self.__storage.store(kind.value + data)

    def _get_root(self) -> 'Item':
        if self.__root is None:
            raise StoreError('Root unset')
        if self.__root == 0:
            raise NullEntryPoint()
        root = self.__root
        if isinstance(root, int):
            root = self.__root = self._load(root)
            assert isinstance(root, Item)
            root._attach(Link(self, '__ROOT__'))
        return root

    def _set_root(self, value: 'Item') -> None:
        assert isinstance(value, Item)
        value._attach(Link(self, '__ROOT__'))
        self.__root = value

    root = property(_get_root, _set_root)

    def _child_changed(self, key) -> None:
        assert key == '__ROOT__'

    def commit(self) -> int:
        if self.__root is None:
            raise DetachedRoot('Cannot commit with a detached root')
        if isinstance(self.__root, int):
            offset = self.__root
        elif isinstance(self.__root, Item):
            if self.__root.offset is None:
                self.__root._persist(self)
            offset = self.__root.offset
        else:
            raise TypeError(self.__root.__class__)
        # This is important to try set_current even if no change
        # occured, because we want to make sure to detect concurrent
        # changes.
        self.__storage.set_current(offset, self.__current_root)
        self.__current_root = offset
        return offset

    def dump_storage(self):
        return self.__storage.dump()

    def __repr__(self):
        r = ['/*** Store root ***/']
        printer = Printer(output=r.append)
        if isinstance(self.root, Leaf):
            printer(repr(self.root.get()))
        else:
            self.root.dump(printer=printer)
        return '\n'.join(r)


class Link(namedtuple('Link', ['parent', 'key'])):

    @property
    def offset(self):
        return self.parent._offset_of(self.key)


Ref = namedtuple('Ref', ['store', 'offset'])


class Item:

    __slots__ = ['__link', '__store', '__offset', '__weakref__']

    def __init__(self, store, offset):
        if store is not None or offset is not None:
            assert isinstance(store, Store)
            assert isinstance(offset, int)
        self.__link = None  # type: Link
        self.__store = store  # type: Optional[Store]
        self.__offset = offset  # type: Optional[int]

    def _persist(self, store) -> int:
        raise NotImplementedError(self.__class__)

    @property
    def offset(self):
        return self.__offset

    def modified(self):
        return self.__offset is None

    def _set_offset(self, offset):
        assert self.modified
        self.__offset = offset

    def _child_changed(self, key):
        self._changed()

    def _changed(self):
        if self.__offset is not None:
            self.__offset = None
            if self._link is not None:
                self._link.parent._child_changed(self._link.key)

    @property
    def _link(self):
        return self.__link

    @property
    def attached(self):
        return self.__link is not None

    def _attach(self, link):
        assert isinstance(link, Link)
        if self.__link is not None:
            raise StoreError('The item is already attached')
        self.__link = link

    def _detach(self):
        if self.__link is None:
            raise StoreError('The item is already detached')
        self.__link = None


class Leaf(Item):

    __slots__ = ['__value']

    def __init__(self, value: Any, store: Store=None, offset: int=None) -> None:
        super().__init__(store, offset)
        self.__value = value

    def _persist(self, store: Store) -> int:
        assert self.attached, \
            'Asked to persist an item that is not attached'
        assert self.offset is None
        self._set_offset(store._record(Kind.leaf, self.__value))

    def get(self):
        return self.__value

    def set(self, value):
        assert not isinstance(value, Item)  # FIXME: json type
        self.__value = value
        self._changed()

    value = property(get, set)

    def clone(self):
        return Leaf(self.__value)

    def __repr__(self):
        return 'Leaf({!r})'.format(self.__value)


class Node(Item):

    __slots__ = ['__entries', '__store']

    def __init__(self, entries=None, store: Store=None, offset: int=None) -> None:
        super().__init__(store, offset)
        if entries is None and store is None:
            entries = {}
        else:
            assert isinstance(entries, dict)
            assert all(isinstance(k, str) for k in entries.keys())
            assert all(isinstance(v, int) for v in entries.values())
            assert isinstance(store, Store)
            entries = {k: (v, None) for k, v in entries.items()}
        self.__entries = entries
        self.__store = store

    def _persist(self, store: Store) -> int:
        assert self.attached, \
            'Asked to persist an item that is not attached'
        node = {}
        for key, item in sorted(self.__entries.items()):
            if isinstance(item, tuple):
                offset = item[0]
            else:
                if item.offset is None:
                    item._persist(store)
                offset = item.offset
                if store.volatile:
                    self.__entries[key] = (offset, weakref(item))
            node[key] = offset
        self._set_offset(store._record(Kind.node, node))

    def keys(self):
        return set(self.__entries)

    def get(self, key: str) -> Item:
        entry = self.__entries[key]
        if isinstance(entry, tuple):
            offset, entry = entry
            if entry is not None:
                entry = entry()
            if entry is None:
                entry = self.__store._load(offset)
                entry._attach(Link(self, key))
                if self.__store.volatile:
                    self.__entries[key] = (offset, weakref(entry))
                else:
                    self.__entries[key] = entry
        return entry

    def set(self, key: str, value: Item) -> None:
        assert isinstance(key, str)
        assert isinstance(value, Item)
        value._attach(Link(self, key))
        if value.offset is not None and self.__store is not None and self.__store.volatile:
            value = (value.offset, weakref(value))
        self.__entries[key] = value
        self._changed()

    def remove(self, key: str) -> Item:
        assert isinstance(key, str)
        item = self.get(key)
        item._detach()
        del self.__entries[key]
        self._changed()
        return item

    def node(self, key: str, created_cb: Callable=None) -> Item:
        try:
            node = self.get(key)
        except KeyError:
            node = Node()
            self.set(key, node)
            if created_cb is not None:
                created_cb(node)
        else:
            assert isinstance(node, Node)
        return node

    def clone(self):
        n = Node()
        for key in self.keys():
            value = self.get(key)
            n.set(key, value.clone())
        return n

    def clear(self):
        r = {}
        for key in self.keys():
            r[key] = self.remove(key)
        return r

    def preload(self):
        for key in self.keys():
            value = self.get(key)
            if isinstance(value, Node):
                value.preload()

    def _dump(self, printer):
        if not self.__entries:
            printer('ø')
        else:
            for key, item in sorted(self.__entries.items()):
                if isinstance(item, tuple):
                    offset, item = item
                    if item is not None:
                        item = item()
                else:
                    offset = item.offset
                # FIXME: format_offset()?
                if offset is None:
                    offset_text = 'UNCOMMITED'
                else:
                    offset_text = '@{}'.format(offset)
                if item is None:
                    printer('{} (..{}..);'.format(key, offset_text))
                elif isinstance(item, Leaf):
                    printer('{} {};'.format(key, json.dumps(item.value)),
                            '  /* {} */'.format(offset_text))
                elif isinstance(item, Node):
                    printer('{} {{'.format(key),
                            '  /* {} */'.format(offset_text))
                    item._dump(printer.shift(2))
                    printer('}')
                else:
                    raise TypeError(item.__class__)

    def dump(self, *, verbose=True, printer=None):
        offset = self.offset
        if printer is None:
            printer = Printer()
        if verbose:
            printer('/* {} */'.format('@{}'.format(offset) if offset is not None else 'UNCOMMITED'))
        self._dump(printer)

    def __repr__(self):
        return 'Node({{{}}})'.format(', '.join('{!r}: ..'.format(key) for key in sorted(self.keys())))
