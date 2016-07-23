#!/usr/bin/env python3

# TODO: .to_json()

import sys
import json

from typing import Any, List, Dict, Optional, Callable, Tuple
from weakref import WeakValueDictionary
from store import Store, Node, Leaf, NullEntryPoint


def colored(text, color):
    if color is None:
        return text
    else:
        return '\033[{};{}m{}\033[0m'.format(1 if color >= 8 else 0, 30 + (color % 8), text)


def bold(text, apply=True):
    if not apply:
        return text
    else:
        return '\033[1m{}\033[0m'.format(text)


def is_interactive():
    return hasattr(sys, 'ps1')


def join_path(a, b):
    return (a[0], join_path(a[1], b)) if a else b


def path_unroll(chain):
    """
    >>> path_unroll(('a', ('b', ('c', ('d', None)))))
    ['d', 'c', 'b', 'a']

    """
    r = []
    while chain:
        el, chain = chain
        r.append(el)
    r.reverse()
    return r


# Catch all operators that must throw an error.
class Nope:

    # Prevent Nope() == 1
    def __eq__(self, other): self._method_error()
    def __ne__(self, other): self._method_error()
    # Prevent Nope() < Nope(), etc.
    def __lt__(self, other): self._method_error()
    def __le__(self, other): self._method_error()
    def __gt__(self, other): self._method_error()
    def __ge__(self, other): self._method_error()
    # Prevent if Nope(): ..
    def __bool__(self): self._method_error()

    def _method_error(self):
        raise NotImplementedError


# Too complicated. Simplify?
class Schema:

    class Error(RuntimeError):
        pass

    def descend(self, tree, name):
        """
        Get the schema for the given subnode.
        """
        return self

    def validate(self, tree, name, value):
        """
        Check if the value of a leaf is valid.
        """
        pass

    def check(self, tree):
        """
        Check tree consistency before a commit."
        """
        pass

    def setup(self, tree):
        """
        Setup newly created node.
        """
        pass

    def extra(self, tree):
        """
        Extra keys to simulate.
        """
        pass

    def pose(self, tree, name: str, value: Any) -> Optional['Tree']:
        """
        Convert a leaf into a tree.
        """
        pass

    def choices(self, tree):
        """
        Get the set of possibles keys.
        """
        # This is useful for auto-completion
        pass

    def format(self, tree, name):
        """
        Indicates how to format the tree.

        Returns 'arg' or None.
        """
        pass

    def full_help(self, tree):
        """
        Get the help for the given tree.
        """
        pass

    def help(self, tree, name):
        """
        Get the help for a specific key.
        """
        # For help output. Could include description, syntax,..
        pass

    # FIXME: Problem is: if missing() returns an non empty set, then
    # check() should always fail. Remove missing() and replace with
    # something else? (hint() that returns a dictionary {key: hint}?)
    def missing(self, tree):
        """
        Get the set of keys that are missing.
        """
        pass  # set() with missing mandatory keys


class Empty:

    pass


class Move:

    __slots__ = ['__source']

    def __init__(self, source: 'Tree') -> None:
        assert isinstance(source, Tree), \
            'Cannot move a Leaf'
        self.__source = source

    @property
    def source(self) -> 'Tree':
        if self.__source is None:
            raise RuntimeError('Already moved')
        s = self.__source
        self.__source = None
        return s


# For __getitem__
def cast_name(name):
    if isinstance(name, int):
        return str(name)
    else:
        return name


# For __getattr__
def conv_name(name):
    return name


_NO_ARG = object()

class Tree(Nope):

    __slots__ = ['__entries', '__parent', '__name', '__node', '__schema']

    def __init__(self, *, parent, name, node, schema):
        super().__init__()
        assert parent is None or name is not None
        self.__entries = WeakValueDictionary()  # type: ignore
        self.__parent = parent
        self.__name = name
        self.__node = node
        self.__schema = schema

    @property
    def _name(self):
        return self.__name

    @property
    def _parent(self):
        return self.__parent

    @property
    def _root(self):
        return self if self.__parent is None else self.__parent._root

    @property
    def _path(self):
        if self.__name is None:
            assert self.__parent is None, 'Non root node without name'
            return []
        else:
            assert self.__parent is not None, 'Root node should not have name'
            return self.__parent._path + [self.__name]

    @property
    def _choices(self):
        return self.__schema.choices(self)

    def _method_error(self):
        assert self.__node is not None  # FIXME: Actually not sure if impossible
        raise KeyError(self.__name)

    def _to_json(self):
        """
        Transform the tree and leafs into a JSON structure.
        """
        r = {}
        for key in self._keys():
            t, value = self._get(key, raw=True, annotated=True)
            if t == 'tree':
                r[key] = value._to_json()
            elif t == 'leaf':
                r[key] = value
            else:
                pass
        return r

    def _keys(self):
        """
        Get the set of keys for this tree.
        """
        self.__load()
        if self.__node is None:
            raise KeyError(self.__name)
        return self.__node.keys()

    def _extra_keys(self):
        return set(self.__schema.extra(self) or ())

    def _missing_keys(self):
        return self.__schema.missing(self) or set()

    # FIXME: Maybe raw=True should be the default.
    def _get(self, name: str, *, raw=False, annotated=False, default=_NO_ARG) -> Any:
        assert isinstance(name, str), \
            'The key must be a string (not {!r})'.format(name)
        def resolve(o):
            return o() if callable(o) else o
        if name not in self.__entries:
            extra = self.__schema.extra(self)
            if extra and name in extra:
                return resolve(extra[name]) if not annotated else ('extra', resolve(extra[name]))
            if self.__node is not None:
                if not isinstance(self.__node, Node):
                    raise ValueError('Expected a node for {!r}'.format(self.__name))
                try:
                    node = self.__node.get(name)
                except KeyError:
                    node = None
            else:
                node = None
            if isinstance(node, Leaf):
                if not raw:
                    fail = False
                    try:
                        result = self.__schema.pose(self, name, node.value)
                    except:
                        raise
                        fail = True
                        result = None
                    if isinstance(result, Tree) or fail:
                        if not annotated:
                            return result
                        elif fail:
                            return ('badref', result)
                        else:
                            return ('ref', result)
                if not annotated:
                    return node.value
                else:
                    return ('leaf', node.value)
            if default is not _NO_ARG:
                return default
            schema = self.__schema.descend(self, name)
            val = Tree(parent=self, name=name, node=node, schema=schema)
            self.__entries[name] = val
        if not annotated:
            return self.__entries[name]
        else:
            return ('tree', self.__entries[name])

    def _del(self, name: str) -> None:
        assert isinstance(name, str), \
            'The key must be a string (not {!r})'.format(name)
        if self.__node is not None:
            self.__node.remove(name)

    def _clear(self):
        self.__load()
        for k in self.__node.keys():
            self._del(k)

    @property
    def _help(self):
        print(self.__schema.full_help(self) or 'No help available.')

    def _has(self, name):
        assert isinstance(name, str), \
            'The key must be a string (not {!r})'.format(name)
        self.__load()
        return name in self.__node.keys()

    def _set(self, name: str, value: Any) -> None:
        assert isinstance(name, str), \
            'The key must be a string (not {!r})'.format(name)
        setup = False
        if isinstance(value, Move):
            # Reparenting
            # BUG: Need to apply correct schema recursively.
            v = value.source
            if v.__node is None:
                raise KeyError(v._name)
            self.__precheck(name, v)
            p = self
            while p is not None:
                if p.__node is v.__node:
                    raise RuntimeError('Cannot move a tree to a child of itself.')
                p = p.__parent
            self.__realize()
            n = v.__parent.__node.remove(v.__name)
            self.__patch(name, v)
            self.__node.set(name, n)
            # FIXME: What are the implications of the following line?
            self.__entries.pop(name, None)
            v.__parent = self
        elif isinstance(value, Tree):
            # Copy
            if value is self.__entries.get(name):
                return
            self.__precheck(name, value)
            n = value.__node.clone()
            self.__realize()
            self.__patch(name, value)
            self.__node.set(name, n)
        else:
            if isinstance(value, Empty):
                value = Node()
                setup = True
            else:
                self.__schema.validate(self, name, value)
                value = Leaf(value)
            self.__realize()
            self.__node.set(name, value)
        # if setup:
        #     self.__schema.setup(self)

    def __check(self, schema):
        self.__realize()
        for k in sorted(self.__node.keys()):
            v = self._get(k)
            if not isinstance(v, Tree):
                schema.validate(self, k, v)
            else:
                v.__check(schema.descend(self, k))

    def __precheck(self, name, value):
        assert isinstance(value, Tree)
        schema = self.__schema.descend(self, name)
        value.__check(schema=schema)

    def __rec_patch(self):
        self.__realize()
        for k in sorted(self.__node.keys()):
            v = self._get(k)
            if isinstance(v, Tree):
                self.__patch(k, v)

    def __patch(self, name, value):
        value.__schema = self.__schema.descend(self, name)
        value.__rec_patch()

    def __commit_check(self):
        self.__schema.check(self)
        self.__realize()
        for k in sorted(self.__node.keys()):
            v = self._get(k, raw=True)
            if isinstance(v, Tree):
                v.__commit_check()

    def _check(self):
        self.__commit_check()

    def __load(self):
        if self.__node is None:
            self.__parent.__load()
            n = self.__parent.__node.get(self.__name)
            if isinstance(n, Node):
                self.__node = n

    def __realize(self) -> None:
        created = False
        def created_cb(node):
            nonlocal created
            created = True
        if self.__node is None:
            self.__parent.__realize()
            self.__node = self.__parent.__node.node(self.__name, created_cb)
            if created:
                self._setup()

    def _setup(self):
        self.__schema.setup(self)

    def __contains__(self, name):
        return self._has(cast_name(name))

    def __getitem__(self, name):
        return self._get(cast_name(name))

    def __setitem__(self, name, value):
        return self._set(cast_name(name), value)

    def __delitem__(self, name):
        self._del(cast_name(name))

    def __getattr__(self, name):
        if name.startswith('_'):
            raise RuntimeError("Use tree['_key'] for accessing keys prefixed by _")
        return self._get(conv_name(name))

    def __setattr__(self, name, value):
        if name.startswith('_'):
            super().__setattr__(name, value)
        else:
            return self._set(conv_name(name), value)

    def __delattr__(self, name):
        if name.startswith('_'):
            raise RuntimeError("Use tree['_key'] for accessing keys prefixed by _")
        self._del(conv_name(name))

    # FIXME: query in extra too?
    def __query(self, qpath: List[str]) -> List[Tuple[Any, Any]]:
        if not qpath:
            return {}
        self.__realize()
        r = [(None, self)]  # type: List[Tuple[Any, Any]]
        f = []
        for i, (element, keep) in enumerate(qpath):
            s = []  # type: List[Tuple[Any, Any]]
            for k, v in r:
                if not isinstance(v, Tree):
                    pass
                elif element in ('*', '**'):
                    # FIXME: We might want to include extra keys too
                    # (perhaps only if matching the final term of the
                    # query)
                    for key in v.__node.keys():
                        vv = v._get(key)
                        if element != '**':
                            s.append(((key, k) if keep else k, vv))
                        elif isinstance(vv, Tree):
                            f += [(join_path(kk, (key, k)) if keep else join_path(kk, k), e) for kk, e in vv.__query(qpath[i:])]
                    if element == '**':
                        f += v.__query(qpath[i+1:])
                else:
                    if element.startswith('{') and element.endswith('}'):
                        keys = element[1:-1].split(',')
                    else:
                        keys = [element]
                    for key in keys:
                        if key in v.__node.keys():
                            s.append(((key, k) if keep else k, v._get(key)))
            r = s
        return r + f

    def _query(self, expr, transform: Callable=None, filter: Callable=None):
        expr, sep, rest = expr.partition(',')
        if sep:
            a = self._query(expr)
            b = self._query(rest)
            r = {**a, **b}
            if len(a) + len(b) != len(r):
                raise RuntimeError('Name ellision conflict (multi-exprs)')
            return r
        path = expr.split('.')
        qpath = [(el, el.startswith('(') and el.endswith(')')) for el in path]
        if not any(item[1] for item in qpath):
            qpath = [(el, True) for el, keep in qpath]
        else:
            qpath = [(el[1:-1] if keep else el, keep) for el, keep in qpath]
        r = self.__query(qpath)
        if transform is None:
            transform = lambda o: o
        if filter is None:
            filter = lambda o: True
        r = [item for item in r if filter(item[1])]
        result = {tuple(path_unroll(k)): transform(v) for k, v in r}
        if len(result) != len(r):
            raise RuntimeError('Name ellision conflict')
        return result

    # -------- Utility --------

    def _get_path(self, path: List[str]) -> Any:
        if not path:
            return self
        elif len(path) == 1:
            return self._get(path[-1])
        else:
            return self._get(path[0])._get_path(path[1:])

    def _update(self, values):
        assert isinstance(values, dict)
        for key, value in values.items():
            if isinstance(value, dict):
                self[key]._update(value)
            else:
                self[key] = value

    # -------- Dump --------

    # FIXME: Pass name_prefix as a list?
    # FIXME: Rewrite this function
    def __dump(self, prefix, name_prefix, show_help, color, expand, depth_limit, flat):
        bottom = (depth_limit <= 0) if depth_limit is not None else False
        next_depth_limit = depth_limit - 1 if depth_limit is not None else None
        r = []
        extra = self.__schema.extra(self) or {}
        ks = self._keys() | extra.keys()
        def quote(n):
            return n.replace('\\', '\\\\').replace(' ', '\\ ').replace('.', '\\.').replace('\n', '\\n')
        if not ks:
            r.append('{}ø'.format(prefix))
        else:
            for k in sorted(ks):
                if name_prefix is not None:
                    nk = '{}{}{}'.format(colored(name_prefix, 12 if color else None), ' ' if not flat else '.', quote(k))
                else:
                    nk = quote(k)
                if show_help and k not in extra:
                    help = self.__schema.help(self, k)
                    if help is not None:
                        h = []
                        h.append('##')
                        for line in help.splitlines():
                            if line.strip():
                                h.append('## {}'.format(line.rstrip()))
                            else:
                                h.append('##')
                        h.append('##')
                        if color:
                            r += ['{}{}'.format(prefix, colored(line, 8))
                                  for line in h]
                        else:
                            r += ['{}{}'.format(prefix, line)
                                  for line in h]
                v = self._get(k, annotated=True)
                if v[0] == 'tree' or (v[0] == 'extra' and isinstance(v[1], Tree)):
                    c1 = 3 if color else None
                    c2 = 2 if color else None
                    c3 = 10 if color else None
                    if not expand and (flat or self.__schema.format(self, k) == 'arg') and (not isinstance(v[1], Tree) or v[1]._keys() or v[1]._extra_keys()):
                        if bottom:
                            r.append('{}{} {}..{}'.format(prefix, nk, colored('{', c1), colored('}', c1)))
                        else:
                            r += v[1].__dump(prefix, nk, show_help, color, expand, next_depth_limit, flat)
                    else:
                        if bottom:
                            r.append('{}{} {}..{}'.format(prefix, nk, colored('{', c1), colored('}', c1)))
                        else:
                            if v[0] == 'tree' and not v[1]._keys() and not v[1]._extra_keys() and not v[1]._missing_keys():
                                if not flat:
                                    r.append('{}{} {} ø {}'.format(prefix, nk, colored('{', c1), colored('}', c1)))
                            else:
                                if v[0] == 'tree':
                                    r.append('{}{} {}'.format(prefix, nk, colored('{', c1)))
                                else:
                                    r.append('{}{} {} {} {}'.format(prefix, colored(nk, c2), colored('=>', c3), colored('{', c1), colored('# ref:{}'.format('.'.join(map(quote, v[1]._path))), 7 if color else None)))
                                r += v[1].__dump(prefix + '  ', None, show_help, color, expand, next_depth_limit, flat)
                                r.append('{}{}'.format(prefix, colored('}', c1)))
                elif v[0] == 'leaf':
                    r.append('{}{} {};'.format(prefix, nk, bold(json.dumps(v[1]), color)))
                elif v[0] in ('ref', 'badref'):
                    bad = (v[0] == 'badref')
                    text = '{}{}{}'.format(colored('@(', (13 if not bad else 9) if color else None),
                                           colored(json.dumps(self._get(k, raw=True)), (5 if not bad else 1) if color else None),
                                           colored(')', (13 if not bad else 9) if color else None))
                    r.append('{}{} {}; {}'.format(prefix, nk, text, colored('# ref:{}'.format('.'.join(v[1]._path)), 7 if color else None)))
                elif v[0] == 'extra':
                    c1 = 2 if color else None
                    c2 = 10 if color else None
                    if not flat:
                        text = '{}{}{}'.format(colored('<', c2),
                                               colored(json.dumps(v[1]), c1),
                                               colored('>', c2))
                    else:
                        text = colored(json.dumps(v[1]), c1)
                    r.append('{}{} {};'.format(prefix, colored(nk, c1), text))
                else:
                    raise RuntimeError('Unexpected tag ({!r})'.format(v[0]))
        for name in sorted(self._missing_keys()):
            text = '/* {}: missing mandatory key {!r} */'.format('Warning' if not color else colored('Warning', 1), name)
            r.append('{}{}'.format(prefix, text))
        return r

    def _dump(self, *, help: bool=False, color: bool=False, expand: bool=False, depth: int=None, flat: bool=False) -> None:
        print('\n'.join(self.__dump('', None, help, color, expand, depth, flat)))

    def __repr__(self):
        if is_interactive():
            return '\n'.join(self.__dump('', None, False, True, False, None, False))
        else:
            return '{}{}({!r})'.format(self.__class__.__name__,
                                       '[{}]'.format(self.__node.offset
                                                     if self.__node.offset is not None
                                                     else '-')
                                       if self.__node is not None
                                       else '',
                                       dict(self.__entries))


class Configuration(Tree):

    __slots__ = ['__store']

    def __init__(self, filename, *, schema=None, volatile=False):
        if schema is None:
            schema = Schema()
        if filename is None:
            n = Node()
            super().__init__(parent=None, name=None, node=n, schema=schema)
            self.__store = Store.open_in_memory(volatile=volatile)
            self.__store.root = n
        else:
            self.__store = Store.open(filename, create_if_missing=True, volatile=volatile)
            setup = False
            try:
                self.__store.root
            except NullEntryPoint:
                self.__store.root = Node()
                setup = True
            super().__init__(parent=None, name=None, node=self.__store.root, schema=schema)
            if setup:
                self._setup()

    @property
    def _store(self):
        return self.__store

    def _commit(self):
        self._check()
        return self.__store.commit()

    def _diff(self):
        def fmt(v):
            if isinstance(v, Leaf):
                return json.dumps(v.value)
            else:
                return repr(v)
        for el in self.__store.diff():
            if el[0] in ('enter', 'leave'):
                pass
            else:
                for i, n in enumerate(el[1][:-1]):
                    print(' {}{} {{'.format('  ' * i, n))
                print(' {}...'.format('  ' * (len(el[1]) - 1)))
                if el[0] in ('added', 'changed'):
                    print('+{}{} {{'.format('  ' * (len(el[1]) - 1), el[1][-1]))
                    print('+{}{}'.format('  ' * len(el[1]), fmt(el[2])))
                    print('+{}}}'.format('  ' * (len(el[1]) - 1)))
                if el[0] in ('removed', 'changed'):
                    print('-{}{} {{'.format('  ' * (len(el[1]) - 1), el[1][-1]))
                    print('-{}{}'.format('  ' * len(el[1]), fmt(el[2] if el[0] == 'removed' else el[3])))
                    print('-{}}}'.format('  ' * (len(el[1]) - 1)))
                print(' {}...'.format('  ' * (len(el[1]) - 1)))
                for i in range(len(el[1])-2, -1, -1):
                    print(' {}}}'.format('  ' * i))
