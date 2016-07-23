#!/usr/bin/env python3

# TODO: Validate the root too! (store.root = 10 => Error, need a tree.)
# TODO: Check for unknown key!

import re
from typing import Dict, Optional, Callable

from tree import Tree, Schema, Empty


class ValidationError(RuntimeError):

    def __init__(self, path, msg):
        super().__init__(path, msg)

    def __str__(self):
        return '[{}] {}'.format(self.args[0], self.args[1])


# Rename ValueValidator to ValueSchema?
class ValueValidator:

    desc = None

    def validate(self, tree, key, value):
        if isinstance(value, Tree):
            p = '.'.join(tree._path + [key])
            raise ValidationError(p, 'This key must be a value, not a tree.') from None


class BooleanValidator(ValueValidator):

    desc = 'a boolean (false or true)'

    def validate(self, tree, key, value):
        super().validate(tree, key, value)
        if not isinstance(value, bool):
            p = '.'.join(tree._path + [key])
            raise ValidationError(p, 'This must be a boolean')


class IntegerValidator(ValueValidator):

    desc = 'an integer'

    def validate(self, tree, key, value):
        super().validate(tree, key, value)
        if not isinstance(value, int):
            p = '.'.join(tree._path + [key])
            raise ValidationError(p, 'This must be an integer')


class StringValidator(ValueValidator):

    desc = 'a string'

    def validate(self, tree, key, value):
        super().validate(tree, key, value)
        if not isinstance(value, str):
            p = '.'.join(tree._path + [key])
            raise ValidationError(p, 'This must be a string')


class Type(Schema):

    __slots__ = ['__mapping', '__pattern_mapping', '__extra', '__extra_dyn', '__check']

    # TODO: Check if there are pattern. If note, optimize lookup with
    # dict. If there are only regexes, maybe first check with a
    # combined regex for faster check?
    def __init__(self, mapping: Dict=None, check: Optional[Callable]=None, extra: Optional[Callable]=None) -> None:
        """
         Args:
             mapping -- (OrderedDict|dict)
        """
        super().__init__()
        assert check is None or callable(check)
        self.__mapping = None
        self.__pattern_mapping = None
        if mapping is not None:
            self.set(mapping)
        self.__check = check
        self.__extra_dyn = extra or (lambda tree: {})

    def set(self, mapping):
        assert self.__mapping is None, 'Cannot set mapping again'
        assert isinstance(mapping, dict)
        assert all(isinstance(key, str) for key in mapping.keys())
        assert all(isinstance(value, dict) or callable(value) for value in mapping.values())
        self.__extra = {}
        self.__mapping = {}
        self.__pattern_mapping = []
        for key, spec in mapping.items():
            if callable(spec):
                self.__extra[key] = spec
            else:
                assert isinstance(spec, dict)
                assert spec.keys() <= {'type', 'description', 'pattern', 'required', 'cond', 'arg', 'extra', 'pose'}, \
                    'Unexpected key in specification'
                spec = dict(spec)
                spec.setdefault('type', Schema())
                spec.setdefault('pattern', False)
                spec.setdefault('required', lambda tree: False)
                spec.setdefault('cond', lambda tree: True)
                spec.setdefault('arg', False)
                spec.setdefault('extra', {})
                spec.setdefault('pose', lambda tree, value: None)
                assert 'description' not in spec or isinstance(spec['description'], str)
                assert isinstance(spec['type'], (Schema, ValueValidator))
                assert isinstance(spec['pattern'], bool)
                assert callable(spec['required']) or isinstance(spec['required'], bool)
                assert callable(spec['cond'])
                assert isinstance(spec['arg'], bool)
                assert isinstance(spec['extra'], dict)
                assert callable(spec['pose'])
                if not callable(spec['required']):
                    v = spec['required']
                    spec['required'] = lambda tree: v
                # FIXME: Replace optional values with actual values.
                if not spec['pattern']:
                    self.__mapping[key] = spec
                else:
                    self.__pattern_mapping.append((re.compile(key), dict(spec, regex=key)))

    def _lookup(self, tree, key):
        for pat, spec in self.__pattern_mapping:
            if pat.match(key):
                break
        else:
            try:
                spec = self.__mapping[key]
            except KeyError:
                p = '.'.join(tree._path + [key])
                candidates = []  # type: List[str]
                candidates += sorted(self.__mapping)
                candidates += ['/{}/'.format(key.pattern) for key, _ in self.__pattern_mapping]
                if not candidates:
                    raise ValidationError(p, 'No key are allowed at this level') from None
                else:
                    raise ValidationError(p,
                                          'Invalid key. Allowed keys are: {}'
                                          .format(', '.join(candidates))) from None
        return spec

    def validate(self, tree, key, value):
        sub = self.get_validator_for_key(tree, key)
        if isinstance(sub, ValueValidator):
            sub.validate(tree, key, value)
        elif isinstance(sub, Schema):
            p = '.'.join(tree._path + [key])
            raise ValidationError(p, 'Expected a tree, not a leaf')

    def format(self, tree, name):
        try:
            spec = self._lookup(tree, name)
        except ValidationError:
            # Can happen for extra keys
            spec = {'arg': False}
        return 'arg' if spec['arg'] else None

    def pose(self, tree, name, value):
        spec = self._lookup(tree, name)
        return spec['pose'](tree, value)

    def full_help(self, tree):
        r = []
        def doc(name, spec):
            if isinstance(spec['type'], ValueValidator):
                t = '= ...;'
            else:
                t = '{ ... }'
            r.append('{} {}'.format(name if not spec['pattern'] else '/{}/'.format(name), t))
            if spec['required'](tree):
                r.append('  *Required*')
            else:
                r.append('  Optional')
            if spec.get('description'):
                r.append('  Description: {}'.format(spec['description']))
            r.append('')
        if self.__mapping:
            for key, spec in sorted(self.__mapping.items()):
                doc(key, spec)
        if self.__pattern_mapping:
            for regex, spec in sorted(self.__pattern_mapping):
                doc(spec['regex'], spec)
        return '\n'.join(r)

    def help(self, tree, name):
        spec = self._lookup(tree, name)
        result = []
        desc = spec.get('description')
        if desc:
            result.append(desc)
        type = spec['type']
        if isinstance(type, ValueValidator):
            if type.desc:
                result.append('Type: {}'.format(type.desc))
        if spec['pattern']:
            result.append('Pattern: {}'.format(spec['regex']))
        required = spec['required'](tree)
        if required:
            result.append('Required')
        else:
            result.append('Optional')
        return '\n'.join(result) or None

    def check_keys(self, tree):
        keys = tree._keys()
        here = '.'.join(tree._path)
        missing = set()
        for k, v in sorted(self.__mapping.items()):
            if v['required'](tree) and k not in keys:
                missing.add(k)
        for k in keys:
            spec = self._lookup(tree, k)
            if not spec['cond'](tree):
                raise ValidationError(here or 'ROOT', 'Key forbidden: {}'.format(k))
            self.get_validator_for_key(tree, k)
        if missing:
            raise ValidationError(here or 'ROOT',
                                  'Mandatory key{} missing: {}'
                                  .format('s' if len(missing) > 1 else '', ', '.join(sorted(missing))))

    def check(self, tree):
        self.check_keys(tree)
        if self.__check is not None:
            self.__check(tree)

    def choices(self, tree):
        return set(self.__mapping.keys())

    def missing(self, tree):
        s = set()
        keys = tree._keys()
        for k, v in self.__mapping.items():
            if v['required'](tree) and k not in keys:
                p = '.'.join(tree._path + [k])
                s.add(k)
        return s

    def get_validator_for_key(self, tree, key):
        spec = self._lookup(tree, key)
        return spec['type']

    def descend(self, tree, key):
        if self.__mapping is None:
            raise RuntimeError('Schema unset')
        validator = self.get_validator_for_key(tree, key)
        if not isinstance(validator, Schema):
            p = '.'.join(tree._path + [key])
            raise ValidationError(p, 'This must be a value, not a tree')
        return validator

    def extra(self, tree):
        r = {k: lambda: v(tree) for k, v in self.__extra.items()}
        r.update(self.__extra_dyn(tree))
        return r

    def setup(self, tree):
        for key, spec in sorted(self.__mapping.items()):
            if spec['required'](tree):
                if not isinstance(spec['type'], ValueValidator):
                    tree[key] = Empty()
