"""

Low level storage
=================

The low level storage knows very little about highel level stuff.

The low level storage operate on a single file, with the following
primitives:

 - storing a value (a byte string) and returning its offset,

 - loading back a value given its offset,

 - managing an special offset at the beginning of the file.

It ensures consistency regarding concurrent accesses.

It does nothing else.

The exact usage of the special offset is up to the user.  But the idea
is that it points to one of the record as the root of a datastructure.

File format
-----------

The file starts with an unique identifier on the first line, which act
as a version number and can help identifying the file.

Then followed by a second line with a 0 padded decimal integer, whose
value indicates an entry point offset.

Then, follow each records, one per line.

A record starts with '\t', followed by the binary data to store (but
not containing '\t' nor '\n'), terminated by a '\n'.

Example:

    3dbf4cbc-f015-43d9-b280-ff6962a22198
    000000037
    <TAB>This is the first record.
    <TAB>This is the second record.

(<TAB> stands for the invisible '\t' character.)

The content above has a pointer with value 37, referencing the third
line.  Thus this file has "This is the second record." as its main
value.

By using '\t' and '\n' to delimit a record, it make possible to perform
corruption detection when attempting to load a record.
"""

# TODO: Prefix record with size in bytes (<TAB>12:Hello, World<NL>)
# for further data corruption detection?

# TODO: We could even put a checksum
# (<TAB>12:82bb413746aee42f89dea2b59614f9ef:Hello, World<NL>) but this
# might be too heavy (or maybe make that conditional).

# TODO: Ensure the file has still the same name? (check inode) This is
# not atomic, but this could detect a file renaming/file deletion/file
# overload.

from typing import Optional

import io
import os
import fcntl
from contextlib import contextmanager

from io import BytesIO  # Note: We use BytesIO only for tests.

from utils import Printer


IDENTIFIER = b'3dbf4cbc-f015-43d9-b280-ff6962a22198'

# Number of digits to use for the root pointer.
# Note: 15 digits is approximately 900 TiB.. This should be enough.
DEFAULT_HEADER = 15  # type: int

# Maximum number of digits to accept for the root pointer.
MAX_HEADER = 15  # type: int


class StorageError(RuntimeError):
    pass


class CorruptedFormat(StorageError):
    pass


class ConcurrencyError(StorageError):
    pass


class Storage:

    __slots__ = ['__file', '__locked', '__lockable']

    def __init__(self, f) -> None:
        self.__file = f
        if not isinstance(self.__file, BytesIO):
            assert 'b' in self.__file.mode, \
                'The storage must be open in binary mode'
            # If this line causes problem, it can probably be
            # removed. It is to ensure that the file is opened without
            # buffering.
            assert hasattr(self.__file, '_blksize'), \
                'The file must be open without buffering (use buffering=0)'
        # We prevent recursive lock, because each lock state overwrites the
        # current one.  Each lock state is used to mark a given range of
        # bytes as either SH (shared), EX (exclusive) or UN (unset).
        self.__locked = False  # type: bool
        self.__lockable = not isinstance(self.__file, BytesIO)  # type: bool

    @staticmethod
    def _init(f) -> None:
        """
        Internal method. Reset a storage to an empty state.
        """
        f.truncate(0)
        f.seek(0)
        f.write('{}\n{:0{}d}\n'.format(IDENTIFIER.decode('ascii'), 0, DEFAULT_HEADER).encode('utf-8'))
        try:
            fd = f.fileno()
        except io.UnsupportedOperation:
            pass
        else:
            os.fsync(fd)

    @staticmethod
    def _create(filename: str) -> None:
        """
        Internal method. Create (or overwrite) an empty storage.
        """
        with open(filename, 'wb', buffering=0) as f:
            Storage._init(f)

    @staticmethod
    def open(filename: str, create_if_missing: bool=False, reset_if_exists: bool=False) -> 'Storage':
        """
        Open a storage.

        Args:
            filename -- (str) the filename of the storage to open.
            create_if_missing -- (bool) if True, the storage is
              created if it doesn't exist.
            reset_if_exists -- (bool) if True, the storage is reset to
              an empty state if already exists. This can be combined
              with create_if_missing to always recreate an empty
              storage, which is useful for testing purpose.
        """
        assert isinstance(filename, str)
        if reset_if_exists:
            try:
                os.unlink(filename)
            except FileNotFoundError:
                pass
            else:
                Storage._create(filename)
        elif not os.path.exists(filename) and create_if_missing:
            Storage._create(filename)
        # NOTE: This will throw if the file doesn't exist and if
        # neither reset_if_exists nor create_if_missing are set.
        # NOTE: Disabling buffering is important to avoid caches..
        f = open(filename, 'r+b', buffering=0)
        return Storage(f)

    @staticmethod
    def open_in_memory():
        """
        Create an empty storage in memory.

        This is especially useful for testing purpose.
        """
        f = BytesIO()
        Storage._init(f)
        return Storage(f)

    @contextmanager
    def __lock(self, *, write: bool=True):
        """
        A context manager to lock the storage for reading or writing.

        If no processes hold the lock for writing, then several
        processes can simultaneously lock the storage for reading.

        If a process holds the lock for writing, then no other process
        can take the lock (either for reading or writing).

        If the lock cannot be obtained, the process is paused until
        the situation allows to take it.

        Args:
            write -- (bool) if False, the storage is locked for
              reading. If True, the storage is locked for writing.
        """
        assert not self.__locked, 'Nested lock'
        if self.__lockable:
            flags = fcntl.LOCK_SH if not write else fcntl.LOCK_EX
            fcntl.lockf(self.__file, flags, 1)
            self.__locked = True
            try:
                yield
            finally:
                fcntl.lockf(self.__file, fcntl.LOCK_UN, 1)
                self.__locked = False
        else:
            yield

    @staticmethod
    def _parse_current_offset(line: bytes) -> int:
        """
        Parse the offset stored in the given line.

        Args:
            line -- (bytes) the line to parse.

        Returns:
            The offset given in the line, if the line is well
            formated.
        """
        assert line, 'Empty storage?'
        if not (2 <= len(line) <= MAX_HEADER+1 and line.endswith(b'\n') and line[:-1].isdigit()):
            raise CorruptedFormat('Invalid header ({!r})'.format(line))
        return int(line)

    def _getline(self, buffer=b''):
        p = buffer.find(b'\n')
        if p != -1:
            return buffer[:p+1], buffer[p+1:]
        while True:
            data = self.__file.read(4096)
            if not data:
                if not buffer:
                    return None, b''
                else:
                    raise RuntimeError('Unterminated line')
            p = data.find(b'\n')
            if p != -1:
                return buffer + data[:p+1], data[p+1:]
            buffer += data

    def get_current(self) -> int:
        """
        Get the offset of the current record.
        """
        # Maybe we don't need a lock, since the file is open with
        # unbuffered access.
        self.__file.seek(0, 0)
        with self.__lock(write=False):
            line, buffer = self._getline()
            assert line[:-1] == IDENTIFIER, 'Unrecognized file format'
            line, buffer = self._getline(buffer)
        return self._parse_current_offset(line)

    def set_current(self, offset: int, lease: Optional[int]=None) -> None:
        """
        Set the offset of the current record.

        Args:
            offset -- (int) the offset to write as the current offset.
            lease -- (None|int) if not None, write the offset only if
              the current one match this value. This acts as a
              Compare-And-Swap (CAS) operation. If the value is
              different, an exception is thrown.
        """
        self.__file.seek(0, 0)
        with self.__lock():
            line, buffer = self._getline()
            assert line[:-1] == IDENTIFIER, 'Unrecognized file format'
            pos = len(line)
            line, buffer = self._getline(buffer)
            current_offset = self._parse_current_offset(line)
            if lease is not None and current_offset != lease:
                raise ConcurrencyError('target={}, current={}, expected={}'.format(offset, current_offset, lease))
            new_line = '{:0{}d}\n'.format(offset, len(line)-1).encode('utf-8')
            assert len(new_line) == len(line), '{!r} vs {!r}'.format(new_line, line)
            self.__file.seek(pos, 0)
            self.__file.write(new_line)
            try:
                fd = self.__file.fileno()
            except io.UnsupportedOperation:
                pass
            else:
                os.fsync(fd)

    def load(self, offset: int) -> bytes:
        """
        Load the record at the given offset.

        Args:
            offset -- (int) the offset of the record to read.

        Returns:
            The record as a byte string.
        """
        self.__file.seek(offset)
        line, buffer = self._getline()
        if not line.startswith(b'\t'):
            raise CorruptedFormat('Missing marker at offset {}'.format(offset))
        if not line.endswith(b'\n'):
            raise CorruptedFormat('Unterminated line at offset {}'.format(offset))
        if line.find(b'\t', 1) != -1:
            raise CorruptedFormat('{} is not pointing at the beginning of a record'.format(offset))
        return line[1:-1]

    def store(self, record: bytes) -> int:
        r"""
        Store a record.

        Args:
            record -- (bytes) the record to store. Any sequence of
              bytes, not containing TAB ('\t') or NL ('\n'), is
              allowed.

        Returns:
            The record offset.
        """
        with self.__lock():
            self.__file.seek(0, 2)
            pos = self.__file.tell()
            # "0\n" is the minimum possible header (even if almost useless)
            assert pos >= 2, 'Empty storage?'
            assert b'\t' not in record, 'TAB (\\t) characters are forbidden in record.'
            assert b'\n' not in record, 'NL (\\n) characters are forbidden in record.'
            self.__file.write(b'\t' + record + b'\n')
            self.__file.flush()
        return pos

    def scan(self):
        """
        Check the integrity of the storage.

        Throw:
            A CorruptedFormat exception if an error is found.
        """
        self.__file.seek(0)
        with self.__lock():
            i = 1
            offset = 0
            line = self.__file.readline()
            if not line:
                raise CorruptedFormat('Empty file')
            if not line.endswith(b'\n'):
                raise CorruptedFormat('Unterminated line at offset {} (line {})'.format(offset, i))
            if line[:-1] != IDENTIFIER:
                raise CorruptedFormat('Identifier not found at offset {} (line {})'.format(offset, i))
            offset = self.__file.tell()
            line = self.__file.readline()
            if not line.endswith(b'\n'):
                raise CorruptedFormat('Unterminated line at offset {} (line {})'.format(offset, i))
            self._parse_current_offset(line)  # Used for checking syntax
            i += 1
            while 1:
                offset = self.__file.tell()
                line = self.__file.readline()
                if not line:
                    break
                if not line.endswith(b'\n'):
                    raise CorruptedFormat('Unterminated line at offset {} (line {})'.format(offset, i))
                if not line.startswith(b'\t'):
                    raise CorruptedFormat('Missing marker at offset {} (line {})'.format(offset, i))
                if line.find(b'\t', 1) != -1:
                    raise CorruptedFormat('Marker find within a record at offset {} (line {})'.format(offset, i))
                i += 1

    def records(self):
        """
        Iterate over all the records contained in the storage.

        Yields:
            First the "current offset" line, then each records.
        """
        self.__file.seek(0)
        with self.__lock():
            offset = 0
            line = self.__file.readline()
            if not line.endswith(b'\n'):
                raise CorruptedFormat('Unterminated line at offset {}'.format(offset))
            if line[:-1] != IDENTIFIER:
                raise CorruptedFormat('Identifier not found on the first line {!r}')
            yield offset, line[:-1]
            offset = self.__file.tell()
            line = self.__file.readline()
            if not line.endswith(b'\n'):
                raise CorruptedFormat('Unterminated line at offset {}'.format(offset))
            yield offset, line[:-1]
            while 1:
                offset = self.__file.tell()
                line = self.__file.readline()
                if not line:
                    break
                if not line.endswith(b'\n'):
                    raise CorruptedFormat('Unterminated line at offset {}'.format(offset))
                if not line.startswith(b'\t'):
                    raise CorruptedFormat('Missing marker at offset {}'.format(offset))
                if line.find(b'\t', 1) != -1:
                    raise CorruptedFormat('Marker find within a record at offset {}'.format(offset))
                yield offset, line[1:-1]

    def _dump(self, printer):
        for offset, record in self.records():
            printer('{:4d} | {}'.format(offset, record.decode('utf-8')))

    def dump(self):
        self._dump(Printer())


if __name__ == '__main__':
    pass
