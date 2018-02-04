"""HFS (Hash File System) is a file system based on the idea of hash tree.

    |         ( Application Layer )         |
    +---------------------------------------+
    |         Structure Layer: HFS          |
    +---------------------------------------+
    |          Storage Layer: Pool          |
    +-------+---------------+---------+-----+
    | Cache | Underlying FS | Network | ... |
    +-------+---------------+---------+-----+

The Storage Layer can store and load blob objects by their hash value.
The Structure Layer builds special blob objects to manage the file system
structure, like metadata or directories.

This module provides the HFS class (on the Structure Layer) and a simple
LocalPool class (on the Storage Layer).
"""
__version__ = '0.1'
__all__ = [
    'HFS',
    'Node',
    'FileNode',
    'ListNode',
    'SetNode',
    'MapNode',
    'LocalPool',
    'HASH',
    'HASHLEN',
    'PACKLIMIT',
]
__author__ = 'Hypercube <hypercube@0x01.me>'

import io
import hashlib
import os
import pathlib
import pickle
import sys
import tempfile

if sys.version_info < (3, 6):
    import sha3

HASH = hashlib.sha3_256
HASHLEN = len(HASH().hexdigest())

PACKLIMIT = 1024


# According to https://eklitzke.org/efficient-file-copying-on-linux ,
# 128 KiB is the best. But my benchmark on a 64-bit computer with both
# SSD and HDD shows that 256 KiB or 512 KiB is the best.
# I also noticed that using the default buffer size of `open` is better
# than setting it to this value.
# There's no much difference using `readinto` on `memoryview(bytearray())`
# or just `read`.
def iomap(func, file, blksize=256 * 1024):
    if not callable(func):
        funcs = func
        func = lambda b: [f(b) for f in funcs]
    buffer = file.read(blksize)
    while buffer:
        func(buffer)
        buffer = file.read(blksize)


class HFS:
    """An HFS, which provides high-level file system APIs.

    This implementation does not contain cache, because caches prevent
    multiple HFS instances of the same HFS running at the same time.
    """

    def __init__(self, pool, root='0' * HASHLEN):
        """Get an HFS object with the given Pool object.

        root means the hash value of the root node.  The default value is
        all zero.  Normally this is a special detached node representing
        the local root.
        """
        self._pool = pool
        self._root = root

    def __repr__(self):
        return '%s(%r, %r)' % (type(self).__name__, self._pool, self._root)

    def __call__(self, item):
        """Put an object into the pool.

        Support list, set, dict, strings, binary file-like objects, and
        node-like objects.
        """
        if isinstance(item, list):
            item = ''.join(map('%s\n'.__mod__, item))
        elif isinstance(item, set):
            item = ''.join(map('%s\n'.__mod__, sorted(item)))
        elif isinstance(item, dict):
            item = ''.join(map('%s: %s\n'.__mod__, sorted(item.items())))
        if isinstance(item, str):
            item = item.encode('utf8', errors='surrogateescape')
        if isinstance(item, (bytes, bytearray, io.BufferedIOBase)):
            return self._pool(item)
        else:
            return item.commit(self)

    def __getitem__(self, key):
        """Get an object by its hash value.

        hfs[<hash value>] -> a binary file-like object
        hfs[<hash value>:str] -> a str
        hfs[<hash value>:bytes] -> a bytes
        """
        if not isinstance(key, slice):
            return self._pool[key]
        data = self._pool[key.start].read()
        if issubclass(key.stop, str):
            data = data.decode('utf8', errors='surrogateescape')
        return key.stop(data)

    def open(self, path):
        """Get a node by its read-only mount path."""
        path = pathlib.PurePosixPath(path)
        pos = Node.load(self, self[self._root:str])
        for part in path.parts:
            if part == '/':
                continue
            pos = Node.load(self, self[pos[part]:str])
        return pos

    def getsize(self, key):
        """Get the size of an object."""
        return self._pool.getsize(key)

    def flush(self):
        """Ensure all the data has been stored safely."""
        self._pool.flush()


class Node:
    """The abstract class of nodes in HFS.

    I think this should be an ABC, but Fluent Python says I shouldn't
    create them.  -- It even has a register method!
    """
    _types = {}

    def __init__(self, data, **attrs):
        """Create a node.  See help(self) for the actual signature."""
        self._data = data
        self._attrs = attrs
        self._attrs['_node'] = self.__node__
        self._size = None

    def commit(self, hfs):
        """Commit this node into an HFS."""
        c = {k: hfs(v) for k, v in self._attrs.items()}
        c['_data'] = self._data
        self._size = hfs.getsize(self._data)
        return hfs(c)

    @property
    def data(self):
        """The hash value of the actual data."""
        return self._attrs['_data']

    @property
    def size(self):
        """The size of the actual data."""
        return self._size

    @property
    def time(self):
        """The creation time."""
        return float(self._attrs.get('time', 0))

    @property
    def access(self):
        """The POSIX file type and mode."""
        return int(self._attrs.get('access', '777'), 8)

    @property
    def uid(self):
        """The POSIX owner user id."""
        return int(self._attrs.get('uid', 0))

    @property
    def gid(self):
        """The POSIX owner group id."""
        return int(self._attrs.get('gid', 0))

    @property
    def nlink(self):
        """The POSIX file system link count."""
        return 1

    @classmethod
    def register(cls, subcls):
        """Register a concrete subclass."""
        cls._types[HASH(subcls.__node__.encode()).hexdigest()] = subcls
        return subcls

    @classmethod
    def load(cls, hfs, data):
        """Build a node from the string representation of its metadata."""
        attrs = {}
        for line in data.splitlines():
            k, _, v = line.partition(': ')
            attrs[k] = v
        node = cls._types[attrs['_node']].parse(hfs, attrs)
        node._size = hfs.getsize(attrs['_data'])
        return node


@Node.register
class FileNode(Node):
    """FileNode(<hash value of the blob>, **attrs)"""
    __node__ = 'file'

    @classmethod
    def parse(cls, hfs, attrs):
        """Build a node from its metadata."""
        return cls(attrs['_data'], **attrs)

    @property
    def access(self):
        """The POSIX file type and mode."""
        if 'access' in self._attrs:
            return int(self._attrs['access'], 8)
        if 'exec' in self._attrs:
            return 0o100777
        return 0o100666


class ContainerNode(Node):
    def __iter__(self):
        """Iter over the read-only mount paths."""
        for k in self._data:
            yield k

    def __getitem__(self, key):
        """Get an item's hash value by its read-only mount path."""
        return self._data[key]

    def commit(self, hfs):
        """Commit this node into an HFS."""
        c = {k: hfs(v) for k, v in self._attrs.items()}
        c['_data'] = hfs(self._data)
        self._size = hfs.getsize(c['_data'])
        return hfs(c)

    @property
    def access(self):
        """The POSIX file type and mode."""
        return int(self._attrs.get('access', '40777'), 8)

    @property
    def nlink(self):
        """The POSIX file system link count."""
        return len(self._data) + 2


@Node.register
class ListNode(ContainerNode):
    """ListNode([<hash value of the node>], **attrs)"""
    __node__ = 'list'

    @classmethod
    def parse(cls, hfs, attrs):
        """Build a node from its metadata."""
        data = hfs[attrs['_data']:str].splitlines()
        return cls(data, **attrs)

    def __iter__(self):
        """Iter over the read-only mount paths."""
        for i in range(len(self._data)):
            yield str(i)

    def __getitem__(self, key):
        """Get an item's hash value by its read-only mount path."""
        return self._data[int(key)]


@Node.register
class SetNode(ContainerNode):
    """SetNode({<hash value of the node>}, **attrs)"""
    __node__ = 'set'

    @classmethod
    def parse(cls, hfs, attrs):
        """Build a node from its metadata."""
        data = set(hfs[attrs['_data']:str].splitlines())
        return cls(data, **attrs)

    def __getitem__(self, key):
        """Get an item's hash value by its read-only mount path."""
        return key


@Node.register
class MapNode(ContainerNode):
    """MapNode({<name>: <hash value of the node>},
            **attrs)"""
    __node__ = 'map'

    def commit(self, hfs):
        """Commit this node into an HFS."""
        c = {k: hfs(v) for k, v in self._attrs.items()}
        data = {hfs(k): v for k, v in self._data.items()}
        c['_data'] = hfs(data)
        self._size = hfs.getsize(c['_data'])
        return hfs(c)

    @classmethod
    def parse(cls, hfs, attrs):
        """Build a node from its metadata."""
        data = {hfs[line[:HASHLEN]:str]: line[-HASHLEN:] for line
                in hfs[attrs['_data']:str].splitlines()}
        return cls(data, **attrs)


class LocalPool:
    """An HFS pool, which maps all the hash values to the objects.

    This pool is based on a local directory.  Objects will be created in a
    temp directory named `_`, before moved into pool with the hash value as
    name.  For better performance, once a directory has more than 256
    entries, new objects will be put into subdirectories named with the
    first 2 digits of the hash value, thus no directory will contain more
    than 512 entries.
    """

    def __init__(self, path):
        self._path = pathlib.Path(path)
        self._temp = self._path / '_'
        if not self._temp.exists():
            self._temp.mkdir()
        packpath = self._path / '_pack.pickle'
        if packpath.exists():
            with packpath.open('rb') as f:
                self._pack = pickle.load(f)
        else:
            self._pack = {}

    def __repr__(self):
        return '%s(%r)' % (type(self).__name__, str(self._path))

    def __call__(self, item):
        """Put a string or a binary file-like object into the pool."""
        if isinstance(item, (bytes, bytearray)):
            key = HASH(item).hexdigest()
            if len(item) < PACKLIMIT:
                self._pack[key] = bytes(item)
                return key
            path = self / key
            if not path.exists():
                with tempfile.NamedTemporaryFile(
                        dir=str(self._temp), delete=False) as f:
                    f.write(item)
                os.rename(f.name, str(path))
            return key
        if item.seekable():
            item.seek(0)
            hashobj = HASH()
            iomap(hashobj.update, item)
            key = hashobj.hexdigest()
            path = self / key
            if not path.exists():
                item.seek(0)
                with tempfile.NamedTemporaryFile(
                        dir=str(self._temp), delete=False) as f:
                    iomap(f.write, item)
                os.rename(f.name, str(path))
            return key
        else:
            hashobj = HASH()
            with tempfile.NamedTemporaryFile(
                    dir=str(self._temp), delete=False) as f:
                iomap((hashobj.update, f.write), item)
            key = hashobj.hexdigest()
            path = self / key
            if path.exists():
                os.remove(f.name)
            else:
                os.rename(f.name, str(path))
            return key

    def __truediv__(self, other):
        """For internal-use only."""
        path = self._path
        for i in range(0, HASHLEN, 2):
            if (path / other[i:]).exists():
                return path / other[i:]
            path /= other[i:i + 2]
            if not path.exists():
                count = len(list(path.parent.iterdir()))
                if count < 250:
                    return path.parent / other[i:]
                path.mkdir()
                return path / other[i + 2:]

    def __getitem__(self, key):
        """Get an object as a binary file-like object."""
        if key in self._pack:
            return io.BytesIO(self._pack[key])
        path = self / key
        if not path.exists():
            raise KeyError(key)
        return path.open('rb')

    def getsize(self, key):
        """Get the size of an object."""
        if key in self._pack:
            return len(self._pack[key])
        path = self / key
        return os.path.getsize(str(path)) if path.exists() else 0

    def flush(self):
        """Save the packed data."""
        packpath = self._path / '_pack.pickle'
        if packpath.exists():
            with packpath.open('rb') as f:
                self._pack.update(pickle.load(f))
        with tempfile.NamedTemporaryFile(
                dir=str(self._temp), delete=False) as f:
            pickle.dump(self._pack, f)
        os.rename(f.name, str(self._path / '_pack.pickle'))
