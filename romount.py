#!/usr/bin/python3

import os
from errno import errorcode, ENOENT, EINVAL
from fusepy import FUSE, FuseOSError, Operations

from hfs import HFS, LocalPool


def logmethod(func):
    # return func  # comment to enable log
    def wrapped(*args):
        try:
            result = func(*args)
        except FuseOSError as e:
            print('%s(%s) -> %s' % (func.__name__,
                                    ', '.join(map(repr, args[1:])),
                                    errorcode[e.args[0]]))
            raise e
        if func.__name__ not in ('getattr', 'read'):
            if result is None:
                print('%s(%s)' % (func.__name__,
                                  ', '.join(map(repr, args[1:]))))
            else:
                print('%s(%s) = %r' % (func.__name__,
                                       ', '.join(map(repr, args[1:])), result))
        else:
            print('%s(%s) = ...' % (func.__name__,
                                    ', '.join(map(repr, args[1:]))))
        return result

    return wrapped


class HFSFuse(Operations):

    def __init__(self, hfs):
        self.hfs = hfs
        self.opened = {}

    @logmethod
    def getattr(self, path, fh=None):
        try:
            node = self.hfs.open(path)
        except KeyError:
            raise FuseOSError(ENOENT)
        return {'st_mode': node.access,
                'st_nlink': node.nlink,
                'st_uid': node.uid,
                'st_gid': node.gid,
                'st_size': node.size,
                'st_blocks': (node.size + 511) // 512,
                'st_atime': node.time,
                'st_mtime': node.time,
                'st_ctime': node.time}

    @logmethod
    def readdir(self, path, fh):
        try:
            node = self.hfs.open(path)
        except KeyError:
            raise FuseOSError(ENOENT)
        result = ['.', '..']
        result.extend(node)
        return result

    @logmethod
    def open(self, path, flags):
        if flags & ~(32768 | os.O_ACCMODE | os.O_NONBLOCK):
            raise FuseOSError(EINVAL)
        accmode = flags & os.O_ACCMODE
        if accmode == os.O_RDONLY:
            try:
                node = self.hfs.open(path)
            except KeyError:
                raise FuseOSError(ENOENT)
            handle = os.open('/dev/null', flags)
            self.opened[handle] = self.hfs[node.data]
            return handle
        else:
            raise FuseOSError(EINVAL)

    @logmethod
    def read(self, path, length, offset, fh):
        self.opened[fh].seek(offset)
        return self.opened[fh].read(length)

    @logmethod
    def release(self, path, fh):
        self.opened[fh].close()
        os.close(fh)


if __name__ == '__main__':
    from sys import argv, exit

    if len(argv) < 4:
        exit('Usage: romount.py <pool path> <root hash> <mount point>')
    FUSE(HFSFuse(HFS(LocalPool(argv[1]), argv[2])), argv[3],
         nothreads=True, foreground=True)
