# Authors:
#   Jason Gerard DeRose <jderose@jasonderose.org>
#
# dmedia: distributed media library
# Copyright (C) 2010 Jason Gerard DeRose <jderose@jasonderose.org>
#
# This file is part of `dmedia`.
#
# `dmedia` is free software: you can redistribute it and/or modify it under the
# terms of the GNU Lesser General Public License as published by the Free
# Software Foundation, either version 3 of the License, or (at your option) any
# later version.
#
# `dmedia` is distributed in the hope that it will be useful, but WITHOUT ANY
# WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR
# A PARTICULAR PURPOSE.  See the GNU Lesser General Public License for more
# details.
#
# You should have received a copy of the GNU Lesser General Public License along
# with `dmedia`.  If not, see <http://www.gnu.org/licenses/>.

"""
Store media files based on content-hash.
"""

import os
from os import path
import shutil
import hashlib
from hashlib import sha1
from base64 import b32encode
import mimetypes


mimetypes.init()


CHUNK = 2 ** 20  # Read in chunks of 1 MiB
DOTDIR = '.dmedia'


def normalize_ext(name):
    """
    Return (root, ext) from *name* where extension is normalized to lower-case.

    If *name* has no extension, ``None`` is returned as 2nd item in (root, ext)
    tuple:

    >>> normalize_ext('IMG_2140.CR2')
    ('IMG_2140', 'cr2')
    >>> normalize_ext('test.jpg')
    ('test', 'jpg')
    >>> normalize_ext('hello_world')
    ('hello_world', None)
    """
    parts = name.rsplit('.', 1)
    if len(parts) == 2:
        return (parts[0], parts[1].lower())
    return (parts[0], None)


def scanfiles(base, extensions=None):
    """
    Recursively iterate through files in directory *base*.
    """
    try:
        names = sorted(os.listdir(base))
    except StandardError:
        return
    for name in names:
        if name.startswith('.') or name.endswith('~'):
            continue
        fullname = path.join(base, name)
        if path.islink(fullname):
            continue
        if path.isfile(fullname):
            (root, ext) = normalize_ext(name)
            if extensions is None or ext in extensions:
                yield {
                    'src': fullname,
                    'base': base,
                    'root': root,
                    'meta': {
                        'name': name,
                        'ext': ext,
                    },
                }
        elif path.isdir(fullname):
            for d in scanfiles(fullname, extensions):
                yield d


class FileNotFound(StandardError):
    def __init__(self, chash, extension):
        self.chash = chash
        self.extension = extension


class FileStore(object):
    def __init__(self, user_dir=None, shared_dir=None):
        self.home = path.abspath(os.environ['HOME'])
        if user_dir is None:
            user_dir = path.join(self.home, DOTDIR)
        if shared_dir is None:
            shared_dir = path.join('/home', DOTDIR)
        self.user_dir = path.abspath(user_dir)
        self.shared_dir = path.abspath(shared_dir)

    def chash(self, filename=None, fp=None):
        """
        Compute the content-hash of the file at *filename*.

        Note that dmedia will migrate to the skein-512-240 hash after the
        Threefish constant change. See:

          http://blog.novacut.com/2010/09/how-about-that-skein-hash.html

        For example:
        >>> from StringIO import StringIO
        >>> fp = StringIO()
        >>> fp.write('Novacut')
        >>> fp.seek(0)
        >>> store = FileStore()
        >>> store.chash(fp=fp)
        'NWBNVXVK5DQGIOW7MYR4K3KA5K22W7NW'
        """
        if filename:
            fp = open(filename, 'rb')
        h = sha1()
        while True:
            chunk = fp.read(CHUNK)
            if not chunk:
                break
            h.update(chunk)
        return b32encode(h.digest())

    def relname(self, chash, extension=None):
        """
        Relative path components for file with *chash*, ending with *extension*.

        For example:

        >>> fs = FileStore()
        >>> fs.relname('NWBNVXVK5DQGIOW7MYR4K3KA5K22W7NW')
        ('NW', 'BNVXVK5DQGIOW7MYR4K3KA5K22W7NW')
        >>> fs.relname('NWBNVXVK5DQGIOW7MYR4K3KA5K22W7NW', extension='txt')
        ('NW', 'BNVXVK5DQGIOW7MYR4K3KA5K22W7NW.txt')
        """
        dname = chash[:2]
        fname = chash[2:]
        if extension:
            return (dname, '.'.join((fname, extension)))
        return (dname, fname)

    def mediadir(self, shared=False):
        """
        Returns user_dir or shared_dir based on *shared* flag.

        By default *shared* is ``False``.  For example:

        >>> fs = FileStore(user_dir='/foo', shared_dir='/bar')
        >>> fs.mediadir()
        '/foo'
        >>> fs.mediadir(shared=True)
        '/bar'
        """
        if shared:
            return self.shared_dir
        return self.user_dir

    def fullname(self, chash, extension=None, shared=False):
        """
        Returns path of file with *chash* and *extension*.

        If *shared* is ``True``, a path in the shared location is returned.
        Otherwise the a path in the user's private dmedia store is returned.

        For example:

        >>> fs = FileStore('/foo', '/bar')
        >>> fs.fullname('NWBNVXVK5DQGIOW7MYR4K3KA5K22W7NW', 'txt')
        '/foo/NW/BNVXVK5DQGIOW7MYR4K3KA5K22W7NW.txt'
        >>> fs.fullname('NWBNVXVK5DQGIOW7MYR4K3KA5K22W7NW', 'txt', shared=True)
        '/bar/NW/BNVXVK5DQGIOW7MYR4K3KA5K22W7NW.txt'
        """
        return path.join(
            self.mediadir(shared), *self.relname(chash, extension)
        )

    def locate(self, chash, extension=None):
        """
        Attempt to locate file with *chash* and *extension*.
        """
        user = self.fullname(chash, extension)
        if path.isfile(user):
            return user
        shared = self.fullname(chash, extension, shared=True)
        if path.isfile(shared):
            return shared
        raise FileNotFound(chash, extension)

    def _do_add(self, d, shared=False):
        """
        Low-level add operation.

        Used by both `FileStore.add()` and `FileStore.add_recursive()`.
        """
        src = d['src']
        chash = self.chash(src)
        if 'meta' not in d:
            d['meta'] = {}
        meta = d['meta']
        meta['_id'] = chash
        dst = self.fullname(chash, meta.get('ext'), shared)
        d['dst'] = dst

        # If file already exists, return a 'skipped_duplicate' action
        if path.exists(dst):
            d['action'] = 'skipped_duplicate'
            return d

        # Otherwise copy or hard-link into mediadir:
        parent = path.dirname(dst)
        if not path.exists(parent):
            os.makedirs(parent)

        meta['bytes'] = path.getsize(src)
        meta['mtime'] = path.getmtime(src)
        if meta.get('ext') is not None:
            meta['mime'] = mimetypes.types_map.get('.' + meta['ext'])
        if os.stat(src).st_dev == os.stat(self.mediadir(shared)).st_dev:
            os.link(src, dst)
            d['action'] = 'linked'
            if src.startswith(self.home):
                meta['links'] = [path.relpath(src, self.home)]
            else:
                meta['links'] = [src]
        else:
            shutil.copy2(src, dst)
            d['action'] = 'copied'
        try:
            os.chmod(dst, 0o444)
        except OSError:
            pass
        return d

    def add(self, src, ext=None):
        src = path.abspath(src)
        name = path.basename(src)
        if ext is None:
            ext = normalize_ext(name)
        else:
            ext = ext.lower()
        d = {
            'src': src,
            'meta': {
                'name': name,
                'ext': ext,
            }
        }
        return self._do_add(d)

    def add_recursive(self, base, extensions=None):
        base = path.abspath(base)
        for d in scanfiles(base, extensions):
            yield self._do_add(d)
