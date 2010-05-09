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
import time


CHUNK = 2 ** 20  # Read in chunks of 1 MiB
MEDIA_DIR = ('.local', 'share', 'dmedia')


def hash_file(filename, hashfunc=hashlib.sha224):
    """
    Compute the content-hash of the file *filename*.
    """
    fp = open(filename, 'rb')
    h = hashfunc()
    while True:
        chunk = fp.read(CHUNK)
        if not chunk:
            break
        h.update(chunk)
    return h.hexdigest()


def normalize_ext(name):
    """
    Return extension from *name* normalized to lower-case.

    If *name* has no extension, ``None`` is returned.  For example:

    >>> normalize_ext('IMG_2140.CR2')
    'cr2'
    >>> normalize_ext('test.jpg')
    'jpg'
    >>> normalize_ext('hello_world') is None
    True
    """
    parts = name.rsplit('.', 1)
    if len(parts) == 2:
        return parts[1].lower()


def scanfiles(base, extensions=None):
    """
    Recursively iterate through files in directory *base*.
    """
    try:
        names = sorted(os.listdir(base))
    except StandardError:
        return
    for name in names:
        if name.startswith('.'):
            continue
        fullname = path.join(base, name)
        if path.islink(fullname):
            continue
        if path.isfile(fullname):
            ext = normalize_ext(name)
            if extensions is None or ext in extensions:
                yield {
                    'src': fullname,
                    'meta': {
                        'name': name,
                        'ext': ext,
                    },
                }
        elif path.isdir(fullname):
            for d in scanfiles(fullname, extensions):
                yield d


class FileStore(object):
    def __init__(self, mediadir=None):
        if mediadir is None:
            mediadir = path.join(os.environ['HOME'], *MEDIA_DIR)
        self.mediadir = mediadir

    def resolve(self, hexdigest, create_parent=False):
        dname = hexdigest[:2]
        fname = hexdigest[2:]
        if create_parent:
            d = path.join(self.mediadir, dname)
            if not path.exists(d):
                os.makedirs(d)
        return path.join(self.mediadir, dname, fname)

    def _do_add(self, d):
        """
        Low-level add operation.

        Used by both `FileStore.add()` and `FileStore.add_recursive()`.
        """
        src = d['src']
        hexdigest = hash_file(src)
        if 'meta' not in d:
            d['meta'] = {}
        meta = d['meta']
        meta['_id'] = hexdigest
        dst = self.resolve(hexdigest, create_parent=True)
        d['dst'] = dst

        # If file already exists, return a 'skipped_duplicate' action
        if path.exists(dst):
            d['action'] = 'skipped_duplicate'
            return d

        # Otherwise copy or hard-link into mediadir:
        meta['size'] = path.getsize(src)
        meta['mtime'] = path.getmtime(src)
        if os.stat(src).st_dev == os.stat(self.mediadir).st_dev:
            os.link(src, dst)
            d['action'] = 'linked'
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
