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
Unit tests for `dmedialib.filestore` module.
"""

import os
from os import path
import hashlib
from helpers import TempDir, TempHome
from dmedialib import filestore


letters = 'gihdwaqoebxtcklrnsmjufyvpz'
extensions = ('png', 'jpg', 'mov')
key = '4e3a57109f226b07fe00e0abac88544b2e8331d0ec47ee00340138dd.iso'
dname = '4e'
fname = '3a57109f226b07fe00e0abac88544b2e8331d0ec47ee00340138dd.iso'

def get_dir():
    return path.join(os.environ['HOME'], '.local', 'share', 'media')


def test_hash_file():
    f = filestore.hash_file
    tmp = TempDir()
    msg = 'I have a colon full of cookie'
    src = tmp.write(msg, 'ihacfoc.txt')
    assert f(src) == hashlib.sha224(msg).hexdigest()
    assert f(src, hashfunc=hashlib.sha384) == hashlib.sha384(msg).hexdigest()


def test_scanfiles():
    f = filestore.scanfiles
    tmp = TempDir()
    assert list(f(tmp.path)) == []
    somefile = tmp.touch('somefile.txt')
    assert list(f(somefile)) == []

    # Create files in a non-alphabetic order:
    names = []
    for (i, l) in enumerate(letters):
        ext = extensions[i % len(extensions)]
        name = '.'.join([l, ext.upper()])
        names.append(name)
        tmp.touch('subdir', name)

    files = list(f(tmp.path, extensions))
    assert files == list(
        {
            'src': tmp.join('subdir', name),
            'meta': {
                'name': name,
                'ext': name.split('.')[1].lower(),
            },
        }
        for name in sorted(names)
    )


class test_FileStore(object):
    klass = filestore.FileStore

    def test_init(self):
        inst = self.klass()
        assert inst.mediadir == get_dir()

    def test_resolve(self):
        inst = self.klass()
        assert inst.resolve(key) == path.join(get_dir(), dname, fname)

    def test_add(self):
        h = TempHome()
        inst = self.klass()
        assert inst.mediadir.startswith(h.path)

        msg = 'I have a colon full of cookie'
        hexdigest = hashlib.sha224(msg).hexdigest()
        dst = path.join(inst.mediadir, hexdigest[:2], hexdigest[2:])
        src = h.write(msg, 'ihacfoc.txt')
        assert inst.add(src) == {
            'action': 'linked',
            'src': src,
            'dst': dst,
            'meta': {
                '_id': hexdigest,
                'name': 'ihacfoc.txt',
                'ext': 'txt',
                'size': path.getsize(src),
                'mtime': path.getmtime(src),
            },
        }
        assert inst.add(src) == {
            'action': 'skipped_duplicate',
            'src': src,
            'dst': dst,
            'meta': {
                '_id': hexdigest,
                'name': 'ihacfoc.txt',
                'ext': 'txt',
            },
        }
        assert path.isfile(dst)
        assert open(dst, 'r').read() == msg

    def test_do_add(self):
        h = TempHome()
        inst = self.klass()
        assert inst.mediadir.startswith(h.path)

        msg = 'I have a colon full of cookie'
        hexdigest = hashlib.sha224(msg).hexdigest()
        src = h.write(msg, 'ihacfoc.txt')
        dst = path.join(inst.mediadir, hexdigest[:2], hexdigest[2:])
        meta = {
            '_id': hexdigest,
            'size': path.getsize(src),
            'mtime': path.getmtime(src),
        }
        assert inst._do_add({'src': src}) == {
            'action': 'linked',
            'src': src,
            'dst': dst,
            'meta': meta,
        }
        assert inst._do_add({'src': src}) == {
            'action': 'skipped_duplicate',
            'src': src,
            'dst': dst,
            'meta': {
                '_id': hexdigest,
            },
        }
        assert path.isfile(dst)
        assert open(dst, 'r').read() == msg
