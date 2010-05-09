# Authors:
#   Jason Gerard DeRose <jderose@jasonderose.org>
#
# media: distributed media library
# Copyright (C) 2010 Jason Gerard DeRose <jderose@jasonderose.org>
#
# This file is part of `media`.
#
# `media` is free software: you can redistribute it and/or modify it under the
# terms of the GNU Lesser General Public License as published by the Free
# Software Foundation, either version 3 of the License, or (at your option) any
# later version.
#
# `media` is distributed in the hope that it will be useful, but WITHOUT ANY
# WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR
# A PARTICULAR PURPOSE.  See the GNU Lesser General Public License for more
# details.
#
# You should have received a copy of the GNU Lesser General Public License along
# with `media`.  If not, see <http://www.gnu.org/licenses/>.

"""
Unit tests for `medialib.filestore` module.
"""

import os
from os import path
import hashlib
from helpers import TempDir, TempHome
from medialib import filestore


letters = 'gihdwaqoebxtcklrnsmjufyvpz'
extensions = ('png', 'jpg', 'mov')
key = 'dc40522fe5e70b2fed5737d1beb29da87b0072cc275e086ebed30cb83ecf1dd8.iso'

def get_dir():
    return path.join(os.environ['HOME'], '.local', 'share', 'media')


def test_hash_file():
    f = filestore.hash_file
    tmp = TempDir()
    msg = 'I have a colon full of cookie'
    src = tmp.write(msg, 'ihacfoc.txt')
    assert f(src) == hashlib.sha256(msg).hexdigest()
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
        name = '.'.join([l, ext])
        names.append(name)
        tmp.touch('subdir', name)

    files = list(f(tmp.path, extensions))
    assert files == [tmp.join('subdir', name) for name in sorted(names)]


class test_FileStore(object):
    klass = filestore.FileStore

    def test_init(self):
        inst = self.klass()
        assert inst.mediadir == get_dir()

    def test_resolve(self):
        inst = self.klass()
        assert inst.resolve(key) == path.join(get_dir(), 'dc', key)

    def test_add(self):
        h = TempHome()
        inst = self.klass()
        assert inst.mediadir.startswith(h.path)

        msg = 'I have a colon full of cookie'
        digest = hashlib.sha256(msg).hexdigest()
        chn = digest + '.txt'
        src = h.write(msg, 'ihacfoc.txt')
        assert inst.add(src) == ('linked', src, chn)
        assert inst.add(src) == ('skipped', src, chn)

        dst = path.join(inst.mediadir, digest[:2], chn)
        assert path.isfile(dst)
        assert open(dst, 'r').read() == msg
