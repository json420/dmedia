# Authors:
#   Jason Gerard DeRose <jderose@novacut.com>
#
# dmedia: distributed media library
# Copyright (C) 2011 Jason Gerard DeRose <jderose@novacut.com>
#
# This file is part of `dmedia`.
#
# `dmedia` is free software: you can redistribute it and/or modify it under the
# terms of the GNU Affero General Public License as published by the Free
# Software Foundation, either version 3 of the License, or (at your option) any
# later version.
#
# `dmedia` is distributed in the hope that it will be useful, but WITHOUT ANY
# WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR
# A PARTICULAR PURPOSE.  See the GNU Affero General Public License for more
# details.
#
# You should have received a copy of the GNU Affero General Public License along
# with `dmedia`.  If not, see <http://www.gnu.org/licenses/>.

"""
Usefull TestCase subclasses.
"""

from unittest import TestCase
from os import path
from base64 import b64decode
import os
from os import path
import tempfile
import shutil
from random import SystemRandom
from zipfile import ZipFile

from filestore import File, Leaf, ContentHash, Batch, Hasher, LEAF_SIZE
from filestore import scandir
from microfiber import random_id


datadir = path.join(path.dirname(path.abspath(__file__)), 'data')
random = SystemRandom()


class DummyQueue(object):
    def __init__(self):
        self.items = []

    def put(self, item):
        self.items.append(item)


class SampleFilesTestCase(TestCase):
    """
    Base clase for tests that use the files in dmedia/tests/data.

    If the MVI_5751.MOV or MVI_5751.THM file isn't present, self.skipTest() is
    called.  This will allow us to stop shipping the 20MB video file in the
    dmedia release tarballs.
    """

    mov = path.join(datadir, 'MVI_5751.MOV')
    thm = path.join(datadir, 'MVI_5751.THM')
    mov_ch = ContentHash(
        'SM3GS4DUDVXOEU2DTTTWU5HKNRK777IWNSI5UQ4ZWNQGRXAN',
        20202333,
        b64decode(b''.join([
            b'Ps9ZlZ5RALOGrqUbXJYJDFJaLClkGKkv4gYu2cWn',
            b'dse+QPUQFn9Q6FBkhhX0hjDGHOyMnFtGdAgRY1Gc',
            b'XzjjVS002vjsMkVKb4/+E7qmeGfHsBFFbYV127ux'
        ]))
    )
    thm_ch = ContentHash(
        'MXPCFNUNPDAWHQWC5QNTPP2U5OF2J267QQVALXX6B5TRJKJB',
        27328,
        b64decode(b'RwtCvXTjDrah3O23qNkobCGNF6hq7HYIB4TRx2Dh'),   
    )

    def setUp(self):
        for filename in (self.mov, self.thm):
            if not path.isfile(filename):
                self.skipTest('Missing file {!r}'.format(filename))


class MagicLanternTestCase(TestCase):
    sample_zip = path.join(datadir, 'EOS_DIGITAL 550D ML Dump.zip')

    def setUp(self):
        for filename in [self.sample_zip]:
            if not path.isfile(filename):
                self.skipTest('Missing file {!r}'.format(filename))
        z = ZipFile(self.sample_zip)
        self.tmp = TempDir()
        z.extractall(path=self.tmp.dir)
        self.assertEqual(
            sorted(os.listdir(self.tmp.dir)),
            ['EOS_DIGITAL 550D ML Dump', '__MACOSX']
        )
        d = self.tmp.join('EOS_DIGITAL 550D ML Dump')
        self.assertTrue(path.isdir(d))
        self.batch = scandir(d)
        self.assertEqual(self.batch.count, 155)
        self.assertEqual(self.batch.size, 10326392)

    def tearDown(self):
        self.tmp = None
        self.batch = None


def random_leaves(file_size):
    index = 0
    for full in range(file_size // LEAF_SIZE):
        data = os.urandom(16) * (LEAF_SIZE // 16)
        yield Leaf(index, data)
        index += 1
    partial = file_size % LEAF_SIZE
    if partial:
        data = os.urandom(1) * partial
        yield Leaf(index, data)


def random_file(tmpdir, max_size=LEAF_SIZE*4):
    filename = path.join(tmpdir, random_id())
    file_size = random.randint(1, max_size)
    dst_fp = open(filename, 'wb')
    h = Hasher()
    for leaf in random_leaves(file_size):
        h.hash_leaf(leaf)
        dst_fp.write(leaf.data)
    dst_fp.close()
    st = os.stat(filename)
    file = File(filename, st.st_size, st.st_mtime)
    assert file.size == file_size
    return (file, h.content_hash())


def random_empty(tmpdir):
    filename = path.join(tmpdir, random_id())
    open(filename, 'wb').close()
    st = os.stat(filename)
    file = File(filename, st.st_size, st.st_mtime)
    assert file.size == 0
    return (file, None)


class TempDir(object):
    def __init__(self):
        self.dir = tempfile.mkdtemp(prefix='unittest.')

    def __del__(self):
        self.rmtree()

    def rmtree(self):
        if self.dir is not None:
            shutil.rmtree(self.dir)
            self.dir = None

    def join(self, *parts):
        return path.join(self.dir, *parts)

    def makedirs(self, *parts):
        d = self.join(*parts)
        if not path.exists(d):
            os.makedirs(d)
        assert path.isdir(d), d
        return d

    def touch(self, *parts):
        self.makedirs(*parts[:-1])
        f = self.join(*parts)
        open(f, 'wb').close()
        return f

    def write(self, data, *parts):
        self.makedirs(*parts[:-1])
        f = self.join(*parts)
        open(f, 'wb').write(data)
        return f

    def copy(self, src, *parts):
        self.makedirs(*parts[:-1])
        dst = self.join(*parts)
        shutil.copy(src, dst)
        return dst

    def random_batch(self, count, empties=0, max_size=LEAF_SIZE*4):
        result = list(self.random_file(max_size) for i in range(count))
        result.extend(self.random_empty() for i in range(empties))
        result.sort(key=lambda tup: tup[0].name)
        files = tuple(file for (file, ch) in result)
        batch = Batch(files, sum(f.size for f in files), len(files))
        return (batch, result)

    def random_file(self, max_size=LEAF_SIZE*4):
        return random_file(self.dir, max_size)

    def random_empty(self):
        return random_empty(self.dir)


class TempHome(TempDir):
    def __init__(self):
        super().__init__()
        self.orig = os.environ['HOME']
        os.environ['HOME'] = self.dir

    def __del__(self):
        os.environ['HOME'] = self.orig
        super().__del__()

