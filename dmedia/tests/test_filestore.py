# Authors:
#   Jason Gerard DeRose <jderose@novacut.com>
#   Akshat Jain <ssj6akshat1234@gmail.com)
#
# dmedia: distributed media library
# Copyright (C) 2010 Jason Gerard DeRose <jderose@novacut.com>
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
Unit tests for `dmedia.filestore` module.
"""

import os
from os import path
from hashlib import sha1
from base64 import b32encode, b32decode
import shutil
import json
from unittest import TestCase
from .helpers import TempDir, TempHome, raises
from .helpers import sample_mov, sample_thm
from .helpers import mov_hash, mov_leaves, mov_qid
from .helpers import thm_hash, thm_leaves, thm_qid
from dmedia.errors import AmbiguousPath, FileStoreTraversal
from dmedia.errors import DuplicateFile, IntegrityError
from dmedia.filestore import HashList
from dmedia import filestore, constants, schema
from dmedia.constants import TYPE_ERROR, EXT_PAT, LEAF_SIZE


class test_functions(TestCase):
    def test_safe_path(self):
        f = filestore.safe_path

        # Test with relative path:
        e = raises(AmbiguousPath, f, 'foo/bar')
        self.assertEqual(e.pathname, 'foo/bar')
        self.assertEqual(e.abspath, path.abspath('foo/bar'))

        # Test with path traversal:
        e = raises(AmbiguousPath, f, '/foo/bar/../../root')
        self.assertEqual(e.pathname, '/foo/bar/../../root')
        self.assertEqual(e.abspath, '/root')

        # Test with normalized absolute path:
        self.assertEqual(f('/home/jderose/.dmedia'), '/home/jderose/.dmedia')


    def test_safe_open(self):
        f = filestore.safe_open
        tmp = TempDir()
        filename = tmp.touch('example.mov')

        # Test that AmbiguousPath is raised:
        e = raises(AmbiguousPath, f, 'foo/bar', 'rb')
        self.assertEqual(e.pathname, 'foo/bar')
        self.assertEqual(e.abspath, path.abspath('foo/bar'))

        e = raises(AmbiguousPath, f, '/foo/bar/../../root', 'rb')
        self.assertEqual(e.pathname, '/foo/bar/../../root')
        self.assertEqual(e.abspath, '/root')

        # Test with absolute normalized path:
        fp = f(filename, 'rb')
        self.assertTrue(isinstance(fp, file))
        self.assertEqual(fp.name, filename)
        self.assertEqual(fp.mode, 'rb')

    def test_safe_ext(self):
        f = filestore.safe_ext

        # Test with wrong type
        e = raises(TypeError, f, 42)
        self.assertEqual(
            str(e),
            TYPE_ERROR % ('ext', basestring, int, 42)
        )

        # Test with invalid case:
        bad = 'ogV'
        e = raises(ValueError, f, bad)
        self.assertEqual(
            str(e),
            'ext %r does not match pattern %r' % (bad, EXT_PAT)
        )

        # Test with invalid charaters:
        bad = '$home'
        e = raises(ValueError, f, bad)
        self.assertEqual(
            str(e),
            'ext %r does not match pattern %r' % (bad, EXT_PAT)
        )

        # Test with path traversal:
        bad = '/../../../.ssh/id_pub'
        e = raises(ValueError, f, bad)
        self.assertEqual(
            str(e),
            'ext %r does not match pattern %r' % (bad, EXT_PAT)
        )

        # Test with a good ext:
        good = 'wav'
        assert f(good) is good
        good = 'cr2'
        assert f(good) is good
        good = 'tar.gz'
        assert f(good) is good

    def test_safe_b32(self):
        f = filestore.safe_b32

        # Test with wrong type
        e = raises(TypeError, f, 42)
        self.assertEqual(
            str(e),
            TYPE_ERROR % ('b32', basestring, int, 42)
        )

        # Test with invalid base32 encoding:
        bad = 'NWBNVXVK5DQGIOW7MYR4K3KA5K22W7N'
        e = raises(ValueError, f, bad)
        self.assertEqual(
            str(e),
            'b32: cannot b32decode %r: Incorrect padding' % bad
        )

        # Test with wrong length:
        bad = 'NWBNVXVK5DQGIOW7MYR4K3KA'
        e = raises(ValueError, f, bad)
        self.assertEqual(
            str(e),
            'len(b32) must be 32; got 24: %r' % bad
        )

        # Test with a good chash:
        good = 'NWBNVXVK5DQGIOW7MYR4K3KA5K22W7NW'
        assert f(good) is good

    def test_pack_leaves(self):
        f = filestore.pack_leaves

        a = 'a' * 20
        b = 'b' * 20
        c = 'c' * 20
        d = 'd' * 20
        self.assertEqual(f([a, b, c]), a + b + c)
        self.assertEqual(f([a, b, c, d]), a + b + c + d)

        e = raises(ValueError, f, [a, b, c], digest_bytes=25)
        self.assertEqual(
            str(e),
            'digest_bytes=25, but len(leaves[0]) is 20'
        )
        e = raises(ValueError, f, [a, 'b' * 15, c])
        self.assertEqual(
            str(e),
            'digest_bytes=20, but len(leaves[1]) is 15'
        )

    def test_unpack_leaves(self):
        f = filestore.unpack_leaves

        a = 'a' * 20
        b = 'b' * 20
        c = 'c' * 20
        d = 'd' * 20
        data = a + b + c + d
        self.assertEqual(f(data), [a, b, c, d])

        a = 'a' * 32
        b = 'b' * 32
        c = 'c' * 32
        d = 'd' * 32
        e = 'e' * 32
        data = a + b + c + d + e
        self.assertEqual(f(data, digest_bytes=32), [a, b, c, d, e])

        e = raises(ValueError, f, 'a' * 201)
        self.assertEqual(
            str(e),
            'len(data)=201, not multiple of digest_bytes=20'
        )
        e = raises(ValueError, f, 'a' * 200, digest_bytes=16)
        self.assertEqual(
            str(e),
            'len(data)=200, not multiple of digest_bytes=16'
        )

    def test_quick_id(self):
        f = filestore.quick_id

        # Test with fp of wrong type
        e = raises(TypeError, f, 'hello')
        self.assertEqual(
            str(e),
            TYPE_ERROR % ('fp', file, str, 'hello')
        )

        # Test with fp opened in wrong mode
        fp = open(sample_mov, 'r')
        e = raises(ValueError, f, fp)
        self.assertEqual(
            str(e),
            "fp: must be opened in mode 'rb'; got 'r'"
        )

        # Test with some known files/values:
        fp = open(sample_mov, 'rb')
        self.assertEqual(f(fp), 'GJ4AQP3BK3DMTXYOLKDK6CW4QIJJGVMN')
        self.assertFalse(fp.closed)  # Should not close file

        fp = open(sample_thm, 'rb')
        self.assertEqual(f(fp), 'EYCDXXCNDB6OIIX5DN74J7KEXLNCQD5M')
        self.assertFalse(fp.closed)  # Should not close file

        # Make user seek(0) is being called:
        fp = open(sample_mov, 'rb')
        fp.seek(1024)
        self.assertEqual(f(fp), 'GJ4AQP3BK3DMTXYOLKDK6CW4QIJJGVMN')
        self.assertFalse(fp.closed)  # Should not close file

    def test_fallocate(self):
        f = filestore.fallocate
        tmp = TempDir()
        filename = tmp.join('example.mov')

        # Test when size is wrong type:
        e = raises(TypeError, f, '2311', filename)
        self.assertEqual(
            str(e),
            TYPE_ERROR % ('size', (int, long), str, '2311')
        )

        # Test when size <= 0
        e = raises(ValueError, f, 0, filename)
        self.assertEqual(str(e), 'size must be >0; got 0')
        e = raises(ValueError, f, -2311, filename)
        self.assertEqual(str(e), 'size must be >0; got -2311')

        # Test with relative path:
        e = raises(AmbiguousPath, f, 2311, 'foo/bar')
        self.assertEqual(e.pathname, 'foo/bar')
        self.assertEqual(e.abspath, path.abspath('foo/bar'))

        # Test with path traversal:
        e = raises(AmbiguousPath, f, 2311, '/foo/bar/../../root')
        self.assertEqual(e.pathname, '/foo/bar/../../root')
        self.assertEqual(e.abspath, '/root')

        # Test with correct args:
        self.assertFalse(path.exists(filename))
        ret = f(2311, filename)
        self.assertTrue(ret in [None, True, False])

        if ret is None:
            self.assertFalse(path.exists(filename))

        if ret is True:
            self.assertTrue(path.exists(filename))
            self.assertEqual(path.getsize(filename), 2311)

        if ret is False:
            self.assertTrue(path.exists(filename))
            self.assertEqual(path.getsize(filename), 0)


class test_HashList(TestCase):
    klass = filestore.HashList

    def test_init(self):
        tmp = TempDir()
        src_fp = open(sample_mov, 'rb')
        dst_fp = open(tmp.join('test.mov'), 'wb')

        # Test with src_fp of wrong type
        e = raises(TypeError, self.klass, 'hello', dst_fp)
        self.assertEqual(
            str(e),
            TYPE_ERROR % ('src_fp', file, str, 'hello')
        )

        # Test with src_fp opened in wrong mode
        e = raises(ValueError, self.klass, open(sample_mov, 'r'), dst_fp)
        self.assertEqual(
            str(e),
            "src_fp: mode must be 'rb'; got 'r'"
        )

        # Test with dst_fp of wrong type
        e = raises(TypeError, self.klass, src_fp, 17)
        self.assertEqual(
            str(e),
            TYPE_ERROR % ('dst_fp', file, int, 17)
        )

        # Test with dst_fp opened in wrong mode
        e = raises(ValueError, self.klass, src_fp,
            open(tmp.join('wrong.mov'), 'w')
        )
        self.assertEqual(
            str(e),
            "dst_fp: mode must be 'wb' or 'r+b'; got 'w'"
        )

        # Test with correct values
        inst = self.klass(src_fp)
        self.assertTrue(inst.src_fp is src_fp)
        self.assertEqual(inst.file_size, os.fstat(src_fp.fileno()).st_size)
        self.assertEqual(inst.leaves, [])
        self.assertTrue(inst.dst_fp is None)
        self.assertEqual(inst.leaf_size, constants.LEAF_SIZE)

        inst = self.klass(src_fp, dst_fp)
        self.assertTrue(inst.src_fp is src_fp)
        self.assertTrue(inst.dst_fp is dst_fp)
        self.assertEqual(inst.leaf_size, constants.LEAF_SIZE)

        inst = self.klass(src_fp, dst_fp, 2 * constants.LEAF_SIZE)
        self.assertTrue(inst.src_fp is src_fp)
        self.assertTrue(inst.dst_fp is dst_fp)
        self.assertEqual(inst.leaf_size, 2 * constants.LEAF_SIZE)

    def test_update(self):
        tmp = TempDir()

        class Example(self.klass):
            def __init__(self, dst_fp=None):
                self.dst_fp = dst_fp

        a = 'a' * (2 ** 20)  # 1 MiB of 'a'
        digest_a = sha1(a).digest()
        b = 'b' * (2 ** 20)  # 1 MiB of 'b'
        digest_b = sha1(b).digest()

        # Test without dst_fp
        inst = Example()
        inst.leaves = []
        inst.h = sha1()

        inst.update(a)
        self.assertEqual(
            inst.leaves,
            [digest_a]
        )
        self.assertEqual(
            inst.h.digest(),
            sha1(digest_a).digest()
        )
        inst.update(b)
        self.assertEqual(
            inst.leaves,
            [digest_a, digest_b]
        )
        self.assertEqual(
            inst.h.digest(),
            sha1(digest_a + digest_b).digest()
        )

        # Test with dst_fp:
        dst = tmp.join('out1')
        dst_fp = open(dst, 'wb')
        inst = Example(dst_fp)
        inst.leaves = []
        inst.h = sha1()

        inst.update(a)
        self.assertEqual(
            inst.leaves,
            [digest_a]
        )
        self.assertEqual(
            inst.h.digest(),
            sha1(digest_a).digest()
        )
        inst.update(b)
        self.assertEqual(
            inst.leaves,
            [digest_a, digest_b]
        )
        self.assertEqual(
            inst.h.digest(),
            sha1(digest_a + digest_b).digest()
        )
        dst_fp.close()
        self.assertEqual(
            open(dst, 'rb').read(),
            (a + b)
        )

    def test_run(self):
        tmp = TempDir()

        # Test when src_fp <= leaf_size:
        src_fp = open(sample_mov, 'rb')
        src_fp.read(1024)  # Make sure seek(0) is called
        dst_fp = open(tmp.join('dst1.mov'), 'wb')
        inst = self.klass(src_fp, dst_fp, 32 * 2**20)
        self.assertEqual(inst.run(), 'R3QI4WFID6VDVK2NBB6WXE5ALMNLZAHQ')
        self.assertFalse(src_fp.closed)  # Should not close file
        self.assertFalse(dst_fp.closed)  # Should not close file
        dst_fp.close()
        self.assertEqual(
            HashList(open(dst_fp.name, 'rb')).run(),
            mov_hash
        )
        self.assertEqual(
            inst.leaves,
            [b32decode('OMLUWEIPEUNRGYMKAEHG3AEZPVZ5TUQE')]
        )

        # Test when src_fp > leaf_size:
        src_fp = open(sample_mov, 'rb')
        src_fp.read(1024)  # Make sure seek(0) is called
        dst_fp = open(tmp.join('dst2.mov'), 'wb')
        inst = self.klass(src_fp, dst_fp, 16 * 2**20)
        self.assertEqual(inst.run(), 'B4IBNJ674EPXZZKNJYXFBDQQTFXIBSSC')
        self.assertFalse(src_fp.closed)  # Should not close file
        self.assertFalse(dst_fp.closed)  # Should not close file
        dst_fp.close()
        self.assertEqual(
            HashList(open(dst_fp.name, 'rb')).run(),
            mov_hash
        )
        self.assertEqual(
            inst.leaves,
            [
                b32decode('7IYAMI5IEHVDWDPWCVPRUMJJNI4TZE75'),
                b32decode('FHF7KDMAGNYOVNYSYT6ZYWQLUOCTUADI'),
            ]
        )


class test_FileStore(TestCase):
    klass = filestore.FileStore

    def test_init(self):
        # Test with relative path:
        e = raises(AmbiguousPath, self.klass, 'foo/bar')
        self.assertEqual(e.pathname, 'foo/bar')
        self.assertEqual(e.abspath, path.abspath('foo/bar'))

        # Test with path traversal:
        e = raises(AmbiguousPath, self.klass, '/foo/bar/../../root')
        self.assertEqual(e.pathname, '/foo/bar/../../root')
        self.assertEqual(e.abspath, '/root')

        # Test when base is a file
        tmp = TempDir()
        base = tmp.touch('.dmedia')
        e = raises(ValueError, self.klass, base)
        self.assertEqual(
            str(e),
            'FileStore.base not a directory: %r' % base
        )

        # Test when base does not exist
        tmp = TempDir()
        base = tmp.join('.dmedia')
        record = tmp.join('.dmedia', 'store.json')
        inst = self.klass(base)
        self.assertEqual(inst.base, base)
        self.assertTrue(path.isdir(inst.base))
        self.assertEqual(inst.record, record)
        self.assertTrue(path.isfile(record))
        store_s = open(record, 'rb').read()
        doc = json.loads(store_s)
        self.assertEqual(schema.check_dmedia_store(doc), None)
        self.assertEqual(inst._doc, doc)
        self.assertEqual(inst._id, doc['_id'])

        # Test when base exists and is a directory
        inst = self.klass(base)
        self.assertEqual(inst.base, base)
        self.assertTrue(path.isdir(inst.base))
        self.assertEqual(inst.record, record)
        self.assertTrue(path.isfile(record))
        self.assertEqual(open(record, 'rb').read(), store_s)

        # Test when base=None
        inst = self.klass()
        self.assertTrue(path.isdir(inst.base))
        self.assertTrue(inst.base.startswith('/tmp/store.'))
        self.assertEqual(inst.record, path.join(inst.base, 'store.json'))

    def test_relpath(self):
        self.assertEqual(
            self.klass.relpath('NWBNVXVK5DQGIOW7MYR4K3KA5K22W7NW'),
            ('NW', 'BNVXVK5DQGIOW7MYR4K3KA5K22W7NW')
        )
        self.assertEqual(
            self.klass.relpath('NWBNVXVK5DQGIOW7MYR4K3KA5K22W7NW', ext='ogv'),
            ('NW', 'BNVXVK5DQGIOW7MYR4K3KA5K22W7NW.ogv')
        )

        # Test to make sure hashes are getting checked with safe_b32():
        bad = 'NWBNVXVK5..GIOW7MYR4K3KA5K22W7NW'
        e = raises(ValueError, self.klass.relpath, bad)
        self.assertEqual(
            str(e),
            'b32: cannot b32decode %r: Non-base32 digit found' % bad
        )
        e = raises(ValueError, self.klass.relpath, bad, ext='ogv')
        self.assertEqual(
            str(e),
            'b32: cannot b32decode %r: Non-base32 digit found' % bad
        )

        # Test to make sure ext is getting checked with safe_ext():
        chash = 'NWBNVXVK5DQGIOW7MYR4K3KA5K22W7NW'
        bad = '/../../../.ssh/id_pub'
        e = raises(ValueError, self.klass.relpath, chash, bad)
        self.assertEqual(
            str(e),
            'ext %r does not match pattern %r' % (bad, EXT_PAT)
        )

    def test_reltemp(self):
        self.assertEqual(
            self.klass.reltemp('NWBNVXVK5DQGIOW7MYR4K3KA5K22W7NW'),
            ('transfers', 'NWBNVXVK5DQGIOW7MYR4K3KA5K22W7NW')
        )
        self.assertEqual(
            self.klass.reltemp('NWBNVXVK5DQGIOW7MYR4K3KA5K22W7NW', ext='ogv'),
            ('transfers', 'NWBNVXVK5DQGIOW7MYR4K3KA5K22W7NW.ogv')
        )

        # Test to make sure hashes are getting checked with safe_b32():
        bad = 'NWBNVXVK5..GIOW7MYR4K3KA5K22W7NW'
        e = raises(ValueError, self.klass.reltemp, bad)
        self.assertEqual(
            str(e),
            'b32: cannot b32decode %r: Non-base32 digit found' % bad
        )
        e = raises(ValueError, self.klass.reltemp, bad, ext='ogv')
        self.assertEqual(
            str(e),
            'b32: cannot b32decode %r: Non-base32 digit found' % bad
        )

        # Test to make sure ext is getting checked with safe_ext():
        chash = 'NWBNVXVK5DQGIOW7MYR4K3KA5K22W7NW'
        bad = '/../../../.ssh/id_pub'
        e = raises(ValueError, self.klass.reltemp, chash, bad)
        self.assertEqual(
            str(e),
            'ext %r does not match pattern %r' % (bad, EXT_PAT)
        )

    def test_check_path(self):
        tmp = TempDir()
        base = tmp.join('foo', 'bar')
        inst = self.klass(base)

        bad = tmp.join('foo', 'barNone', 'stuff')
        e = raises(FileStoreTraversal, inst.check_path, bad)
        self.assertEqual(e.pathname, bad)
        self.assertEqual(e.abspath, bad)
        self.assertEqual(e.base, base)

        bad = tmp.join('foo', 'bar', '..', 'barNone')
        assert '..' in bad
        e = raises(FileStoreTraversal, inst.check_path, bad)
        self.assertEqual(e.pathname, bad)
        self.assertEqual(e.abspath, tmp.join('foo', 'barNone'))
        self.assertEqual(e.base, base)

        good = tmp.join('foo', 'bar', 'stuff')
        self.assertEqual(inst.check_path(good), good)

    def test_join(self):
        tmp = TempDir()
        base = tmp.join('foo', 'bar')
        inst = self.klass(base)

        # Test with an absolute path in parts:
        e = raises(FileStoreTraversal, inst.join, 'dmedia', '/root')
        self.assertEqual(e.pathname, '/root')
        self.assertEqual(e.abspath, '/root')
        self.assertEqual(e.base, base)

        # Test with some .. climbers:
        e = raises(FileStoreTraversal, inst.join, 'NW', '..', '..', '.ssh')
        self.assertEqual(
            e.pathname,
            tmp.join('foo', 'bar', 'NW', '..', '..', '.ssh')
        )
        self.assertEqual(e.abspath, tmp.join('foo', '.ssh'))
        self.assertEqual(e.base, base)

        # Test for former security issue!  See:
        # https://bugs.launchpad.net/dmedia/+bug/708663
        e = raises(FileStoreTraversal, inst.join, '..', 'barNone', 'stuff')
        self.assertEqual(
            e.pathname,
            tmp.join('foo', 'bar', '..', 'barNone', 'stuff')
        )
        self.assertEqual(e.abspath, tmp.join('foo', 'barNone', 'stuff'))
        self.assertEqual(e.base, base)

        # Test with some correct parts:
        self.assertEqual(
            inst.join('NW', 'BNVXVK5DQGIOW7MYR4K3KA5K22W7NW'),
            tmp.join('foo', 'bar', 'NW', 'BNVXVK5DQGIOW7MYR4K3KA5K22W7NW')
        )

    def test_create_parent(self):
        tmp = TempDir()
        tmp2 = TempDir()
        inst = self.klass(tmp.path)

        # Test with a normpath but outside of base:
        f = tmp2.join('foo', 'bar')
        d = tmp2.join('foo')
        self.assertFalse(path.exists(f))
        self.assertFalse(path.exists(d))
        e = raises(FileStoreTraversal, inst.create_parent, f)
        self.assertEqual(e.pathname, f)
        self.assertEqual(e.abspath, f)
        self.assertEqual(e.base, tmp.path)
        self.assertFalse(path.exists(f))
        self.assertFalse(path.exists(d))

        # Test with some .. climbers:
        name = path.basename(tmp2.path)
        f = tmp.join('foo', '..', '..', name, 'baz', 'f')
        d = tmp2.join('baz')
        self.assertFalse(path.exists(f))
        self.assertFalse(path.exists(d))
        e = raises(FileStoreTraversal, inst.create_parent, f)
        self.assertEqual(e.pathname, f)
        self.assertEqual(e.abspath, tmp2.join('baz', 'f'))
        self.assertEqual(e.base, tmp.path)
        self.assertFalse(path.exists(f))
        self.assertFalse(path.exists(d))

        # Test with some correct parts:
        f = tmp.join('NW', 'BNVXVK5DQGIOW7MYR4K3KA5K22W7NW')
        d = tmp.join('NW')
        self.assertFalse(path.exists(f))
        self.assertFalse(path.exists(d))
        self.assertEqual(inst.create_parent(f), d)
        self.assertFalse(path.exists(f))
        self.assertTrue(path.isdir(d))
        self.assertEqual(inst.create_parent(f), d)  # When d already exists
        self.assertFalse(path.exists(f))
        self.assertTrue(path.isdir(d))

        # Confirm that it's using os.makedirs(), not os.mkdir()
        f = tmp.join('OM', 'LU', 'WE', 'IP')
        d = tmp.join('OM', 'LU', 'WE')
        self.assertFalse(path.exists(f))
        self.assertFalse(path.exists(d))
        self.assertEqual(inst.create_parent(f), d)
        self.assertFalse(path.exists(f))
        self.assertTrue(path.isdir(d))
        self.assertEqual(inst.create_parent(f), d)  # When d already exists
        self.assertFalse(path.exists(f))
        self.assertTrue(path.isdir(d))

        # Test with 1-deep:
        f = tmp.join('woot')
        self.assertFalse(path.exists(f))
        self.assertEqual(inst.create_parent(f), tmp.path)
        self.assertFalse(path.exists(f))

        # Test for former security issue!  See:
        # https://bugs.launchpad.net/dmedia/+bug/708663
        tmp = TempDir()
        base = tmp.join('foo', 'bar')
        bad = tmp.join('foo', 'barNone', 'stuff')
        baddir = tmp.join('foo', 'barNone')
        inst = self.klass(base)
        e = raises(FileStoreTraversal, inst.create_parent, bad)
        self.assertEqual(e.pathname, bad)
        self.assertEqual(e.abspath, bad)
        self.assertEqual(e.base, base)
        self.assertFalse(path.exists(bad))
        self.assertFalse(path.exists(baddir))

    def test_path(self):
        tmp = TempDir()
        base = tmp.join('foo', 'bar')
        inst = self.klass(base)

        self.assertEqual(
            inst.path('NWBNVXVK5DQGIOW7MYR4K3KA5K22W7NW'),
            tmp.join('foo', 'bar', 'NW', 'BNVXVK5DQGIOW7MYR4K3KA5K22W7NW')
        )
        self.assertEqual(
            inst.path('NWBNVXVK5DQGIOW7MYR4K3KA5K22W7NW', ext='ogv'),
            tmp.join('foo', 'bar', 'NW', 'BNVXVK5DQGIOW7MYR4K3KA5K22W7NW.ogv')
        )

        # Test to make sure hashes are getting checked with safe_b32():
        bad = 'NWBNVXVK5..GIOW7MYR4K3KA5K22W7NW'
        e = raises(ValueError, inst.path, bad)
        self.assertEqual(
            str(e),
            'b32: cannot b32decode %r: Non-base32 digit found' % bad
        )
        e = raises(ValueError, inst.path, bad, ext='ogv')
        self.assertEqual(
            str(e),
            'b32: cannot b32decode %r: Non-base32 digit found' % bad
        )

        # Test to make sure ext is getting checked with safe_ext():
        chash = 'NWBNVXVK5DQGIOW7MYR4K3KA5K22W7NW'
        bad = '/../../../.ssh/id_pub'
        e = raises(ValueError, inst.path, chash, bad)
        self.assertEqual(
            str(e),
            'ext %r does not match pattern %r' % (bad, EXT_PAT)
        )

        # Test with create=True
        tmp = TempDir()
        inst = self.klass(tmp.path)

        f = tmp.join('NW', 'BNVXVK5DQGIOW7MYR4K3KA5K22W7NW')
        d = tmp.join('NW')
        self.assertFalse(path.exists(f))
        self.assertFalse(path.exists(d))
        self.assertEqual(
            inst.path('NWBNVXVK5DQGIOW7MYR4K3KA5K22W7NW'),
            f
        )
        self.assertFalse(path.exists(f))
        self.assertFalse(path.exists(d))
        self.assertEqual(
            inst.path('NWBNVXVK5DQGIOW7MYR4K3KA5K22W7NW', create=True),
            f
        )
        self.assertFalse(path.exists(f))
        self.assertTrue(path.isdir(d))

    def test_temp(self):
        tmp = TempDir()
        inst = self.klass(tmp.path)

        self.assertEqual(
            inst.temp('NWBNVXVK5DQGIOW7MYR4K3KA5K22W7NW'),
            tmp.join('transfers', 'NWBNVXVK5DQGIOW7MYR4K3KA5K22W7NW')
        )
        self.assertEqual(
            inst.temp('NWBNVXVK5DQGIOW7MYR4K3KA5K22W7NW', ext='ogv'),
            tmp.join('transfers', 'NWBNVXVK5DQGIOW7MYR4K3KA5K22W7NW.ogv')
        )

        # Test to make sure hashes are getting checked with safe_b32():
        bad = 'NWBNVXVK5..GIOW7MYR4K3KA5K22W7NW'
        e = raises(ValueError, inst.temp, bad)
        self.assertEqual(
            str(e),
            'b32: cannot b32decode %r: Non-base32 digit found' % bad
        )
        e = raises(ValueError, inst.temp, bad, ext='ogv')
        self.assertEqual(
            str(e),
            'b32: cannot b32decode %r: Non-base32 digit found' % bad
        )

        # Test to make sure ext is getting checked with safe_ext():
        chash = 'NWBNVXVK5DQGIOW7MYR4K3KA5K22W7NW'
        bad = '/../../../.ssh/id_pub'
        e = raises(ValueError, inst.temp, chash, bad)
        self.assertEqual(
            str(e),
            'ext %r does not match pattern %r' % (bad, EXT_PAT)
        )

        # Test with create=True
        tmp = TempDir()
        inst = self.klass(tmp.path)

        f = tmp.join('transfers', 'NWBNVXVK5DQGIOW7MYR4K3KA5K22W7NW')
        d = tmp.join('transfers')
        self.assertFalse(path.exists(f))
        self.assertFalse(path.exists(d))
        self.assertEqual(
            inst.temp('NWBNVXVK5DQGIOW7MYR4K3KA5K22W7NW'),
            f
        )
        self.assertFalse(path.exists(f))
        self.assertFalse(path.exists(d))
        self.assertEqual(
            inst.temp('NWBNVXVK5DQGIOW7MYR4K3KA5K22W7NW', create=True),
            f
        )
        self.assertFalse(path.exists(f))
        self.assertTrue(path.isdir(d))

    def test_allocate_for_transfer(self):
        chash = 'NWBNVXVK5DQGIOW7MYR4K3KA5K22W7NW'

        tmp = TempDir()
        inst = self.klass(tmp.path)
        filename = tmp.join('transfers', chash)

        # Test when file dosen't yet exist
        fp = inst.allocate_for_transfer(2311, chash)
        self.assertTrue(isinstance(fp, file))
        self.assertEqual(fp.name, filename)
        stat = os.fstat(fp.fileno())

        self.assertTrue(fp.mode in ['r+b', 'wb'])
        if fp.mode == 'r+b':
            self.assertTrue(stat.st_size in (0, 2311))
        if fp.mode == 'wb':
            self.assertEqual(stat.st_size, 0)
        fp.write('a' * 3141)  # Write something to file
        fp.close()

        # Test with pre-existing file longer than size:
        fp = inst.allocate_for_transfer(2311, chash)
        self.assertTrue(isinstance(fp, file))
        self.assertEqual(fp.name, filename)
        stat = os.fstat(fp.fileno())

        self.assertEqual(fp.mode, 'r+b')
        self.assertEqual(stat.st_size, 2311)  # fp.trucate() was called

        #####################################
        # Again, but this time with ext='mov'

        tmp = TempDir()
        inst = self.klass(tmp.path)
        filename = tmp.join('transfers', chash + '.mov')

        # Test when file dosen't yet exist
        fp = inst.allocate_for_transfer(2311, chash, ext='mov')
        self.assertTrue(isinstance(fp, file))
        self.assertEqual(fp.name, filename)
        stat = os.fstat(fp.fileno())

        self.assertTrue(fp.mode in ['r+b', 'wb'])
        if fp.mode == 'r+b':
            self.assertTrue(stat.st_size in (0, 2311))
        if fp.mode == 'wb':
            self.assertEqual(stat.st_size, 0)
        fp.write('a' * 3141)  # write something to file > size
        fp.close()

        # Test with pre-existing file longer than size:
        fp = inst.allocate_for_transfer(2311, chash, ext='mov')
        self.assertTrue(isinstance(fp, file))
        self.assertEqual(fp.name, filename)
        stat = os.fstat(fp.fileno())

        self.assertEqual(fp.mode, 'r+b')
        self.assertEqual(stat.st_size, 2311)  # fp.trucate() was called

    def test_allocate_for_import(self):
        tmp = TempDir()
        imports = tmp.join('imports')

        inst = self.klass(tmp.path)
        self.assertFalse(path.isdir(imports))

        # Test with ext=None:
        fp = inst.allocate_for_import(2311)
        self.assertTrue(path.isdir(imports))
        self.assertTrue(isinstance(fp, file))
        self.assertEqual(fp.mode, 'r+b')
        stat = os.fstat(fp.fileno())
        self.assertTrue(stat.st_size in [0, 2311])
        self.assertEqual(path.dirname(fp.name), imports)
        self.assertTrue(
            '.' not in path.basename(fp.name)
        )

        # Test with ext='mov':
        fp = inst.allocate_for_import(3141, ext='mov')
        self.assertTrue(isinstance(fp, file))
        self.assertEqual(fp.mode, 'r+b')
        stat = os.fstat(fp.fileno())
        self.assertTrue(stat.st_size in [0, 3141])
        self.assertEqual(path.dirname(fp.name), imports)
        self.assertTrue(fp.name.endswith('.mov'))

    def test_finalize_transfer(self):
        tmp = TempDir()
        inst = self.klass(tmp.path)

        src_d = tmp.join('transfers')
        src = tmp.join('transfers', mov_hash + '.mov')
        dst_d = tmp.join(mov_hash[:2])
        dst = tmp.join(mov_hash[:2], mov_hash[2:] + '.mov')

        # Test when transfers/ dir doesn't exist:
        e = raises(IOError, inst.finalize_transfer, mov_hash, 'mov')
        self.assertFalse(path.exists(src_d))
        self.assertFalse(path.exists(dst_d))
        self.assertFalse(path.exists(dst))

        # Test when transfers/ exists but file does not:
        self.assertEqual(inst.temp(mov_hash, 'mov', create=True), src)
        self.assertTrue(path.isdir(src_d))
        e = raises(IOError, inst.finalize_transfer, mov_hash, 'mov')
        self.assertFalse(path.exists(src))
        self.assertFalse(path.exists(dst_d))
        self.assertFalse(path.exists(dst))

        # Test when file has wrong content hash and wrong size:
        open(src, 'wb').write(open(sample_thm, 'rb').read())
        e = raises(IntegrityError, inst.finalize_transfer, mov_hash, 'mov')
        self.assertEqual(e.got, thm_hash)
        self.assertEqual(e.expected, mov_hash)
        self.assertEqual(e.filename, src)
        self.assertFalse(path.exists(dst_d))
        self.assertFalse(path.exists(dst))

        # Test when file has wrong content hash and *correct* size:
        fp1 = open(sample_mov, 'rb')
        fp2 = open(src, 'wb')
        while True:
            chunk = fp1.read(LEAF_SIZE)
            if not chunk:
                break
            fp2.write(chunk)
        fp1.close()

        # Now change final byte at end file:
        fp2.seek(-1, os.SEEK_END)
        fp2.write('A')
        fp2.close()
        self.assertEqual(path.getsize(sample_mov), path.getsize(src))

        e = raises(IntegrityError, inst.finalize_transfer, mov_hash, 'mov')
        self.assertEqual(e.got, 'UECTT7A7EIHZ2SGGBMMO5WTTSVU4SUWM')
        self.assertEqual(e.expected, mov_hash)
        self.assertEqual(e.filename, src)
        self.assertFalse(path.exists(dst_d))
        self.assertFalse(path.exists(dst))

        # Test with correct content hash:
        fp1 = open(sample_mov, 'rb')
        fp2 = open(src, 'wb')
        while True:
            chunk = fp1.read(LEAF_SIZE)
            if not chunk:
                break
            fp2.write(chunk)
        fp1.close()
        fp2.close()
        self.assertEqual(inst.finalize_transfer(mov_hash, 'mov'), dst)
        self.assertTrue(path.isdir(src_d))
        self.assertFalse(path.exists(src))
        self.assertTrue(path.isdir(dst_d))
        self.assertTrue(path.isfile(dst))

        # Check content hash of file in canonical location
        fp = open(dst, 'rb')
        self.assertEqual(HashList(fp).run(), mov_hash)

    def test_import_file(self):
        tmp = TempDir()
        src = tmp.join('movie.mov')
        base = tmp.join('.dmedia')
        dst = tmp.join('.dmedia', mov_hash[:2], mov_hash[2:] + '.mov')
        shutil.copy(sample_mov, src)

        inst = self.klass(base)
        self.assertTrue(path.isfile(src))
        self.assertTrue(path.isdir(base))
        self.assertFalse(path.exists(dst))
        src_fp = open(src, 'rb')
        self.assertEqual(
            inst.import_file(src_fp, ext='mov'),
            (mov_hash, mov_leaves)
        )
        self.assertTrue(path.isfile(src))
        self.assertTrue(path.isdir(base))
        self.assertTrue(path.isfile(dst))

        self.assertEqual(
            filestore.HashList(open(dst, 'rb')).run(),
            mov_hash
        )

        e = raises(DuplicateFile, inst.import_file, src_fp, ext='mov')
        self.assertEqual(e.chash, mov_hash)
        self.assertEqual(e.src, src)
        self.assertEqual(e.dst, dst)
