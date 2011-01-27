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
from unittest import TestCase
from .helpers import TempDir, TempHome, raises
from .helpers import sample_mov, sample_thm
from .helpers import mov_hash, mov_leaves, mov_qid
from dmedia.errors import AmbiguousPath, DuplicateFile
from dmedia.filestore import HashList
from dmedia import filestore, constants

TYPE_ERROR = '%s: need a %r; got a %r: %r'  # Standard TypeError message


class test_functions(TestCase):
    def test_safe_open(self):
        f = filestore.safe_open
        tmp = TempDir()
        filename = tmp.touch('example.mov')

        # Test that AmbiguousPath is raised:
        e = raises(AmbiguousPath, f, 'foo/bar', 'rb')
        self.assertEqual(e.filename, 'foo/bar')
        self.assertEqual(e.abspath, path.abspath('foo/bar'))

        e = raises(AmbiguousPath, f, '/foo/bar/../../root', 'rb')
        self.assertEqual(e.filename, '/foo/bar/../../root')
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
            'ext: can only contain ascii lowercase, digits; got %r' % bad
        )

        # Test with invalid charaters:
        bad = '$home'
        e = raises(ValueError, f, bad)
        self.assertEqual(
            str(e),
            'ext: can only contain ascii lowercase, digits; got %r' % bad
        )

        # Test with path traversal:
        bad = '/../../../.ssh/id_pub'
        e = raises(ValueError, f, bad)
        self.assertEqual(
            str(e),
            'ext: can only contain ascii lowercase, digits; got %r' % bad
        )

        # Test with a good ext:
        good = 'wav'
        assert f(good) is good
        good = 'cr2'
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
        tmp = TempDir()
        orig = os.getcwd()
        try:
            os.chdir(tmp.path)
            inst = self.klass('foo/bar')
            self.assertEqual(inst.base, tmp.join('foo/bar'))
        finally:
            os.chdir(orig)

    def test_relpath(self):
        inst = self.klass('/foo')

        self.assertEqual(
            inst.relpath('NWBNVXVK5DQGIOW7MYR4K3KA5K22W7NW'),
            ('NW', 'BNVXVK5DQGIOW7MYR4K3KA5K22W7NW')
        )
        self.assertEqual(
            inst.relpath('NWBNVXVK5DQGIOW7MYR4K3KA5K22W7NW', ext='ogv'),
            ('NW', 'BNVXVK5DQGIOW7MYR4K3KA5K22W7NW.ogv')
        )

        # Test to make sure hashes are getting checked with safe_b32():
        bad = 'NWBNVXVK5..GIOW7MYR4K3KA5K22W7NW'
        e = raises(ValueError, inst.relpath, bad)
        self.assertEqual(
            str(e),
            'b32: cannot b32decode %r: Non-base32 digit found' % bad
        )
        e = raises(ValueError, inst.relpath, bad, ext='ogv')
        self.assertEqual(
            str(e),
            'b32: cannot b32decode %r: Non-base32 digit found' % bad
        )

        # Test to make sure ext is getting checked with safe_ext():
        chash = 'NWBNVXVK5DQGIOW7MYR4K3KA5K22W7NW'
        bad = '/../../../.ssh/id_pub'
        e = raises(ValueError, inst.relpath, chash, bad)
        self.assertEqual(
            str(e),
            'ext: can only contain ascii lowercase, digits; got %r' % bad
        )

    def test_reltemp(self):
        inst = self.klass('/foo')

        self.assertEqual(
            inst.reltemp('NWBNVXVK5DQGIOW7MYR4K3KA5K22W7NW'),
            ('transfers', 'NWBNVXVK5DQGIOW7MYR4K3KA5K22W7NW')
        )
        self.assertEqual(
            inst.reltemp('NWBNVXVK5DQGIOW7MYR4K3KA5K22W7NW', ext='ogv'),
            ('transfers', 'NWBNVXVK5DQGIOW7MYR4K3KA5K22W7NW.ogv')
        )

        # Test to make sure hashes are getting checked with safe_b32():
        bad = 'NWBNVXVK5..GIOW7MYR4K3KA5K22W7NW'
        e = raises(ValueError, inst.reltemp, bad)
        self.assertEqual(
            str(e),
            'b32: cannot b32decode %r: Non-base32 digit found' % bad
        )
        e = raises(ValueError, inst.reltemp, bad, ext='ogv')
        self.assertEqual(
            str(e),
            'b32: cannot b32decode %r: Non-base32 digit found' % bad
        )

        # Test to make sure ext is getting checked with safe_ext():
        chash = 'NWBNVXVK5DQGIOW7MYR4K3KA5K22W7NW'
        bad = '/../../../.ssh/id_pub'
        e = raises(ValueError, inst.reltemp, chash, bad)
        self.assertEqual(
            str(e),
            'ext: can only contain ascii lowercase, digits; got %r' % bad
        )

    def test_reltmp(self):
        inst = self.klass('/foo')

        self.assertEqual(
            inst.reltmp(quickid='NWBNVXVK5DQGIOW7MYR4K3KA5K22W7NW'),
            ('imports', 'NWBNVXVK5DQGIOW7MYR4K3KA5K22W7NW')
        )
        self.assertEqual(
            inst.reltmp(quickid='NWBNVXVK5DQGIOW7MYR4K3KA5K22W7NW', ext='mov'),
            ('imports', 'NWBNVXVK5DQGIOW7MYR4K3KA5K22W7NW.mov')
        )
        self.assertEqual(
            inst.reltmp(chash='NWBNVXVK5DQGIOW7MYR4K3KA5K22W7NW'),
            ('downloads', 'NWBNVXVK5DQGIOW7MYR4K3KA5K22W7NW')
        )
        self.assertEqual(
            inst.reltmp(chash='NWBNVXVK5DQGIOW7MYR4K3KA5K22W7NW', ext='mov'),
            ('downloads', 'NWBNVXVK5DQGIOW7MYR4K3KA5K22W7NW.mov')
        )

        # Test to make sure hashes are getting checked with safe_b32():
        bad = 'NWBNVXVK5..GIOW7MYR4K3KA5K22W7NW'
        e = raises(ValueError, inst.reltmp, quickid=bad)
        self.assertEqual(
            str(e),
            'b32: cannot b32decode %r: Non-base32 digit found' % bad
        )
        e = raises(ValueError, inst.reltmp, chash=bad)
        self.assertEqual(
            str(e),
            'b32: cannot b32decode %r: Non-base32 digit found' % bad
        )

        # Test when neither quickid nor chash is provided:
        e = raises(TypeError, inst.reltmp)
        self.assertEqual(str(e), 'must provide either `chash` or `quickid`')

        # Test to make sure ext is getting checked with safe_ext():
        b32 = 'NWBNVXVK5DQGIOW7MYR4K3KA5K22W7NW'
        bad = '/../../../.ssh/id_pub'
        e = raises(ValueError, inst.reltmp, quickid=b32, ext=bad)
        self.assertEqual(
            str(e),
            'ext: can only contain ascii lowercase, digits; got %r' % bad
        )
        e = raises(ValueError, inst.reltmp, chash=b32, ext=bad)
        self.assertEqual(
            str(e),
            'ext: can only contain ascii lowercase, digits; got %r' % bad
        )

    def test_join(self):
        inst = self.klass('/foo/bar')
        TRAVERSAL = 'parts %r cause path traversal to %r'

        # Test with an absolute path in parts:
        e = raises(ValueError, inst.join, 'dmedia', '/root')
        self.assertEqual(
            str(e),
            TRAVERSAL % (('dmedia', '/root'), '/root')
        )

        # Test with some .. climbers:
        e = raises(ValueError, inst.join, 'NW/../../.ssh')
        self.assertEqual(
            str(e),
            TRAVERSAL % (('NW/../../.ssh',), '/foo/.ssh')
        )

        # Test with some correct parts:
        self.assertEqual(
            inst.join('NW', 'BNVXVK5DQGIOW7MYR4K3KA5K22W7NW'),
            '/foo/bar/NW/BNVXVK5DQGIOW7MYR4K3KA5K22W7NW'
        )
        self.assertEqual(
            inst.join('NW/BNVXVK5DQGIOW7MYR4K3KA5K22W7NW'),
            '/foo/bar/NW/BNVXVK5DQGIOW7MYR4K3KA5K22W7NW'
        )

    def test_create_parent(self):
        tmp = TempDir()
        tmp2 = TempDir()
        inst = self.klass(tmp.path)
        TRAVERSAL = 'Wont create %r outside of base %r for file %r'

        # Test with a normpath but outside of base:
        f = tmp2.join('foo', 'bar')
        d = tmp2.join('foo')
        self.assertFalse(path.exists(f))
        self.assertFalse(path.exists(d))
        e = raises(ValueError, inst.create_parent, f)
        self.assertEqual(
            str(e),
            TRAVERSAL % (d, inst.base, f)
        )
        self.assertFalse(path.exists(f))
        self.assertFalse(path.exists(d))

        # Test with some .. climbers:
        name = path.basename(tmp2.path)
        f = tmp.join('foo', '..', '..', name, 'baz', 'f')
        d = tmp2.join('baz')
        self.assertFalse(path.exists(f))
        self.assertFalse(path.exists(d))
        e = raises(ValueError, inst.create_parent, f)
        self.assertEqual(
            str(e),
            TRAVERSAL % (d, inst.base, f)
        )
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

    def test_path(self):
        inst = self.klass('/foo')

        self.assertEqual(
            inst.path('NWBNVXVK5DQGIOW7MYR4K3KA5K22W7NW'),
            '/foo/NW/BNVXVK5DQGIOW7MYR4K3KA5K22W7NW'
        )
        self.assertEqual(
            inst.path('NWBNVXVK5DQGIOW7MYR4K3KA5K22W7NW', ext='ogv'),
            '/foo/NW/BNVXVK5DQGIOW7MYR4K3KA5K22W7NW.ogv'
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
            'ext: can only contain ascii lowercase, digits; got %r' % bad
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

    def test_tmp(self):
        inst = self.klass('/foo')

        self.assertEqual(
            inst.tmp(quickid='NWBNVXVK5DQGIOW7MYR4K3KA5K22W7NW'),
            '/foo/imports/NWBNVXVK5DQGIOW7MYR4K3KA5K22W7NW'
        )
        self.assertEqual(
            inst.tmp(quickid='NWBNVXVK5DQGIOW7MYR4K3KA5K22W7NW', ext='mov'),
            '/foo/imports/NWBNVXVK5DQGIOW7MYR4K3KA5K22W7NW.mov'
        )
        self.assertEqual(
            inst.tmp(chash='NWBNVXVK5DQGIOW7MYR4K3KA5K22W7NW'),
            '/foo/downloads/NWBNVXVK5DQGIOW7MYR4K3KA5K22W7NW'
        )
        self.assertEqual(
            inst.tmp(chash='NWBNVXVK5DQGIOW7MYR4K3KA5K22W7NW', ext='mov'),
            '/foo/downloads/NWBNVXVK5DQGIOW7MYR4K3KA5K22W7NW.mov'
        )

        # Test to make sure hashes are getting checked with safe_b32():
        bad = 'NWBNVXVK5..GIOW7MYR4K3KA5K22W7NW'
        e = raises(ValueError, inst.tmp, quickid=bad)
        self.assertEqual(
            str(e),
            'b32: cannot b32decode %r: Non-base32 digit found' % bad
        )
        e = raises(ValueError, inst.tmp, chash=bad)
        self.assertEqual(
            str(e),
            'b32: cannot b32decode %r: Non-base32 digit found' % bad
        )

        # Test to make sure ext is getting checked with safe_ext():
        b32 = 'NWBNVXVK5DQGIOW7MYR4K3KA5K22W7NW'
        bad = '/../../../.ssh/id_pub'
        e = raises(ValueError, inst.tmp, quickid=b32, ext=bad)
        self.assertEqual(
            str(e),
            'ext: can only contain ascii lowercase, digits; got %r' % bad
        )
        e = raises(ValueError, inst.tmp, chash=b32, ext=bad)
        self.assertEqual(
            str(e),
            'ext: can only contain ascii lowercase, digits; got %r' % bad
        )

        # Test when neither quickid nor chash is provided:
        e = raises(TypeError, inst.tmp)
        self.assertEqual(str(e), 'must provide either `chash` or `quickid`')

        # Test with create=True
        tmp = TempDir()
        b32 = 'NWBNVXVK5DQGIOW7MYR4K3KA5K22W7NW'
        inst = self.klass(tmp.path)

        # With quickid
        f = tmp.join('imports', b32 + '.mov')
        d = tmp.join('imports')
        self.assertFalse(path.exists(f))
        self.assertFalse(path.exists(d))
        self.assertEqual(
            inst.tmp(quickid=b32, ext='mov'),
            f
        )
        self.assertFalse(path.exists(f))
        self.assertFalse(path.exists(d))
        self.assertEqual(
            inst.tmp(quickid=b32, ext='mov', create=True),
            f
        )
        self.assertFalse(path.exists(f))
        self.assertTrue(path.isdir(d))

        # With chash
        f = tmp.join('downloads', b32 + '.mov')
        d = tmp.join('downloads')
        self.assertFalse(path.exists(f))
        self.assertFalse(path.exists(d))
        self.assertEqual(
            inst.tmp(chash=b32, ext='mov'),
            f
        )
        self.assertFalse(path.exists(f))
        self.assertFalse(path.exists(d))
        self.assertEqual(
            inst.tmp(chash=b32, ext='mov', create=True),
            f
        )
        self.assertFalse(path.exists(f))
        self.assertTrue(path.isdir(d))

    def test_allocate_tmp(self):
        tmp = TempDir()
        inst = self.klass(tmp.path)

        # Test to make sure hashes are getting checked with safe_b32():
        bad = 'NWBNVXVK5..GIOW7MYR4K3KA5K22W7NW'
        e = raises(ValueError, inst.allocate_tmp, quickid=bad)
        self.assertEqual(
            str(e),
            'b32: cannot b32decode %r: Non-base32 digit found' % bad
        )
        e = raises(ValueError, inst.allocate_tmp, chash=bad)
        self.assertEqual(
            str(e),
            'b32: cannot b32decode %r: Non-base32 digit found' % bad
        )

        # Test when neither quickid nor chash is provided:
        e = raises(TypeError, inst.allocate_tmp)
        self.assertEqual(str(e), 'must provide either `chash` or `quickid`')

        # Test with good quickid
        f = tmp.join('imports', 'NWBNVXVK5DQGIOW7MYR4K3KA5K22W7NW.mov')
        d = tmp.join('imports')
        self.assertFalse(path.exists(f))
        self.assertFalse(path.exists(d))
        fp = inst.allocate_tmp(quickid='NWBNVXVK5DQGIOW7MYR4K3KA5K22W7NW', ext='mov')
        self.assertTrue(isinstance(fp, file))
        self.assertTrue(fp.mode in ['wb', 'r+b'])
        self.assertEqual(fp.name, f)
        self.assertTrue(path.isfile(f))
        self.assertTrue(path.isdir(d))

        # Test with good chash
        f = tmp.join('downloads', 'NWBNVXVK5DQGIOW7MYR4K3KA5K22W7NW.mov')
        d = tmp.join('downloads')
        self.assertFalse(path.exists(f))
        self.assertFalse(path.exists(d))
        fp = inst.allocate_tmp(chash='NWBNVXVK5DQGIOW7MYR4K3KA5K22W7NW', ext='mov')
        self.assertTrue(isinstance(fp, file))
        self.assertTrue(fp.mode in ['wb', 'r+b'])
        self.assertEqual(fp.name, f)
        self.assertTrue(path.isfile(f))
        self.assertTrue(path.isdir(d))

    def test_import_file(self):
        tmp = TempDir()
        src = tmp.join('movie.mov')
        base = tmp.join('.dmedia')
        dst = tmp.join('.dmedia', mov_hash[:2], mov_hash[2:] + '.mov')
        shutil.copy(sample_mov, src)

        inst = self.klass(base)
        self.assertTrue(path.isfile(src))
        self.assertFalse(path.exists(base))
        self.assertFalse(path.exists(dst))
        src_fp = open(src, 'rb')
        self.assertEqual(
            inst.import_file(src_fp, mov_qid, ext='mov'),
            (mov_hash, mov_leaves)
        )
        self.assertTrue(path.isfile(src))
        self.assertTrue(path.isdir(base))
        self.assertTrue(path.isfile(dst))

        self.assertEqual(
            filestore.HashList(open(dst, 'rb')).run(),
            mov_hash
        )

        e = raises(DuplicateFile, inst.import_file, src_fp, mov_qid, ext='mov')
        self.assertEqual(e.chash, mov_hash)
        self.assertEqual(e.src, src)
        self.assertEqual(e.dst, dst)
