# Authors:
#   Jason Gerard DeRose <jderose@jasonderose.org>
#   Akshat Jain <ssj6akshat1234@gmail.com>
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
from unittest import TestCase
from .helpers import TempDir, TempHome, raises, sample_mov, sample_thm
from dmedialib import filestore2


TYPE_ERROR = '%s: need a %r; got a %r: %r'  # Standard TypeError message


class test_functions(TestCase):
    def test_issafe(self):
        f = filestore2.issafe

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

    def test_hash_file(self):
        f = filestore2.hash_file
        self.assertEqual(f(sample_mov), 'OMLUWEIPEUNRGYMKAEHG3AEZPVZ5TUQE')
        self.assertEqual(f(sample_thm), 'F6ATTKI6YVWVRBQQESAZ4DSUXQ4G457A')

    def test_hash_and_copy(self):
        f = filestore2.hash_and_copy
        hash_file = filestore2.hash_file
        tmp = TempDir()
        dst1 = tmp.join('sample.mov')
        dst2 = tmp.join('sample.thm')
        self.assertEqual(f(sample_mov, dst1), 'OMLUWEIPEUNRGYMKAEHG3AEZPVZ5TUQE')
        self.assertEqual(hash_file(dst1), 'OMLUWEIPEUNRGYMKAEHG3AEZPVZ5TUQE')
        self.assertEqual(f(sample_thm, dst2), 'F6ATTKI6YVWVRBQQESAZ4DSUXQ4G457A')
        self.assertEqual(hash_file(dst2), 'F6ATTKI6YVWVRBQQESAZ4DSUXQ4G457A')

    def test_quick_id(self):
        f = filestore2.quick_id
        self.assertEqual(f(sample_mov), 'GJ4AQP3BK3DMTXYOLKDK6CW4QIJJGVMN')
        self.assertEqual(f(sample_thm), 'EYCDXXCNDB6OIIX5DN74J7KEXLNCQD5M')


class test_FileStore(TestCase):
    klass = filestore2.FileStore

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

        # Test to make sure hashes are getting checked with issafe():
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

        # Test to make sure hashes are getting checked with issafe():
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
        inst = self.klass(tmp.path)
        TRAVERSAL = 'parts %r cause path traversal to %r'

        # Test with an absolute path in parts:
        e = raises(ValueError, inst.create_parent, 'dmedia', '/root')
        self.assertEqual(
            str(e),
            TRAVERSAL % (('dmedia', '/root'), '/root')
        )

        # Test with some .. climbers:
        e = raises(ValueError, inst.create_parent, 'NW/../../.ssh')
        self.assertEqual(
            str(e),
            TRAVERSAL % (('NW/../../.ssh',), '/tmp/.ssh')
        )

        # Test with some correct parts:
        f = tmp.join('NW', 'BNVXVK5DQGIOW7MYR4K3KA5K22W7NW')
        d = tmp.join('NW')
        self.assertFalse(path.exists(d))
        self.assertFalse(path.exists(f))
        self.assertEqual(inst.create_parent('NW', 'BNVXVK5DQGIOW7MYR4K3KA5K22W7NW'), f)
        self.assertTrue(path.isdir(d))
        self.assertFalse(path.exists(f))
        return
        self.assertEqual(
            inst.join('NW/BNVXVK5DQGIOW7MYR4K3KA5K22W7NW'),
            '/foo/bar/NW/BNVXVK5DQGIOW7MYR4K3KA5K22W7NW'
        )
