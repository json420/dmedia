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
from .helpers import TempDir, TempHome, raises, sample_mov
from dmedialib import filestore


letters = 'gihdwaqoebxtcklrnsmjufyvpz'
extensions = ('png', 'jpg', 'mov')
key = '4e3a57109f226b07fe00e0abac88544b2e8331d0ec47ee00340138dd.iso'
dname = '4e'
fname = '3a57109f226b07fe00e0abac88544b2e8331d0ec47ee00340138dd.iso'


def user_dir():
    return path.join(os.environ['HOME'], '.dmedia')



class test_functions(TestCase):
    def test_quick_id(self):
        f = filestore.quick_id
        self.assertEqual(f(sample_mov), 'GJ4AQP3BK3DMTXYOLKDK6CW4QIJJGVMN')
        
    def test_scanfiles(self):
        f = filestore.scanfiles
        tmp = TempDir()
        self.assertEqual(list(f(tmp.path)), [])
        somefile = tmp.touch('somefile.txt')
        self.assertEqual(list(f(somefile)), [])

        # Create files in a non-alphabetic order:
        names = []
        for (i, l) in enumerate(letters):
            ext = extensions[i % len(extensions)]
            name = '.'.join([l, ext.upper()])
            names.append(name)
            tmp.touch('subdir', name)

        got = list(f(tmp.path, extensions))
        expected = list(
            {
                'src': tmp.join('subdir', name),
                'base': tmp.join('subdir'),
                'root': name.split('.')[0],
                'meta': {
                    'name': name,
                    'ext': name.split('.')[1].lower(),
                },
            }
            for name in sorted(names)
        )
        self.assertEqual(got, expected)



class test_FileStore(TestCase):
    klass = filestore.FileStore

    def test_init(self):
        home = TempHome()
        inst = self.klass()
        self.assertEqual(inst.home, home.path)
        self.assertEqual(inst.user_dir, user_dir())
        self.assertEqual(inst.shared_dir, '/home/.dmedia')
        inst = self.klass(user_dir='/foo', shared_dir='/bar')
        self.assertEqual(inst.user_dir, '/foo')
        self.assertEqual(inst.shared_dir, '/bar')
        inst = self.klass('/foo', '/bar')
        self.assertEqual(inst.user_dir, '/foo')
        self.assertEqual(inst.shared_dir, '/bar')

    def test_chash(self):
        inst = self.klass()
        tmp = TempDir()
        src = tmp.write('Novacut', 'msg.txt')
        self.assertEqual(inst.chash(src), 'NWBNVXVK5DQGIOW7MYR4K3KA5K22W7NW')
        fp = open(src, 'rb')
        self.assertEqual(inst.chash(fp=fp), 'NWBNVXVK5DQGIOW7MYR4K3KA5K22W7NW')

    def test_relname(self):
        inst = self.klass()
        self.assertEqual(
            inst.relname('NWBNVXVK5DQGIOW7MYR4K3KA5K22W7NW'),
            ('NW', 'BNVXVK5DQGIOW7MYR4K3KA5K22W7NW')
        )
        self.assertEqual(
            inst.relname('NWBNVXVK5DQGIOW7MYR4K3KA5K22W7NW', None),
            ('NW', 'BNVXVK5DQGIOW7MYR4K3KA5K22W7NW')
        )
        self.assertEqual(
            inst.relname('NWBNVXVK5DQGIOW7MYR4K3KA5K22W7NW', ''),
            ('NW', 'BNVXVK5DQGIOW7MYR4K3KA5K22W7NW')
        )
        self.assertEqual(
            inst.relname('NWBNVXVK5DQGIOW7MYR4K3KA5K22W7NW', 'ogv'),
            ('NW', 'BNVXVK5DQGIOW7MYR4K3KA5K22W7NW.ogv')
        )
        self.assertEqual(
            inst.relname('6d82dadeaae8e0643adf6623c56d40eab5ab7db6'),
            ('6d', '82dadeaae8e0643adf6623c56d40eab5ab7db6')
        )
        self.assertEqual(
            inst.relname('6d82dadeaae8e0643adf6623c56d40eab5ab7db6', None),
            ('6d', '82dadeaae8e0643adf6623c56d40eab5ab7db6')
        )
        self.assertEqual(
            inst.relname('6d82dadeaae8e0643adf6623c56d40eab5ab7db6', ''),
            ('6d', '82dadeaae8e0643adf6623c56d40eab5ab7db6')
        )
        self.assertEqual(
            inst.relname('6d82dadeaae8e0643adf6623c56d40eab5ab7db6', 'mov'),
            ('6d', '82dadeaae8e0643adf6623c56d40eab5ab7db6.mov')
        )

    def test_mediadir(self):
        inst = self.klass('/foo', '/bar')
        self.assertEqual(inst.mediadir(), '/foo')
        self.assertEqual(inst.mediadir(False), '/foo')
        self.assertEqual(inst.mediadir(True), '/bar')
        self.assertEqual(inst.mediadir(shared=False), '/foo')
        self.assertEqual(inst.mediadir(shared=True), '/bar')

    def test_fullname(self):
        chash = 'NWBNVXVK5DQGIOW7MYR4K3KA5K22W7NW'
        inst = self.klass()
        self.assertEqual(
            inst.fullname(chash),
            path.join(user_dir(), 'NW', 'BNVXVK5DQGIOW7MYR4K3KA5K22W7NW')
        )
        self.assertEqual(
            inst.fullname(chash, 'ogv'),
            path.join(user_dir(), 'NW', 'BNVXVK5DQGIOW7MYR4K3KA5K22W7NW.ogv')
        )
        self.assertEqual(
            inst.fullname(chash, None, True),
            path.join('/home/.dmedia', 'NW', 'BNVXVK5DQGIOW7MYR4K3KA5K22W7NW')
        )
        self.assertEqual(
            inst.fullname(chash, 'ogv', True),
            path.join('/home/.dmedia', 'NW', 'BNVXVK5DQGIOW7MYR4K3KA5K22W7NW.ogv')
        )

    def test_locate(self):
        user = TempDir()
        shared = TempDir()
        inst = self.klass(user.path, shared.path)
        chash = 'NWBNVXVK5DQGIOW7MYR4K3KA5K22W7NW'
        e = raises(filestore.FileNotFound, inst.locate, chash, 'txt')
        self.assertEqual(e.chash, chash)
        self.assertEqual(e.extension, 'txt')
        file1 = shared.write('Novacut', 'NW', 'BNVXVK5DQGIOW7MYR4K3KA5K22W7NW.txt')
        self.assertEqual(inst.locate(chash, 'txt'), file1)
        file2 = user.write('Novacut', 'NW', 'BNVXVK5DQGIOW7MYR4K3KA5K22W7NW.txt')
        self.assertEqual(inst.locate(chash, 'txt'), file2)

    def test_do_add(self):
        chash = 'NWBNVXVK5DQGIOW7MYR4K3KA5K22W7NW'
        h = TempHome()
        inst = self.klass()
        self.assertEqual(inst.user_dir.startswith(h.path), True)

        src = h.write('Novacut', 'Documents', 'test.txt')
        dst = path.join(inst.user_dir, chash[:2], chash[2:] + '.txt')
        self.assertEqual(
            inst._do_add({'src': src, 'meta': {'ext': 'txt'}}),
            {
                'action': 'linked',
                'src': src,
                'dst': dst,
                'meta': {
                    '_id': chash,
                    'bytes': path.getsize(src),
                    'mtime': path.getmtime(src),
                    'ext': 'txt',
                    'mime': 'text/plain',
                    'links': ['Documents/test.txt'],
                },
            }
        )
        self.assertEqual(
            inst._do_add({'src': src, 'meta': {'ext': 'txt'}}),
            {
                'action': 'skipped_duplicate',
                'src': src,
                'dst': dst,
                'meta': {
                    '_id': chash,
                    'ext': 'txt',
                },
            }
        )
        self.assertEqual(path.isfile(dst), True)
        self.assertEqual(open(dst, 'r').read(), 'Novacut')

        # Test that correct mime-type is retrieved for .cr2 files:
        chash = 'HECAODPVLQKOWHA3UVQO6ULCHW4PM3DZ'
        src = h.write('A Canon .cr2 RAW image', 'Pictures', 'IMG_1300.CR2')
        dst = path.join(inst.user_dir, chash[:2], chash[2:] + '.cr2')
        self.assertEqual(
            inst._do_add({'src': src, 'meta': {'ext': 'cr2'}}),
                {
                'action': 'linked',
                'src': src,
                'dst': dst,
                'meta': {
                    '_id': chash,
                    'bytes': path.getsize(src),
                    'mtime': path.getmtime(src),
                    'ext': 'cr2',
                    'mime': 'image/x-canon-cr2',
                    'links': ['Pictures/IMG_1300.CR2'],
                },
            }
        )
