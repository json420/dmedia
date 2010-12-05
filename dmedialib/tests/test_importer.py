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
Unit tests for `dmedialib.importer` module.
"""

import os
from os import path
import hashlib
import tempfile
import shutil
from unittest import TestCase
from .helpers import TempDir, TempHome, raises, sample_mov
from dmedialib.filestore import FileStore
from dmedialib.metastore import MetaStore
from dmedialib import importer

import desktopcouch
from desktopcouch.stop_local_couchdb import stop_couchdb


letters = 'gihdwaqoebxtcklrnsmjufyvpz'
extensions = ('png', 'jpg', 'mov')

relpaths = (
    ('a.jpg',),
    ('b.png',),
    ('c.png',),
    ('.car', 'a.jpg'), # files_iter() is breadth-first
    ('.car', 'b.png'), # and we make sure .dirs are being traversed
    ('.car', 'c.cr2'),
    ('bar', 'a.jpg'),
    ('bar', 'b.png'),
    ('bar', 'c.cr2'),
)


class test_functions(TestCase):
    def test_scanfiles(self):
        f = importer.scanfiles
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
                'doc': {
                    'name': name,
                    'ext': name.split('.')[1].lower(),
                },
            }
            for name in sorted(names)
        )
        self.assertEqual(got, expected)

    def test_files_iter(self):
        f = importer.files_iter
        tmp = TempDir()
        files = []
        for args in relpaths:
            p = tmp.touch('subdir', *args)
            files.append(p)

        # Test when base is a file:
        for p in files:
            self.assertEqual(list(f(p)), [p])

        # Test importing from tmp.path:
        self.assertEqual(list(f(tmp.path)), files)

        # Test from tmp.path/subdir:
        subdir = tmp.join('subdir')
        self.assertEqual(list(f(subdir)), files)

        # Test that OSError propigates up:
        os.chmod(subdir, 0o000)
        e = raises(OSError, list, f(tmp.path))
        self.assertEqual(
            str(e),
            '[Errno 13] Permission denied: %r' % subdir
        )



class test_Importer(TestCase):
    klass = importer.Importer

    def new(self):
        return self.klass(context=self.ctx)

    def setUp(self):
        self.data_dir = tempfile.mkdtemp(prefix='dc-test.')
        cache = os.path.join(self.data_dir, 'cache')
        data = os.path.join(self.data_dir, 'data')
        config = os.path.join(self.data_dir, 'config')
        self.ctx = desktopcouch.local_files.Context(cache, data, config)
        self.home = TempHome()

    def tearDown(self):
        stop_couchdb(ctx=self.ctx)
        shutil.rmtree(self.data_dir)
        self.ctx = None
        self.home.rmtree()
        self.home = None

    def test_init(self):
        inst = self.klass()
        self.assertEqual(inst.home, self.home.path)
        self.assertTrue(isinstance(inst.filestore, FileStore))
        self.assertEqual(inst.filestore.base, self.home.join('.dmedia'))
        self.assertTrue(isinstance(inst.metastore, MetaStore))
