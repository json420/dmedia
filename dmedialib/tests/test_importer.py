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
from multiprocessing import current_process
from .helpers import TempDir, TempHome, raises, DummyQueue
from .helpers import sample_mov, sample_mov_hash, sample_mov_qid
from .helpers import sample_thm, sample_thm_hash, sample_thm_qid
from dmedialib.errors import AmbiguousPath
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
        os.chmod(subdir, 0o700)


class test_Importer(TestCase):
    klass = importer.Importer

    def new(self, base=None, extract=True):
        if base is None:
            self.tmp = TempDir()
            base = self.tmp.path
        return self.klass(base, extract=extract, ctx=self.ctx)

    def setUp(self):
        self.home = TempHome()
        self.tmp = TempDir()
        self.data_dir = tempfile.mkdtemp(prefix='dc-test.')
        cache = os.path.join(self.data_dir, 'cache')
        data = os.path.join(self.data_dir, 'data')
        config = os.path.join(self.data_dir, 'config')
        self.ctx = desktopcouch.local_files.Context(cache, data, config)

    def tearDown(self):
        stop_couchdb(ctx=self.ctx)
        shutil.rmtree(self.data_dir)
        self.ctx = None
        self.home = None
        self.tmp = None

    def test_init(self):
        inst = self.new()
        self.assertEqual(inst.base, self.tmp.path)
        self.assertTrue(inst.extract is True)
        self.assertEqual(inst.home, self.home.path)
        self.assertTrue(isinstance(inst.filestore, FileStore))
        self.assertEqual(inst.filestore.base, self.home.join('.dmedia'))
        self.assertTrue(isinstance(inst.metastore, MetaStore))

    def test_get_stats(self):
        inst = self.new()
        one = inst.get_stats()
        self.assertEqual(one,
             {
                'imported': {
                    'count': 0,
                    'bytes': 0,
                },
                'skipped': {
                    'count': 0,
                    'bytes': 0,
                },
            }
        )
        two = inst.get_stats()
        self.assertFalse(one is two)
        self.assertFalse(one['imported'] is two['imported'])
        self.assertFalse(one['skipped'] is two['skipped'])

    def test_scanfiles(self):
        inst = self.new()
        files = []
        for args in relpaths:
            p = self.tmp.touch('subdir', *args)
            files.append(p)

        got = inst.scanfiles()
        self.assertEqual(got, tuple(files))
        self.assertTrue(inst.scanfiles() is got)

    def test_import_file(self):
        inst = self.new()

        # Test that AmbiguousPath is raised:
        traversal = '/home/foo/.dmedia/../.ssh/id_rsa'
        e = raises(AmbiguousPath, inst.import_file, traversal)
        self.assertEqual(e.filename, traversal)
        self.assertEqual(e.abspath, '/home/foo/.ssh/id_rsa')

        # Test that IOError propagates up with missing file
        nope = self.tmp.join('nope.mov')
        e = raises(IOError, inst.import_file, nope)
        self.assertEqual(
            str(e),
            '[Errno 2] No such file or directory: %r' % nope
        )

        # Test that IOError propagates up with unreadable file
        nope = self.tmp.touch('nope.mov')
        os.chmod(nope, 0o000)
        e = raises(IOError, inst.import_file, nope)
        self.assertEqual(
            str(e),
            '[Errno 13] Permission denied: %r' % nope
        )
        os.chmod(nope, 0o600)

        src1 = self.tmp.copy(sample_mov, 'DCIM', '100EOS5D2', 'MVI_5751.MOV')
        src2 = self.tmp.copy(sample_mov, 'DCIM', '100EOS5D2', 'duplicate.MOV')

        # Test with new file
        size = path.getsize(src1)
        doc = {
            '_id': sample_mov_hash,
            'quickid': sample_mov_qid,
            'bytes': size,
            'mtime': path.getmtime(src1),
            'basename': 'MVI_5751.MOV',
            'dirname': 'DCIM/100EOS5D2',
            'ext': 'mov',
            'mime': 'video/quicktime',
        }
        self.assertEqual(
            inst.import_file(src1),
            ('imported', doc)
        )
        self.assertEqual(inst.get_stats(),
             {
                'imported': {
                    'count': 1,
                    'bytes': size,
                },
                'skipped': {
                    'count': 0,
                    'bytes': 0,
                },
            }
        )

        # Test with duplicate
        (action, wrapper) = inst.import_file(src2)
        self.assertEqual(action, 'skipped')
        doc2 = dict(wrapper)
        del doc2['_rev']
        self.assertEqual(doc2, doc)
        self.assertEqual(inst.get_stats(),
             {
                'imported': {
                    'count': 1,
                    'bytes': size,
                },
                'skipped': {
                    'count': 1,
                    'bytes': size,
                },
            }
        )


    def test_import_all_iter(self):
        inst = self.new()

        src1 = self.tmp.copy(sample_mov, 'DCIM', '100EOS5D2', 'MVI_5751.MOV')
        dup1 = self.tmp.copy(sample_mov, 'DCIM', '100EOS5D2', 'MVI_5752.MOV')
        src2 = self.tmp.copy(sample_thm, 'DCIM', '100EOS5D2', 'MVI_5751.THM')

        items = tuple(inst.import_all_iter())
        self.assertEqual(len(items), 3)
        self.assertEqual(
            [t[:2] for t in items],
            [
                (src1, 'imported'),
                (src2, 'imported'),
                (dup1, 'skipped'),
            ]
        )
        self.assertEqual(items[0][2],
            {
                '_id': sample_mov_hash,
                'quickid': sample_mov_qid,
                'bytes': path.getsize(src1),
                'mtime': path.getmtime(src1),
                'basename': 'MVI_5751.MOV',
                'dirname': 'DCIM/100EOS5D2',
                'ext': 'mov',
                'mime': 'video/quicktime',
            }
        )
        self.assertEqual(items[1][2],
            {
                '_id': sample_thm_hash,
                'quickid': sample_thm_qid,
                'bytes': path.getsize(src2),
                'mtime': path.getmtime(src2),
                'basename': 'MVI_5751.THM',
                'dirname': 'DCIM/100EOS5D2',
                'ext': 'thm',
                'mime': None,
            }
        )

        self.assertEqual(inst.finalize(),
             {
                'imported': {
                    'count': 2,
                    'bytes': path.getsize(src1) + path.getsize(src2),
                },
                'skipped': {
                    'count': 1,
                    'bytes': path.getsize(dup1),
                },
            }
        )


class DummyImporter(object):

    def scanfiles(self, base):
        return list(
            path.join(base, 'DCIM', '100EOS7D', 'MVI_%04d.MOV' % i)
            for i in xrange(17)
        )

    def import_file(self, src):
        return src


class import_files(TestCase):
    klass = importer.import_files

    def setUp(self):
        self.home = TempHome()
        self.tmp = TempDir()
        self.data_dir = tempfile.mkdtemp(prefix='dc-test.')
        cache = os.path.join(self.data_dir, 'cache')
        data = os.path.join(self.data_dir, 'data')
        config = os.path.join(self.data_dir, 'config')
        self.ctx = desktopcouch.local_files.Context(cache, data, config)

    def tearDown(self):
        stop_couchdb(ctx=self.ctx)
        shutil.rmtree(self.data_dir)
        self.ctx = None
        self.home = None
        self.tmp = None

    def test_run(self):
        q = DummyQueue()
        pid = current_process().pid

        base = self.tmp.path
        inst = self.klass(q, (base,))
        inst.ctx = self.ctx

        src1 = self.tmp.copy(sample_mov, 'DCIM', '100EOS5D2', 'MVI_5751.MOV')
        dup1 = self.tmp.copy(sample_mov, 'DCIM', '100EOS5D2', 'MVI_5752.MOV')
        src2 = self.tmp.copy(sample_thm, 'DCIM', '100EOS5D2', 'MVI_5751.THM')

        mov_size = path.getsize(sample_mov)
        thm_size = path.getsize(sample_thm)

        inst.run()

        self.assertEqual(len(q.messages), 6)
        self.assertEqual(
            q.messages[0],
            dict(
                worker='import_files',
                pid=pid,
                signal='ImportStarted',
                args=(base,),
            )
        )
        self.assertEqual(
            q.messages[1],
            dict(
                worker='import_files',
                pid=pid,
                signal='FileCount',
                args=(base, 3),
            )
        )
        self.assertEqual(q.messages[2],
            dict(
                worker='import_files',
                pid=pid,
                signal='ImportProgress',
                args=(base, 1, 3,
                    dict(action='imported', src=src1, _id=sample_mov_hash),
                ),
            )
        )
        self.assertEqual(q.messages[3],
            dict(
                worker='import_files',
                pid=pid,
                signal='ImportProgress',
                args=(base, 2, 3,
                    dict(action='imported', src=src2, _id=sample_thm_hash),
                ),
            )
        )
        self.assertEqual(q.messages[4],
            dict(
                worker='import_files',
                pid=pid,
                signal='ImportProgress',
                args=(base, 3, 3,
                    dict(action='skipped', src=dup1, _id=sample_mov_hash),
                ),
            )
        )
        self.assertEqual(
            q.messages[5],
            dict(
                worker='import_files',
                pid=pid,
                signal='ImportFinished',
                args=(base,
                    dict(
                        imported=2,
                        imported_bytes=(mov_size + thm_size),
                        skipped=1,
                        skipped_bytes=mov_size,
                    ),
                ),
            )
        )
