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
import time
from base64 import b32decode, b32encode
from unittest import TestCase
from multiprocessing import current_process
from .helpers import CouchCase, TempDir, TempHome, raises, DummyQueue
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

    def test_create_batch(self):
        f = importer.create_batch
        doc = f()
        self.assertTrue(isinstance(doc, dict))
        self.assertEqual(
            set(doc),
            set([
                '_id',
                'type',
                'time_start',
                'imports',
            ])
        )
        _id = doc['_id']
        self.assertEqual(b32encode(b32decode(_id)), _id)
        self.assertEqual(len(_id), 24)
        self.assertEqual(doc['type'], 'dmedia/batch')
        self.assertTrue(isinstance(doc['time_start'], (int, float)))
        self.assertTrue(doc['time_start'] <= time.time())
        self.assertEqual(doc['imports'], [])

    def test_create_import(self):
        f = importer.create_import
        doc = f('YKGHY6H5RVCDNMUBL4NLP6AU', '/media/EOS_DIGITAL')
        self.assertTrue(isinstance(doc, dict))
        self.assertEqual(
            set(doc),
            set([
                '_id',
                'type',
                'time_start',
                'mount',
                'batch_id',
            ])
        )
        _id = doc['_id']
        self.assertEqual(b32encode(b32decode(_id)), _id)
        self.assertEqual(len(_id), 24)
        self.assertEqual(doc['type'], 'dmedia/import')
        self.assertTrue(isinstance(doc['time_start'], (int, float)))
        self.assertTrue(doc['time_start'] <= time.time())
        self.assertEqual(doc['batch_id'], 'YKGHY6H5RVCDNMUBL4NLP6AU')
        self.assertEqual(doc['mount'], '/media/EOS_DIGITAL')


class test_Importer(CouchCase):
    klass = importer.Importer
    batch_id = 'YKGHY6H5RVCDNMUBL4NLP6AU'

    def new(self, base, extract=False):
        return self.klass(self.batch_id, base, extract, couchdir=self.couchdir)

    def test_init(self):
        tmp = TempDir()
        inst = self.new(tmp.path, True)
        self.assertEqual(inst.batch_id, self.batch_id)
        self.assertEqual(inst.base, tmp.path)
        self.assertTrue(inst.extract is True)
        self.assertEqual(inst.home, self.home.path)
        self.assertTrue(isinstance(inst.filestore, FileStore))
        self.assertEqual(inst.filestore.base, self.home.join('.dmedia'))
        self.assertTrue(isinstance(inst.metastore, MetaStore))

        # Test with extract = False
        inst = self.new(tmp.path, False)
        self.assertTrue(inst.extract is False)

    def test_start(self):
        tmp = TempDir()
        inst = self.new(tmp.path)
        self.assertTrue(inst._import is None)
        _id = inst.start()
        self.assertEqual(len(_id), 24)
        store = MetaStore(couchdir=self.couchdir)
        self.assertEqual(inst._import, store.db[_id])
        self.assertEqual(
            set(inst._import),
            set([
                '_id',
                '_rev',
                'type',
                'time_start',
                'mount',
                'batch_id',
            ])
        )
        self.assertEqual(inst._import['batch_id'], self.batch_id)
        self.assertEqual(inst._import['mount'], tmp.path)

    def test_get_stats(self):
        tmp = TempDir()
        inst = self.new(tmp.path)
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
        tmp = TempDir()
        inst = self.new(tmp.path)
        files = []
        for args in relpaths:
            p = tmp.touch('subdir', *args)
            files.append(p)
        got = inst.scanfiles()
        self.assertEqual(got, tuple(files))
        self.assertTrue(inst.scanfiles() is got)

    def test_import_file(self):
        tmp = TempDir()
        inst = self.new(tmp.path)

        # Test that AmbiguousPath is raised:
        traversal = '/home/foo/.dmedia/../.ssh/id_rsa'
        e = raises(AmbiguousPath, inst.import_file, traversal)
        self.assertEqual(e.filename, traversal)
        self.assertEqual(e.abspath, '/home/foo/.ssh/id_rsa')

        # Test that IOError propagates up with missing file
        nope = tmp.join('nope.mov')
        e = raises(IOError, inst.import_file, nope)
        self.assertEqual(
            str(e),
            '[Errno 2] No such file or directory: %r' % nope
        )

        # Test that IOError propagates up with unreadable file
        nope = tmp.touch('nope.mov')
        os.chmod(nope, 0o000)
        e = raises(IOError, inst.import_file, nope)
        self.assertEqual(
            str(e),
            '[Errno 13] Permission denied: %r' % nope
        )
        os.chmod(nope, 0o600)

        src1 = tmp.copy(sample_mov, 'DCIM', '100EOS5D2', 'MVI_5751.MOV')
        src2 = tmp.copy(sample_mov, 'DCIM', '100EOS5D2', 'duplicate.MOV')

        # Test with new file
        size = path.getsize(src1)
        doc = {
            '_id': sample_mov_hash,
            'type': 'dmedia/file',
            'import_id': None,
            'qid': sample_mov_qid,
            'bytes': size,
            'mtime': path.getmtime(src1),
            'basename': 'MVI_5751.MOV',
            'dirname': 'DCIM/100EOS5D2',
            'ext': 'mov',
            'content_type': 'video/quicktime',
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
        tmp = TempDir()
        inst = self.new(tmp.path)

        src1 = tmp.copy(sample_mov, 'DCIM', '100EOS5D2', 'MVI_5751.MOV')
        dup1 = tmp.copy(sample_mov, 'DCIM', '100EOS5D2', 'MVI_5752.MOV')
        src2 = tmp.copy(sample_thm, 'DCIM', '100EOS5D2', 'MVI_5751.THM')

        import_id = inst.start()
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
                'type': 'dmedia/file',
                'import_id': import_id,
                'qid': sample_mov_qid,
                'bytes': path.getsize(src1),
                'mtime': path.getmtime(src1),
                'basename': 'MVI_5751.MOV',
                'dirname': 'DCIM/100EOS5D2',
                'ext': 'mov',
                'content_type': 'video/quicktime',
            }
        )
        self.assertEqual(items[1][2],
            {
                '_id': sample_thm_hash,
                'type': 'dmedia/file',
                'import_id': import_id,
                'qid': sample_thm_qid,
                'bytes': path.getsize(src2),
                'mtime': path.getmtime(src2),
                'basename': 'MVI_5751.THM',
                'dirname': 'DCIM/100EOS5D2',
                'ext': 'thm',
                'content_type': None,
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


class test_ImportWorker(CouchCase):
    klass = importer.ImportWorker

    def test_run(self):
        q = DummyQueue()
        pid = current_process().pid

        tmp = TempDir()
        batch_id = 'YKGHY6H5RVCDNMUBL4NLP6AU'
        base = tmp.path
        inst = self.klass(q, base, (batch_id, base, False, self.couchdir))

        src1 = tmp.copy(sample_mov, 'DCIM', '100EOS5D2', 'MVI_5751.MOV')
        dup1 = tmp.copy(sample_mov, 'DCIM', '100EOS5D2', 'MVI_5752.MOV')
        src2 = tmp.copy(sample_thm, 'DCIM', '100EOS5D2', 'MVI_5751.THM')

        mov_size = path.getsize(sample_mov)
        thm_size = path.getsize(sample_thm)

        inst.run()

        self.assertEqual(len(q.messages), 6)
        _id = q.messages[0]['args'][1]
        self.assertEqual(len(_id), 24)
        self.assertEqual(
            q.messages[0],
            dict(
                signal='started',
                args=(base, _id),
                worker='ImportWorker',
                pid=pid,
            )
        )
        self.assertEqual(
            q.messages[1],
            dict(
                signal='count',
                args=(base, 3),
                worker='ImportWorker',
                pid=pid,
            )
        )
        self.assertEqual(q.messages[2],
            dict(
                signal='progress',
                args=(base, 1, 3,
                    dict(action='imported', src=src1, _id=sample_mov_hash),
                ),
                worker='ImportWorker',
                pid=pid,
            )
        )
        self.assertEqual(q.messages[3],
            dict(
                signal='progress',
                args=(base, 2, 3,
                    dict(action='imported', src=src2, _id=sample_thm_hash),
                ),
                worker='ImportWorker',
                pid=pid,
            )
        )
        self.assertEqual(q.messages[4],
            dict(
                signal='progress',
                args=(base, 3, 3,
                    dict(action='skipped', src=dup1, _id=sample_mov_hash),
                ),
                worker='ImportWorker',
                pid=pid,
            )
        )
        self.assertEqual(
            q.messages[5],
            dict(
                signal='finished',
                args=(base,
                    dict(
                        imported={'count': 2, 'bytes': (mov_size + thm_size)},
                        skipped={'count': 1, 'bytes': mov_size},
                    ),
                ),
                worker='ImportWorker',
                pid=pid,
            )
        )


class test_ImportManager(CouchCase):
    klass = importer.ImportManager

    def test_start_batch(self):
        inst = self.klass(couchdir=self.couchdir)

        # Test that batch cannot be started when there are active workers:
        inst._workers['foo'] = 'bar'
        self.assertRaises(AssertionError, inst._start_batch)
        inst._workers.clear()

        # Test under normal conditions
        inst._start_batch()
        batch = inst._batch
        self.assertTrue(isinstance(batch, dict))
        self.assertEqual(
            set(batch),
            set(['_id', '_rev', 'type', 'time_start', 'imports'])
        )
        self.assertEqual(batch['type'], 'dmedia/batch')
        self.assertEqual(batch['imports'], [])
        self.assertEqual(inst.db[batch['_id']], batch)

        # Test that batch cannot be re-started without first finishing
        self.assertRaises(AssertionError, inst._start_batch)

    def test_start_import(self):
        inst = self.klass(couchdir=self.couchdir)

        # Test that False is returned
        inst._workers['foo'] = 'bar'
        self.assertTrue(inst.start_import('foo') is False)
