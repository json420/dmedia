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
Unit tests for `dmedia.importer` module.
"""

import os
from os import path
import hashlib
import tempfile
import shutil
import time
from base64 import b32decode, b32encode, b64encode
from unittest import TestCase
from multiprocessing import current_process
from .helpers import CouchCase, TempDir, TempHome, raises
from .helpers import DummyQueue, DummyCallback, prep_import_source
from .helpers import sample_mov, sample_thm
from .helpers import mov_hash, mov_leaves, mov_qid
from .helpers import thm_hash, thm_leaves, thm_qid
from dmedia.errors import AmbiguousPath
from dmedia.filestore import FileStore
from dmedia.metastore import MetaStore
from dmedia.util import random_id
from dmedia import importer

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
    def test_normalize_ext(self):
        f = importer.normalize_ext
        weird = ['._501', 'movie.mov.', '.movie.mov.', 'movie._501']
        for name in weird:
            self.assertEqual(f(name), (name, None))

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
        machine_id = random_id()
        doc = f(machine_id)

        self.assertTrue(isinstance(doc, dict))
        self.assertEqual(
            set(doc),
            set([
                '_id',
                'type',
                'time',
                'imports',
                'imported',
                'skipped',
                'machine_id',
            ])
        )
        _id = doc['_id']
        self.assertEqual(b32encode(b32decode(_id)), _id)
        self.assertEqual(len(_id), 24)
        self.assertEqual(doc['type'], 'dmedia/batch')
        self.assertTrue(isinstance(doc['time'], (int, float)))
        self.assertTrue(doc['time'] <= time.time())
        self.assertEqual(doc['imports'], [])
        self.assertEqual(doc['imported'], {'count': 0, 'bytes': 0})
        self.assertEqual(doc['skipped'], {'count': 0, 'bytes': 0})
        self.assertEqual(doc['machine_id'], machine_id)

    def test_create_import(self):
        f = importer.create_import

        base = '/media/EOS_DIGITAL'
        batch_id = random_id()
        machine_id = random_id()

        keys = set([
            '_id',
            'type',
            'time',
            'base',
            'batch_id',
            'machine_id',
        ])

        doc = f(base, batch_id=batch_id, machine_id=machine_id)
        self.assertTrue(isinstance(doc, dict))
        self.assertEqual(set(doc), keys)

        _id = doc['_id']
        self.assertEqual(b32encode(b32decode(_id)), _id)
        self.assertEqual(len(_id), 24)

        self.assertEqual(doc['type'], 'dmedia/import')
        self.assertTrue(isinstance(doc['time'], (int, float)))
        self.assertTrue(doc['time'] <= time.time())
        self.assertEqual(doc['base'], base)
        self.assertEqual(doc['batch_id'], batch_id)
        self.assertEqual(doc['machine_id'], machine_id)

        doc = f(base)
        self.assertEqual(set(doc), keys)
        self.assertEqual(doc['batch_id'], None)
        self.assertEqual(doc['machine_id'], None)

    def test_to_dbus_stats(self):
        f = importer.to_dbus_stats
        stats = dict(
            imported={'count': 17, 'bytes': 98765},
            skipped={'count': 3, 'bytes': 12345},
        )
        result = dict(
            imported=17,
            imported_bytes=98765,
            skipped=3,
            skipped_bytes=12345,
        )
        self.assertEqual(f(stats), result)

    def test_accumulate_stats(self):
        f = importer.accumulate_stats
        accum = dict(
            imported={'count': 0, 'bytes': 0},
            skipped={'count': 0, 'bytes': 0},
        )
        stats1 = dict(
            imported={'count': 17, 'bytes': 98765},
            skipped={'count': 3, 'bytes': 12345},
        )
        stats2 = dict(
            imported={'count': 18, 'bytes': 9876},
            skipped={'count': 5, 'bytes': 1234},
        )
        f(accum, stats1)
        self.assertEqual(accum, stats1)
        f(accum, dict(stats2))
        self.assertEqual(
            accum,
            dict(
                imported={'count': 17 + 18, 'bytes': 98765 + 9876},
                skipped={'count': 3 + 5, 'bytes': 12345 + 1234},
            )
        )


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
                'time',
                'base',
                'batch_id',
                'machine_id',
            ])
        )
        self.assertEqual(inst._import['batch_id'], self.batch_id)
        self.assertEqual(
            inst._import['machine_id'],
            inst.metastore.machine_id
        )
        self.assertEqual(inst._import['base'], tmp.path)

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
            '_id': mov_hash,
            '_attachments': {
                'leaves': {
                    'data': b64encode(''.join(mov_leaves)),
                    'content_type': 'application/octet-stream',
                }
            },
            'type': 'dmedia/file',
            'import_id': None,
            'qid': mov_qid,
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
        del doc2['_attachments']
        del doc['_attachments']
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
                '_id': mov_hash,
                '_attachments': {
                    'leaves': {
                        'data': b64encode(''.join(mov_leaves)),
                        'content_type': 'application/octet-stream',
                    }
                },
                'type': 'dmedia/file',
                'import_id': import_id,
                'qid': mov_qid,
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
                '_id': thm_hash,
                '_attachments': {
                    'leaves': {
                        'data': b64encode(''.join(thm_leaves)),
                        'content_type': 'application/octet-stream',
                    }
                },
                'type': 'dmedia/file',
                'import_id': import_id,
                'qid': thm_qid,
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
                args=(base, _id, 3),
                worker='ImportWorker',
                pid=pid,
            )
        )
        self.assertEqual(q.messages[2],
            dict(
                signal='progress',
                args=(base, _id, 1, 3,
                    dict(action='imported', src=src1, _id=mov_hash)
                ),
                worker='ImportWorker',
                pid=pid,
            )
        )
        self.assertEqual(q.messages[3],
            dict(
                signal='progress',
                args=(base, _id, 2, 3,
                    dict(action='imported', src=src2, _id=thm_hash)
                ),
                worker='ImportWorker',
                pid=pid,
            )
        )
        self.assertEqual(q.messages[4],
            dict(
                signal='progress',
                args=(base, _id, 3, 3,
                    dict(action='skipped', src=dup1, _id=mov_hash)
                ),
                worker='ImportWorker',
                pid=pid,
            )
        )
        self.assertEqual(
            q.messages[5],
            dict(
                signal='finished',
                args=(base, _id,
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
        callback = DummyCallback()
        inst = self.klass(callback, self.couchdir)

        # Test that batch cannot be started when there are active workers:
        inst._workers['foo'] = 'bar'
        self.assertRaises(AssertionError, inst._start_batch)
        inst._workers.clear()

        # Test under normal conditions
        inst._completed = 17
        inst._total = 18
        inst._start_batch()
        self.assertEqual(inst._completed, 0)
        self.assertEqual(inst._total, 0)
        batch = inst._batch
        batch_id = batch['_id']
        self.assertTrue(isinstance(batch, dict))
        self.assertEqual(
            set(batch),
            set([
                '_id', '_rev',
                'type',
                'time',
                'imports',
                'imported',
                'skipped',
                'machine_id',
            ])
        )
        self.assertEqual(batch['type'], 'dmedia/batch')
        self.assertEqual(batch['imports'], [])
        self.assertEqual(batch['machine_id'], inst.metastore.machine_id)
        self.assertEqual(inst.db[batch['_id']], batch)
        self.assertEqual(
            callback.messages,
            [
                ('BatchStarted', (batch_id,)),
            ]
        )

        # Test that batch cannot be re-started without first finishing
        self.assertRaises(AssertionError, inst._start_batch)

    def test_finish_batch(self):
        callback = DummyCallback()
        inst = self.klass(callback, self.couchdir)
        batch_id = random_id()
        inst._batch = dict(
            _id=batch_id,
            imported={'count': 17, 'bytes': 98765},
            skipped={'count': 3, 'bytes': 12345},
        )

        # Make sure it checks that workers is empty
        inst._workers['foo'] = 'bar'
        self.assertRaises(AssertionError, inst._finish_batch)
        self.assertEqual(callback.messages, [])

        # Check that it fires signal correctly
        inst._workers.clear()
        inst._finish_batch()
        self.assertEqual(inst._batch, None)
        stats = dict(
            imported=17,
            imported_bytes=98765,
            skipped=3,
            skipped_bytes=12345,
        )
        self.assertEqual(
            callback.messages,
            [
                ('BatchFinished', (batch_id, stats)),
            ]
        )
        doc = inst.db[batch_id]
        self.assertEqual(
            set(doc),
            set([
                '_id',
                '_rev',
                'imported',
                'skipped',
                'time_end',
            ])
        )
        cur = time.time()
        self.assertTrue(cur - 1 <= doc['time_end'] <= cur)

    def test_on_started(self):
        callback = DummyCallback()
        inst = self.klass(callback, self.couchdir)
        self.assertEqual(callback.messages, [])
        inst._start_batch()
        batch_id = inst._batch['_id']
        self.assertEqual(inst.db[batch_id]['imports'], [])
        self.assertEqual(
            callback.messages,
            [
                ('BatchStarted', (batch_id,)),
            ]
        )

        one = TempDir()
        one_id = random_id()
        inst.on_started(one.path, one_id)
        self.assertEqual(inst.db[batch_id]['imports'], [one_id])
        self.assertEqual(
            callback.messages,
            [
                ('BatchStarted', (batch_id,)),
                ('ImportStarted', (one.path, one_id)),
            ]
        )

        two = TempDir()
        two_id = random_id()
        inst.on_started(two.path, two_id)
        self.assertEqual(inst.db[batch_id]['imports'], [one_id, two_id])
        self.assertEqual(
            callback.messages,
            [
                ('BatchStarted', (batch_id,)),
                ('ImportStarted', (one.path, one_id)),
                ('ImportStarted', (two.path, two_id)),
            ]
        )

    def test_on_count(self):
        callback = DummyCallback()
        inst = self.klass(callback, self.couchdir)
        self.assertEqual(callback.messages, [])

        one = TempDir()
        one_id = random_id()
        self.assertEqual(inst._total, 0)
        inst.on_count(one.path, one_id, 378)
        self.assertEqual(inst._total, 378)
        self.assertEqual(
            callback.messages,
            [
                ('ImportCount', (one.path, one_id, 378)),
            ]
        )

        two = TempDir()
        two_id = random_id()
        self.assertEqual(inst._total, 378)
        inst.on_count(two.path, two_id, 17)
        self.assertEqual(inst._total, 395)
        self.assertEqual(
            callback.messages,
            [
                ('ImportCount', (one.path, one_id, 378)),
                ('ImportCount', (two.path, two_id, 17)),
            ]
        )

    def test_on_progress(self):
        callback = DummyCallback()
        inst = self.klass(callback, self.couchdir)
        self.assertEqual(callback.messages, [])

        one = TempDir()
        one_id = random_id()
        one_info = dict(
            src=one.join('happy.mov'),
            _id=mov_hash,
            action='imported',
        )
        self.assertEqual(inst._completed, 0)
        inst.on_progress(one.path, one_id, 1, 18, one_info)
        self.assertEqual(inst._completed, 1)
        self.assertEqual(
            callback.messages,
            [
                ('ImportProgress', (one.path, one_id, 1, 18, one_info)),
            ]
        )

        two = TempDir()
        two_id = random_id()
        two_info = dict(
            src=two.join('happy.thm'),
            _id='BKSTXEA5MI5DZTUDIHLI3KM3',
            action='imported',
        )
        self.assertEqual(inst._completed, 1)
        inst.on_progress(two.path, two_id, 2, 18, two_info)
        self.assertEqual(inst._completed, 2)
        self.assertEqual(
            callback.messages,
            [
                ('ImportProgress', (one.path, one_id, 1, 18, one_info)),
                ('ImportProgress', (two.path, two_id, 2, 18, two_info)),
            ]
        )

    def test_on_finished(self):
        callback = DummyCallback()
        inst = self.klass(callback, self.couchdir)
        batch_id = random_id()
        inst._batch = dict(
            _id=batch_id,
            imported={'count': 0, 'bytes': 0},
            skipped={'count': 0, 'bytes': 0},
        )

        # Call with first import
        one = TempDir()
        one_id = random_id()
        one_stats = dict(
            imported={'count': 17, 'bytes': 98765},
            skipped={'count': 3, 'bytes': 12345},
        )
        inst.on_finished(one.path, one_id, one_stats)
        self.assertEqual(
            callback.messages,
            [
                ('ImportFinished', (one.path, one_id, dict(
                        imported=17,
                        imported_bytes=98765,
                        skipped=3,
                        skipped_bytes=12345,
                    ))
                ),
            ]
        )
        self.assertEqual(
            set(inst._batch),
            set(['_id', '_rev', 'imported', 'skipped'])
        )
        self.assertEqual(inst._batch['_id'], batch_id)
        self.assertEqual(
            inst._batch['imported'],
            {'count': 17, 'bytes': 98765}
        )
        self.assertEqual(
            inst._batch['skipped'],
            {'count': 3, 'bytes': 12345}
        )

        # Call with second import
        two = TempDir()
        two_id = random_id()
        two_stats = dict(
            imported={'count': 18, 'bytes': 9876},
            skipped={'count': 5, 'bytes': 1234},
        )
        inst.on_finished(two.path, two_id, two_stats)
        self.assertEqual(
            callback.messages,
            [
                ('ImportFinished', (one.path, one_id, dict(
                        imported=17,
                        imported_bytes=98765,
                        skipped=3,
                        skipped_bytes=12345,
                    ))
                ),
                ('ImportFinished', (two.path, two_id, dict(
                        imported=18,
                        imported_bytes=9876,
                        skipped=5,
                        skipped_bytes=1234,
                    ))
                ),
            ]
        )
        self.assertEqual(
            set(inst._batch),
            set(['_id', '_rev', 'imported', 'skipped'])
        )
        self.assertEqual(inst._batch['_id'], batch_id)
        self.assertEqual(
            inst._batch['imported'],
            {'count': 17 + 18, 'bytes': 98765 + 9876}
        )
        self.assertEqual(
            inst._batch['skipped'],
            {'count': 3 + 5, 'bytes': 12345 + 1234}
        )

    def test_get_batch_progress(self):
        inst = self.klass(couchdir=self.couchdir)
        self.assertEqual(inst.get_batch_progress(), (0, 0))
        inst._total = 18
        self.assertEqual(inst.get_batch_progress(), (0, 18))
        inst._completed = 17
        self.assertEqual(inst.get_batch_progress(), (17, 18))
        inst._completed = 0
        inst._total = 0
        self.assertEqual(inst.get_batch_progress(), (0, 0))

    def test_start_import(self):
        callback = DummyCallback()
        inst = self.klass(callback, self.couchdir)
        self.assertTrue(inst.start())

        tmp = TempDir()
        (src1, src2, dup1) = prep_import_source(tmp)
        base = tmp.path
        mov_size = path.getsize(sample_mov)
        thm_size = path.getsize(sample_thm)

        # Test that False is returned when key is present
        inst._workers[base] = 'foo'
        self.assertTrue(inst.start_import(base, False) is False)

        # Now do the real thing
        inst._workers.clear()
        self.assertEqual(callback.messages, [])
        self.assertTrue(inst.start_import(base, False) is True)
        while inst._workers:
            time.sleep(1)

        self.assertEqual(len(callback.messages), 8)
        batch_id = callback.messages[0][1][0]
        import_id = callback.messages[1][1][1]
        self.assertEqual(
            callback.messages[0],
            ('BatchStarted', (batch_id,))
        )
        self.assertEqual(
            callback.messages[1],
            ('ImportStarted', (base, import_id))
        )
        self.assertEqual(
            callback.messages[2],
            ('ImportCount', (base, import_id, 3))
        )
        self.assertEqual(
            callback.messages[3],
            ('ImportProgress', (base, import_id, 1, 3,
                    dict(action='imported', src=src1, _id=mov_hash)
                )
            )
        )
        self.assertEqual(
            callback.messages[4],
            ('ImportProgress', (base, import_id, 2, 3,
                    dict(action='imported', src=src2, _id=thm_hash)
                )
            )
        )
        self.assertEqual(
            callback.messages[5],
            ('ImportProgress', (base, import_id, 3, 3,
                    dict(action='skipped', src=dup1, _id=mov_hash)
                )
            )
        )
        self.assertEqual(
            callback.messages[6],
            ('ImportFinished', (base, import_id,
                    dict(
                        imported=2,
                        imported_bytes=(mov_size + thm_size),
                        skipped=1,
                        skipped_bytes=mov_size,
                    )
                )
            )
        )
        self.assertEqual(
            callback.messages[7],
            ('BatchFinished', (batch_id,
                    dict(
                        imported=2,
                        imported_bytes=(mov_size + thm_size),
                        skipped=1,
                        skipped_bytes=mov_size,
                    )
                )
            )
        )

    def test_list_imports(self):
        inst = self.klass(couchdir=self.couchdir)
        self.assertEqual(inst.list_imports(), [])
        inst._workers.update(
            dict(foo=None, bar=None, baz=None)
        )
        self.assertEqual(inst.list_imports(), ['bar', 'baz', 'foo'])
        inst._workers.clear()
        self.assertEqual(inst.list_imports(), [])
