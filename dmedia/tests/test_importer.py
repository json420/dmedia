# Authors:
#   Jason Gerard DeRose <jderose@novacut.com>
#   Akshat Jain <ssj6akshat1234@gmail.com>
#   David Green <david4dev@gmail.com>
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

from unittest import TestCase
import time

import filestore
from microfiber import random_id, Database

from .couch import CouchCase
from .base import TempDir

from dmedia import importer, schema


class DummyQueue(object):
    def __init__(self):
        self.items = []

    def put(self, item):
        self.items.append(item)
  

class DummyCallback(object):
    def __init__(self):
        self.messages = []

    def __call__(self, signal, args):
        self.messages.append((signal, args))


class TestFunctions(TestCase):
    def test_normalize_ext(self):
        self.skipTest('fix when normalize_ext() is used again')
        f = importer.normalize_ext
        weird = ['._501', 'movie.mov.', '.movie.mov.', 'movie._501']
        for name in weird:
            self.assertEqual(f(name), (name, None))

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


class ImportCase(CouchCase):
    def setUp(self):
        super().setUp()
        self.q = DummyQueue()

        self.src = TempDir()

        self.dst1 = TempDir()
        self.dst2 = TempDir()
        self.store1_id = random_id()
        self.store2_id = random_id()
        self.env['filestores'] = [
            {
                '_id': self.store1_id,
                'parentdir': self.dst1.dir,
                'copies': 1,
            },
            {
                '_id': self.store2_id,
                'parentdir': self.dst2.dir,
                'copies': 2,
            },
        ]

        self.db = Database('dmedia', self.env)

    def tearDown(self):
        super().tearDown()
        self.q = None
        self.src = None
        self.dst1 = None
        self.dst2 = None


class TestImportWorker(ImportCase):
    def setUp(self):
        super().setUp()
        self.batch_id = random_id()
        self.env['batch_id'] = self.batch_id

    def test_random_batch(self):
        key = self.src.dir
        args = (self.src.dir,)
        inst = importer.ImportWorker(self.env, self.q, key, args)
        self.assertEqual(inst.basedir, self.src.dir)

        # start()
        self.assertEqual(self.q.items, [])
        self.assertIsNone(inst.id)
        self.assertIsNone(inst.doc)
        inst.start()
        doc = self.db.get(inst.id)
        self.assertEqual(doc['basedir'], self.src.dir)
        self.assertEqual(doc['machine_id'], self.machine_id)
        self.assertEqual(doc['batch_id'], self.batch_id)
        self.assertEqual(doc['import_order'], [])
        self.assertEqual(doc['files'], {})
        self.assertEqual(doc['stats'],
            {
                'total': {'count': 0, 'bytes': 0},
                'duplicate': {'count': 0, 'bytes': 0},
                'empty': {'count': 0, 'bytes': 0},
                'new': {'count': 0, 'bytes': 0},
            }
        )
        self.assertEqual(self.q.items[0]['signal'], 'started')
        self.assertEqual(self.q.items[0]['args'], (self.src.dir, inst.id))

        # scan()
        (batch, result) = self.src.random_batch(25)
        files = dict(
            (file.name, {'bytes': file.size, 'mtime': file.mtime})
            for file in batch.files
        )
        inst.scan()
        doc = self.db.get(inst.id)
        self.assertEqual(doc['stats'],
            {
                'total': {'bytes': batch.size, 'count': batch.count},
                'duplicate': {'count': 0, 'bytes': 0},
                'empty': {'count': 0, 'bytes': 0},
                'new': {'count': 0, 'bytes': 0},
            }
        )
        self.assertEqual(doc['import_order'],
            [file.name for file in batch.files]
        )
        self.assertEqual(doc['files'], files)
        item = self.q.items[1]
        self.assertEqual(item['signal'], 'scanned')
        self.assertEqual(
            item['args'],
            (self.src.dir, batch.count, batch.size)
        )

        # get_filestores()
        stores = inst.get_filestores()
        self.assertEqual(len(stores), 2)
        fs1 = stores[0]
        self.assertIsInstance(fs1, filestore.FileStore)
        self.assertEquals(fs1.parentdir, self.dst1.dir)
        self.assertEquals(fs1.id, self.store1_id)
        self.assertEquals(fs1.copies, 1)

        fs2 = stores[1]
        self.assertIsInstance(fs2, filestore.FileStore)
        self.assertEquals(fs2.parentdir, self.dst2.dir)
        self.assertEquals(fs2.id, self.store2_id)
        self.assertEquals(fs2.copies, 2)

        # import_all()
        for (file, ch) in result:
            files[file.name].update(
                {'status': 'new', 'id': ch.id}
            )
        inst.import_all()
        doc = self.db.get(inst.id)
        stats = {
            'total': {'bytes': batch.size, 'count': batch.count},
            'duplicate': {'count': 0, 'bytes': 0},
            'empty': {'count': 0, 'bytes': 0},
            'new': {'bytes': batch.size, 'count': batch.count},
        }
        self.assertEqual(doc['stats'], stats)
        self.assertEqual(doc['import_order'],
            [file.name for file in batch.files]
        )
        self.assertEqual(doc['files'], files)

        # Check the 'progress' signals
        for (i, file) in enumerate(batch.files):
            item = self.q.items[i + 2]
            self.assertEqual(item['signal'], 'progress')
            self.assertEqual(item['args'], (self.src.dir, file.size))

        # Check the 'finished' signal
        item = self.q.items[-1]
        self.assertEqual(item['signal'], 'finished')
        self.assertEqual(item['args'], (self.src.dir, stats))

        # Check all the dmedia/file docs:
        for (file, ch) in result:
            doc = self.db.get(ch.id)
            schema.check_file(doc)
            self.assertEqual(doc['import_id'], inst.id)
            self.assertEqual(doc['mtime'], file.mtime)
            self.assertEqual(doc['bytes'], ch.file_size)
            (content_type, leaf_hashes) = self.db.get_att(ch.id, 'leaf_hashes')
            self.assertEqual(content_type, 'application/octet-stream')
            self.assertEqual(leaf_hashes, ch.leaf_hashes)
            self.assertEqual(
                set(doc['stored']),
                set([self.store1_id, self.store2_id])
            )
            self.assertEqual(
                doc['stored'][self.store1_id],
                {
                    'mtime': fs1.stat(ch.id).mtime,
                    'copies': 1,
                }
            )
            self.assertEqual(
                doc['stored'][self.store2_id],
                {
                    'mtime': fs2.stat(ch.id).mtime,
                    'copies': 2,
                }
            )


class TestImportManager(ImportCase):
    klass = importer.ImportManager

    def new(self, callback=None):
        return self.klass(self.env, callback)

    def test_first_worker_starting(self):
        callback = DummyCallback()
        inst = self.new(callback)

        # Test that batch cannot be started when there are active workers:
        inst._workers['foo'] = 'bar'
        self.assertRaises(AssertionError, inst.first_worker_starting)
        inst._workers.clear()

        # Test under normal conditions
        inst._count = 17
        inst._total_count = 18
        inst._bytes = 19
        inst._total_bytes = 20

        inst.first_worker_starting()
        self.assertEqual(inst._count, 0)
        self.assertEqual(inst._total_count, 0)
        self.assertEqual(inst._bytes, 0)
        self.assertEqual(inst._total_bytes, 0)

        batch = inst.doc
        self.assertTrue(isinstance(batch, dict))
        self.assertEqual(
            set(batch),
            set([
                '_id',
                '_rev',
                'ver',
                'type',
                'time',
                'imports',
                'errors',
                'machine_id',
                'stats',
            ])
        )
        self.assertEqual(batch['type'], 'dmedia/batch')
        self.assertEqual(batch['imports'], [])
        self.assertEqual(batch['machine_id'], self.machine_id)
        self.assertEqual(inst.db.get(batch['_id']), batch)
        self.assertEqual(
            callback.messages,
            [
                ('batch_started', (batch['_id'],)),
            ]
        )

        # Test that batch cannot be re-started without first finishing
        self.assertRaises(AssertionError, inst.first_worker_starting)

    def test_last_worker_finished(self):
        callback = DummyCallback()
        inst = self.new(callback)
        batch_id = random_id()
        stats = {
            'total': {'count': 0, 'bytes': 0},
            'new': {'count': 0, 'bytes': 0},
            'duplicate': {'count': 0, 'bytes': 0},
            'empty': {'count': 0, 'bytes': 0},
        }
        inst.doc = dict(
            _id=batch_id,
            stats=stats,
        )

        # Make sure it checks that workers is empty
        inst._workers['foo'] = 'bar'
        self.assertRaises(AssertionError, inst.last_worker_finished)
        self.assertEqual(callback.messages, [])

        # Check that it fires signal correctly
        inst._workers.clear()
        inst.last_worker_finished()
        self.assertEqual(inst.doc, None)
        self.assertEqual(
            callback.messages,
            [
                ('batch_finished', (batch_id, stats)),
            ]
        )
        doc = inst.db.get(batch_id)
        self.assertEqual(
            set(doc),
            set([
                '_id',
                '_rev',
                'stats',
                'time_end',
            ])
        )
        self.assertLessEqual(doc['time_end'], time.time())

    def test_on_error(self):
        callback = DummyCallback()
        inst = self.new(callback)

        # Make sure it works when doc is None:
        inst.on_error('foo', 'IOError', 'nope')
        self.assertEqual(inst.doc, None)

        # Test normally:
        inst.first_worker_starting()
        self.assertEqual(inst.doc['errors'], [])
        inst.on_error('foo', 'IOError', 'nope')
        doc = inst.db.get(inst.doc['_id'])
        self.assertEqual(
            doc['errors'],
            [
                {'key': 'foo', 'name': 'IOError', 'msg': 'nope'},
            ]
        )
        inst.on_error('bar', 'error!', 'no way')
        doc = inst.db.get(inst.doc['_id'])
        self.assertEqual(
            doc['errors'],
            [
                {'key': 'foo', 'name': 'IOError', 'msg': 'nope'},
                {'key': 'bar', 'name': 'error!', 'msg': 'no way'},
            ]
        )

    def test_get_worker_env(self):
        batch_id = random_id()
        inst = self.new()
        env = dict(self.env)
        assert 'batch_id' not in env
        inst.doc = {'_id': batch_id}
        env['batch_id'] = batch_id
        self.assertEqual(
            inst.get_worker_env('ImportWorker', 'a key', ('some', 'args')),
            env
        )

    def test_on_started(self):
        callback = DummyCallback()
        inst = self.new(callback)
        self.assertEqual(callback.messages, [])
        inst.first_worker_starting()
        batch_id = inst.doc['_id']
        self.assertEqual(inst.db.get(batch_id)['imports'], [])
        self.assertEqual(
            callback.messages,
            [
                ('batch_started', (batch_id,)),
            ]
        )

        one = TempDir()
        one_id = random_id()
        inst.on_started(one.dir, one_id)
        self.assertEqual(inst.db.get(batch_id)['imports'], [one_id])
        self.assertEqual(
            callback.messages,
            [
                ('batch_started', (batch_id,)),
                ('import_started', (one.dir, one_id)),
            ]
        )

        two = TempDir()
        two_id = random_id()
        inst.on_started(two.dir, two_id)
        self.assertEqual(inst.db.get(batch_id)['imports'], [one_id, two_id])
        self.assertEqual(
            callback.messages,
            [
                ('batch_started', (batch_id,)),
                ('import_started', (one.dir, one_id)),
                ('import_started', (two.dir, two_id)),
            ]
        )

    def test_on_scanned(self):
        callback = DummyCallback()
        inst = self.new(callback)
        self.assertEqual(callback.messages, [])

        one = TempDir()
        self.assertEqual(inst._total_count, 0)
        self.assertEqual(inst._total_bytes, 0)
        inst.on_scanned(one.dir, 123, 4567)
        self.assertEqual(inst._total_count, 123)
        self.assertEqual(inst._total_bytes, 4567)
        self.assertEqual(
            callback.messages,
            [
                ('batch_progress', (0, 123, 0, 4567)),
            ]
        )

        two = TempDir()
        inst.on_scanned(two.dir, 234, 5678)
        self.assertEqual(inst._total_count, 123 + 234)
        self.assertEqual(inst._total_bytes, 4567 + 5678)
        self.assertEqual(
            callback.messages,
            [
                ('batch_progress', (0, 123, 0, 4567)),
                ('batch_progress', (0, 123 + 234, 0, 4567 + 5678)),
            ]
        )

    def test_on_progress(self):
        callback = DummyCallback()
        inst = self.new(callback)
        self.assertEqual(callback.messages, [])

        self.assertEqual(inst._count, 0)
        self.assertEqual(inst._bytes, 0)
        inst._total_count = 3
        inst._total_bytes = 123

        one = TempDir()
        inst.on_progress(one.dir, 17)
        self.assertEqual(inst._count, 1)
        self.assertEqual(inst._bytes, 17)
        self.assertEqual(
            callback.messages,
            [
                ('batch_progress', (1, 3, 17, 123)),
            ]
        )

        two = TempDir()
        inst.on_progress(two.dir, 18)
        self.assertEqual(inst._count, 2)
        self.assertEqual(inst._bytes, 17 + 18)

        self.assertEqual(
            callback.messages,
            [
                ('batch_progress', (1, 3, 17, 123)),
                ('batch_progress', (2, 3, 17+18, 123)),
            ]
        )

    def test_on_finished(self):
        callback = DummyCallback()
        inst = self.new(callback)

        batch_id = random_id()
        stats = {
            'total': {'count': 0, 'bytes': 0},
            'new': {'count': 0, 'bytes': 0},
            'duplicate': {'count': 0, 'bytes': 0},
            'empty': {'count': 0, 'bytes': 0},
        }
        inst.doc = dict(
            _id=batch_id,
            stats=stats,
        )

        # Call with 1st import
        one = TempDir()
        one_stats = {
            'total': {'count': 1, 'bytes': 2},
            'new': {'count': 3, 'bytes': 4},
            'duplicate': {'count': 5, 'bytes': 6},
            'empty': {'count': 7, 'bytes': 8},
        }
        inst.on_finished(one.dir, one_stats)
        doc = self.db.get(batch_id)
        self.assertEqual(doc['stats'], one_stats)

        # Call with 2nd import
        two = TempDir()
        two_stats = {
            'total': {'count': 8, 'bytes': 7},
            'new': {'count': 6, 'bytes': 5},
            'duplicate': {'count': 4, 'bytes': 3},
            'empty': {'count': 2, 'bytes': 1},
        }
        inst.on_finished(two.dir, two_stats)
        doc = self.db.get(batch_id)
        self.assertEqual(doc['stats'],
            {
                'total': {'count': 9, 'bytes': 9},
                'new': {'count': 9, 'bytes': 9},
                'duplicate': {'count': 9, 'bytes': 9},
                'empty': {'count': 9, 'bytes': 9},
            }
        )

    def test_get_batch_progress(self):
        inst = self.new()
        self.assertEqual(
            inst.get_batch_progress(),
            (0, 0, 0, 0)
        )
        inst._count = 1
        inst._total_count = 2
        inst._bytes = 3
        inst._total_bytes = 4
        self.assertEqual(
            inst.get_batch_progress(),
            (1, 2, 3, 4)
        )

    def test_start_import(self):
        callback = DummyCallback()
        inst = self.new(callback)

        # Test that False is returned when key is present
        inst._workers[self.src.dir] = 'foo'
        self.assertFalse(inst.start_import(self.src.dir))

        # Now do the real thing with 25 random files, 11 empty files:
        (batch, result) = self.src.random_batch(25, empties=11)
        ids = set(
            ch.id for ch in filter(None, (ch for (file, ch) in result))
        )

        inst._workers.clear()
        self.assertEqual(callback.messages, [])
        self.assertTrue(inst.start_import(self.src.dir))
        while inst._workers:
            time.sleep(0.5)
        time.sleep(1.0)

        self.assertEqual(len(callback.messages), 40)

        batch_id = callback.messages[0][1][0]
        import_id = callback.messages[1][1][1]
        self.assertEqual(
            callback.messages[0],
            ('batch_started', (batch_id,))
        )
        self.assertEqual(
            callback.messages[1],
            ('import_started', (self.src.dir, import_id))
        )
        self.assertEqual(
            callback.messages[2],
            ('batch_progress', (0, batch.count, 0, batch.size))
        )
        size = 0
        for (i, file) in enumerate(batch.files):
            size += file.size
            self.assertEqual(
                callback.messages[i + 3],
                ('batch_progress', (i + 1, batch.count, size, batch.size))
            )

        stats = {
            'total': {'count': batch.count, 'bytes': batch.size},
            'new': {'count': batch.count - 11, 'bytes': batch.size},
            'duplicate': {'count': 0, 'bytes': 0},
            'empty': {'count': 11, 'bytes': 0},
        }
        self.assertEqual(
            callback.messages[-1],
            ('batch_finished', (batch_id, stats))
        )

        fs1 = filestore.FileStore(self.dst1.dir)
        fs2 = filestore.FileStore(self.dst2.dir)
        self.assertEqual(set(st.id for st in fs1), ids)
        self.assertEqual(set(st.id for st in fs2), ids)

        # Check all the dmedia/file docs:
        for (file, ch) in result:
            if ch is None:
                continue
            doc = self.db.get(ch.id)
            schema.check_file(doc)
            self.assertTrue(doc['_rev'].startswith('1-'))
            self.assertEqual(doc['import_id'], import_id)
            self.assertEqual(doc['mtime'], file.mtime)
            self.assertEqual(doc['bytes'], file.size)
            (content_type, leaf_hashes) = self.db.get_att(ch.id, 'leaf_hashes')
            self.assertEqual(content_type, 'application/octet-stream')
            self.assertEqual(leaf_hashes, ch.leaf_hashes)
            self.assertEqual(
                set(doc['stored']),
                set([self.store1_id, self.store2_id])
            )
            self.assertEqual(
                doc['stored'][self.store1_id],
                {
                    'mtime': fs1.stat(ch.id).mtime,
                    'copies': 1,
                }
            )
            self.assertEqual(
                doc['stored'][self.store2_id],
                {
                    'mtime': fs2.stat(ch.id).mtime,
                    'copies': 2,
                }
            )

        # Verify all the files
        for (file, ch) in result:
            if ch is None:
                continue
            self.assertEqual(fs1.verify(ch.id), ch)
            self.assertEqual(fs2.verify(ch.id), ch)


        ##################################################################
        # Okay, now run the whole thing again when they're all duplicates:
        callback.messages = []
        self.assertTrue(inst.start_import(self.src.dir))
        while inst._workers:
            time.sleep(0.5)
        time.sleep(1.0)

        self.assertEqual(len(callback.messages), 40)

        batch_id = callback.messages[0][1][0]
        import_id = callback.messages[1][1][1]
        self.assertEqual(
            callback.messages[0],
            ('batch_started', (batch_id,))
        )
        self.assertEqual(
            callback.messages[1],
            ('import_started', (self.src.dir, import_id))
        )
        self.assertEqual(
            callback.messages[2],
            ('batch_progress', (0, batch.count, 0, batch.size))
        )
        size = 0
        for (i, file) in enumerate(batch.files):
            size += file.size
            self.assertEqual(
                callback.messages[i + 3],
                ('batch_progress', (i + 1, batch.count, size, batch.size))
            )

        stats = {
            'total': {'count': batch.count, 'bytes': batch.size},
            'new': {'count': 0, 'bytes': 0},
            'duplicate': {'count': batch.count - 11, 'bytes': batch.size},
            'empty': {'count': 11, 'bytes': 0},
        }
        self.assertEqual(
            callback.messages[-1],
            ('batch_finished', (batch_id, stats))
        )

        fs1 = filestore.FileStore(self.dst1.dir)
        fs2 = filestore.FileStore(self.dst2.dir)
        self.assertEqual(set(st.id for st in fs1), ids)
        self.assertEqual(set(st.id for st in fs2), ids)

        # Check all the dmedia/file docs:
        for (file, ch) in result:
            if ch is None:
                continue
            doc = self.db.get(ch.id)
            schema.check_file(doc)
            self.assertTrue(doc['_rev'].startswith('2-'))
            self.assertNotEqual(doc['import_id'], import_id)
            self.assertEqual(doc['mtime'], file.mtime)
            self.assertEqual(doc['bytes'], file.size)
            (content_type, leaf_hashes) = self.db.get_att(ch.id, 'leaf_hashes')
            self.assertEqual(content_type, 'application/octet-stream')
            self.assertEqual(leaf_hashes, ch.leaf_hashes)
            self.assertEqual(
                set(doc['stored']),
                set([self.store1_id, self.store2_id])
            )
            self.assertEqual(
                doc['stored'][self.store1_id],
                {
                    'mtime': fs1.stat(ch.id).mtime,
                    'copies': 1,
                }
            )
            self.assertEqual(
                doc['stored'][self.store2_id],
                {
                    'mtime': fs2.stat(ch.id).mtime,
                    'copies': 2,
                }
            )

        # Verify all the files
        for (file, ch) in result:
            if ch is None:
                continue
            self.assertEqual(fs1.verify(ch.id), ch)
            self.assertEqual(fs2.verify(ch.id), ch)


