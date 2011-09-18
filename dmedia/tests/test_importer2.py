# dmedia: distributed media library
# Copyright (C) 2011 Novacut Inc
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
#
# Authors:
#   Jason Gerard DeRose <jderose@novacut.com>

"""
Unit tests for `dmedia.importer2` module.
"""

from multiprocessing import current_process

import filestore
from microfiber import random_id, Database

from .couch import CouchCase
from .base import TempDir

from dmedia import importer2, schema


class DummyQueue(object):
    def __init__(self):
        self.items = []

    def put(self, item):
        self.items.append(item)


class TestImportWorker(CouchCase):
    def setUp(self):
        super().setUp()
        self.batch_id = random_id()
        self.env['batch_id'] = self.batch_id
        self.q = DummyQueue()
        self.pid = current_process().pid
        self.src = TempDir()

        self.dst = TempDir()
        self.store_id = random_id()
        self.env['filestore'] = {
            '_id': self.store_id,
            'parentdir': self.dst.dir,
        }

        self.db = Database('dmedia', self.env)

    def tearDown(self):
        super().tearDown()
        self.q = None
        self.src = None
        self.dst = None

    def test_random_batch(self):
        key = self.src.dir
        args = (self.src.dir,)
        inst = importer2.ImportWorker(self.env, self.q, key, args)
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
            (self.src.dir, {'bytes': batch.size, 'count': batch.count})
        )

        # get_filestores()
        stores = inst.get_filestores()
        self.assertEqual(len(stores), 1)
        fs = stores[0]
        self.assertIsInstance(fs, filestore.FileStore)

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

