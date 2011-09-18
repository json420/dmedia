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

from dmedia import importer2


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

    def test_random_batch(self):
        key = self.src.dir
        args = (self.src.dir,)
        inst = importer2.ImportWorker(self.env, self.q, key, args)
        self.assertEqual(inst.srcdir, self.src.dir)

        # start()
        self.assertIsNone(inst.id)
        self.assertIsNone(inst.doc)
        inst.start()
        doc = self.db.get(inst.id)
        self.assertEqual(doc['srcdir'], self.src.dir)
        self.assertEqual(doc['machine_id'], self.machine_id)
        self.assertEqual(doc['batch_id'], self.batch_id)
        self.assertEqual(doc['log'],
            {
                'all': [],
                'duplicate': [],
                'empty': [],
                'new': [],
            }
        )
        self.assertEqual(doc['stats'],
            {
                'all': {'count': 0, 'bytes': 0},
                'duplicate': {'count': 0, 'bytes': 0},
                'empty': {'count': 0, 'bytes': 0},
                'new': {'count': 0, 'bytes': 0},
            }
        )

        # scan()
        (batch, result) = self.src.random_batch(25)
        log_all = [
            {'src': file.name, 'bytes': file.size, 'mtime': file.mtime}
            for file in batch.files
        ]
        stats_all = {'bytes': batch.size, 'count': batch.count}
        inst.scan()
        doc = self.db.get(inst.id)
        self.assertEqual(doc['log'],
            {
                'all': log_all,
                'duplicate': [],
                'empty': [],
                'new': [],
            }
        )
        self.assertEqual(doc['stats'],
            {
                'all': stats_all,
                'duplicate': {'count': 0, 'bytes': 0},
                'empty': {'count': 0, 'bytes': 0},
                'new': {'count': 0, 'bytes': 0},
            }
        )

        # get_filestores()
        stores = inst.get_filestores()
        self.assertEqual(len(stores), 1)
        fs = stores[0]
        self.assertIsInstance(fs, filestore.FileStore)

        # import_all()
        log_new = [
            {'src': file.name, 'id': ch.id}
            for (file, ch) in result
        ]
        stats_new = {'bytes': batch.size, 'count': batch.count}
        inst.import_all()
        doc = self.db.get(inst.id)
        self.assertEqual(doc['log'],
            {
                'all': log_all,
                'duplicate': [],
                'empty': [],
                'new': log_new,
            }
        )
        self.assertEqual(doc['stats'],
            {
                'all': stats_all,
                'duplicate': {'count': 0, 'bytes': 0},
                'empty': {'count': 0, 'bytes': 0},
                'new': stats_new,
            }
        )
