# dmedia: distributed media library
# Copyright (C) 2011 Novacut Inc
#
# This file is part of `dmedia`.
#
# `dmedia` is free software: you can redistribute it and/or modify it under
# the terms of the GNU Affero General Public License as published by the Free
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
Unit tests for `dmedia.verification` module.
"""

from unittest import TestCase
import time
from os import path

from filestore import FileStore, DIGEST_BYTES
from microfiber import Database, random_id

from .couch import CouchCase
from .base import TempDir, random_file

from dmedia.views import init_views
from dmedia.schema import create_file
from dmedia import verification


class TestFunctions(CouchCase):

    def setUp(self):
        super().setUp()
        self.tmp = TempDir()
        self.store_id = random_id()
        local = {
            '_id': '_local/dmedia',
            'stores': {
                self.tmp.dir: {'id': self.store_id, 'copies': 1},
            },
        }
        self.db = Database('dmedia', self.env)
        self.db.ensure()
        self.db.save(local)
        init_views(self.db)

    def tearDown(self):
        super().tearDown()
        self.tmp = None

    def test_verify(self):
        fs = FileStore(self.tmp.dir, self.store_id, 1)

        # 10 good files:
        good = []
        for i in range(10):
            (file, ch) = random_file(fs.tmp)
            fs.move_to_canonical(open(file.name, 'rb'), ch.id)
            stored = {
                fs.id: {'mtime': fs.stat(ch.id).mtime, 'copies': fs.copies}
            }
            doc = create_file(ch.id, ch.file_size, ch.leaf_hashes, stored)
            self.db.save(doc)
            good.append(ch.id)

        # 10 corrupt files:
        bad = []
        for i in range(10):
            (file, ch) = random_file(fs.tmp)
            _id = random_id(DIGEST_BYTES)
            fs.move_to_canonical(open(file.name, 'rb'), _id)
            stored = {
                fs.id: {'mtime': fs.stat(_id).mtime, 'copies': fs.copies}
            }
            doc = create_file(_id, ch.file_size, ch.leaf_hashes, stored)
            self.db.save(doc)
            bad.append(_id)

        # 5 empty files:
        empty = []
        for i in range(5):
            (file, ch) = random_file(fs.tmp)
            open(file.name, 'wb').close()
            fs.move_to_canonical(open(file.name, 'rb'), ch.id)
            stored = {
                fs.id: {'mtime': path.getmtime(fs.path(ch.id)), 'copies': fs.copies}
            }
            doc = create_file(ch.id, ch.file_size, ch.leaf_hashes, stored)
            self.db.save(doc)
            empty.append(ch.id)
 
        # 5 missing files:
        missing = []
        for i in range(5):
            (file, ch) = random_file(fs.tmp)
            stored = {
                fs.id: {'mtime': path.getmtime(file.name), 'copies': fs.copies}
            }
            doc = create_file(ch.id, ch.file_size, ch.leaf_hashes, stored)
            self.db.save(doc)
            missing.append(ch.id)

        # Now run the verification:
        start = time.time()
        verification.verify(self.env, self.tmp.dir)
        end = time.time()

        # Check the results:
        for _id in good:
            doc = self.db.get(_id)
            self.assertEqual(set(doc['stored']), set([fs.id]))
            self.assertEqual(
                set(doc['stored'][fs.id]),
                set(['copies', 'mtime', 'verified'])
            )
            verified = doc['stored'][fs.id]['verified']
            self.assertIsInstance(verified, (int, float))
            self.assertLessEqual(start, verified)
            self.assertLessEqual(verified, end)
            self.assertEqual(doc['corrupt'], {})
        for _id in bad:
            doc = self.db.get(_id)
            self.assertEqual(doc['stored'], {})
            self.assertEqual(set(doc['corrupt']), set([fs.id]))
            self.assertEqual(set(doc['corrupt'][fs.id]), set(['time']))
            _time = doc['corrupt'][fs.id]['time']
            self.assertIsInstance(_time, (int, float))
            self.assertLessEqual(start, _time)
            self.assertLessEqual(_time, end)
        for _id in empty:
            doc = self.db.get(_id)
            self.assertEqual(doc['stored'], {})
            self.assertEqual(doc['corrupt'], {})
        for _id in missing:
            doc = self.db.get(_id)
            self.assertEqual(doc['stored'], {})
            self.assertEqual(doc['corrupt'], {})
        
            


