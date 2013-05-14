# dmedia: dmedia hashing protocol and file layout
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
Unit tests for `dmedia.local`.
"""

from unittest import TestCase
from random import Random
import time

import filestore
from filestore import DIGEST_BYTES
from filestore.misc import TempFileStore
from dbase32 import random_id

from .base import TempDir
from .couch import CouchCase

from dmedia import local, schema, util


class TestFunctions(TestCase):
    def test_choose_local_store(self):
        fast = tuple(random_id() for i in range(3))
        slow = tuple(random_id() for i in range(3))
        _fast = set(fast)
        _slow = set(slow)
        remote = tuple(random_id() for i in range(4))

        # Empty stores and stored
        doc = {'_id': random_id(), 'stored': {}}
        with self.assertRaises(local.FileNotLocal) as cm:
            local.choose_local_store(doc, set(), set())
        self.assertEqual(cm.exception.id, doc['_id'])

        # disjoint set
        doc = {'_id': random_id(), 'stored': remote}
        with self.assertRaises(local.FileNotLocal) as cm:
            local.choose_local_store(doc, _fast, set())
        self.assertEqual(cm.exception.id, doc['_id'])

        doc = {'_id': random_id(), 'stored': fast + remote}
        _id = local.choose_local_store(doc, set(fast), set())
        self.assertIn(_id, fast)
        self.assertEqual(_id, Random(doc['_id']).choice(sorted(fast)))
        self.assertEqual(local.choose_local_store(doc, _fast, set()), _id)

        doc = {'_id': random_id(), 'stored': (fast[0], remote[0])}
        self.assertEqual(local.choose_local_store(doc, _fast, set()), fast[0])
        self.assertEqual(local.choose_local_store(doc, _fast, set()), fast[0])

        # Test that fast are use preferentially
        for i in range(100):
            doc = doc = {'_id': random_id(), 'stored': fast + slow}
            _id = local.choose_local_store(doc, _fast, _slow)
            self.assertIn(_id, fast)
            self.assertEqual(_id,
                Random(doc['_id']).choice(sorted(fast))
            )
            self.assertEqual(local.choose_local_store(doc, _fast, _slow), _id)

        # Test when only available on slow
        for i in range(100):
            doc = doc = {'_id': random_id(), 'stored': remote + slow}
            _id = local.choose_local_store(doc, _fast, _slow)
            self.assertIn(_id, slow)
            self.assertEqual(_id,
                Random(doc['_id']).choice(sorted(slow))
            )
            self.assertEqual(local.choose_local_store(doc, _fast, _slow), _id)


class TestLocalStores(TestCase):
    def test_init(self):
        inst = local.LocalStores()
        self.assertEqual(inst.ids, {})
        self.assertEqual(inst.parentdirs, {})
        self.assertEqual(inst.fast, set())
        self.assertEqual(inst.slow, set())

    def test_add(self):
        fs1 = TempFileStore()
        fs2 = TempFileStore()
        inst = local.LocalStores()

        # Test when fs.id is already present
        inst.ids[fs1.id] = None
        with self.assertRaises(Exception) as cm:
            inst.add(fs1)
        self.assertEqual(
            str(cm.exception),
            'already have ID {!r}'.format(fs1.id)
        )
        inst.ids.clear()

        # Test when fs.parentdir is already present
        inst.parentdirs[fs1.parentdir] = None
        with self.assertRaises(Exception) as cm:
            inst.add(fs1)
        self.assertEqual(
            str(cm.exception),
            'already have parentdir {!r}'.format(fs1.parentdir)
        )
        inst.parentdirs.clear()

        # Add when fast=True
        self.assertIsNone(inst.add(fs1))
        self.assertEqual(set(inst.ids), set([fs1.id]))
        self.assertIs(inst.ids[fs1.id], fs1)
        self.assertEqual(set(inst.parentdirs), set([fs1.parentdir]))
        self.assertIs(inst.parentdirs[fs1.parentdir], fs1)
        self.assertEqual(inst.fast, set([fs1.id]))
        self.assertEqual(inst.slow, set())

        # Add when fast=False
        self.assertIsNone(inst.add(fs2, fast=False))
        self.assertEqual(set(inst.ids), set([fs1.id, fs2.id]))
        self.assertIs(inst.ids[fs2.id], fs2)
        self.assertEqual(set(inst.parentdirs),
            set([fs1.parentdir, fs2.parentdir])
        )
        self.assertIs(inst.parentdirs[fs2.parentdir], fs2)
        self.assertEqual(inst.fast, set([fs1.id]))
        self.assertEqual(inst.slow, set([fs2.id]))

    def test_remove(self):
        fs1 = TempFileStore()
        fs2 = TempFileStore()
        inst = local.LocalStores()

        # Test when fs.id not present
        with self.assertRaises(KeyError) as cm:
            inst.remove(fs1)
        self.assertEqual(str(cm.exception), repr(fs1.id))

        # Test when fs.parentdir not present
        inst.ids[fs1.id] = None
        with self.assertRaises(KeyError) as cm:
            inst.remove(fs1)
        self.assertEqual(str(cm.exception), repr(fs1.parentdir))
        inst.ids.clear()

        # Test when it's all good
        inst.ids[fs1.id] = fs1
        inst.parentdirs[fs1.parentdir] = fs1
        inst.slow.add(fs1.id)

        inst.ids[fs2.id] = fs2
        inst.parentdirs[fs2.parentdir] = fs2
        inst.fast.add(fs2.id)

        self.assertIsNone(inst.remove(fs1))
        self.assertEqual(inst.ids, {fs2.id: fs2})
        self.assertEqual(inst.parentdirs, {fs2.parentdir: fs2})
        self.assertEqual(inst.fast, set([fs2.id]))
        self.assertEqual(inst.slow, set())

        self.assertIsNone(inst.remove(fs2))
        self.assertEqual(inst.ids, {})
        self.assertEqual(inst.parentdirs, {})
        self.assertEqual(inst.fast, set())
        self.assertEqual(inst.slow, set())

    def test_local_stores(self):
        fs1 = TempFileStore(copies=1)
        fs2 = TempFileStore(copies=0)
        inst = local.LocalStores()

        self.assertEqual(inst.local_stores(), {})

        inst.add(fs1)
        self.assertEqual(inst.local_stores(),
            {
                fs1.parentdir: {'id': fs1.id, 'copies': 1},
            }
        )
        
        inst.add(fs2)
        self.assertEqual(inst.local_stores(),
            {
                fs1.parentdir: {'id': fs1.id, 'copies': 1},
                fs2.parentdir: {'id': fs2.id, 'copies': 0},  
            }
        )
        


class TestLocalSlave(CouchCase):
    def setUp(self):
        super().setUp()
        util.get_db(self.env, True)

    def test_get_doc(self):
        inst = local.LocalSlave(self.env)

        # When doc doesn't exist
        _id = random_id(DIGEST_BYTES)
        with self.assertRaises(local.NoSuchFile) as cm:
            doc = inst.get_doc(_id)
        self.assertEqual(cm.exception.id, _id)

        # When doc does exist
        doc = {'_id': _id}
        inst.db.save(doc)
        self.assertEqual(inst.get_doc(_id), doc)

    def test_content_hash(self):
        tmp = TempDir()
        (file, ch) = tmp.random_file()
        unpacked = filestore.check_root_hash(
            ch.id, ch.file_size, ch.leaf_hashes, unpack=True
        )
        inst = local.LocalSlave(self.env)

        # When doc doesn't exist
        with self.assertRaises(local.NoSuchFile) as cm:
            doc = inst.content_hash(ch.id)
        self.assertEqual(cm.exception.id, ch.id)

        # When doc does exist
        doc = schema.create_file(time.time(), ch, {})
        inst.db.save(doc)
        self.assertEqual(inst.content_hash(ch.id), unpacked)
        self.assertEqual(inst.content_hash(ch.id, False), ch)

        # Test when root hash is wrong:
        doc['bytes'] += 1
        inst.db.save(doc)
        with self.assertRaises(filestore.RootHashError) as cm:
            inst.content_hash(ch.id)
        self.assertEqual(cm.exception.id, ch.id)

