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

import filestore
from filestore import FileStore, DIGEST_B32LEN, DIGEST_BYTES
from microfiber import random_id

from .base import TempDir
from .couch import CouchCase

from dmedia import local, schema


class TestFunctions(TestCase):
    def test_get_filestore(self):
        tmp = TempDir()
        _id = random_id()
        doc = {
            '_id': _id,
            'parentdir': tmp.dir,
            'copies': 2,
        }
        fs = local.get_filestore(doc)
        self.assertIsInstance(fs, FileStore)
        self.assertEqual(fs.parentdir, tmp.dir)
        self.assertEqual(fs.id, _id)
        self.assertEqual(fs.copies, 2)

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
        tmp1 = TempDir()
        tmp2 = TempDir()
        id1 = random_id()
        id2 = random_id()
        fs1 = FileStore(tmp1.dir, id1)
        fs2 = FileStore(tmp2.dir, id2)
        inst = local.LocalStores()

        # Test when fs.id is already present
        inst.ids[id1] = None
        with self.assertRaises(Exception) as cm:
            inst.add(fs1)
        self.assertEqual(
            str(cm.exception),
            'already have ID {!r}'.format(id1)
        )
        inst.ids.clear()

        # Test when fs.parentdir is already present
        inst.parentdirs[tmp1.dir] = None
        with self.assertRaises(Exception) as cm:
            inst.add(fs1)
        self.assertEqual(
            str(cm.exception),
            'already have parentdir {!r}'.format(tmp1.dir)
        )
        inst.parentdirs.clear()

        # Add when fast=True
        self.assertIsNone(inst.add(fs1))
        self.assertEqual(set(inst.ids), set([id1]))
        self.assertIs(inst.ids[id1], fs1)
        self.assertEqual(set(inst.parentdirs), set([tmp1.dir]))
        self.assertIs(inst.parentdirs[tmp1.dir], fs1)
        self.assertEqual(inst.fast, set([id1]))
        self.assertEqual(inst.slow, set())

        # Add when fast=False
        self.assertIsNone(inst.add(fs2, fast=False))
        self.assertEqual(set(inst.ids), set([id1, id2]))
        self.assertIs(inst.ids[id2], fs2)
        self.assertEqual(set(inst.parentdirs), set([tmp1.dir, tmp2.dir]))
        self.assertIs(inst.parentdirs[tmp2.dir], fs2)
        self.assertEqual(inst.fast, set([id1]))
        self.assertEqual(inst.slow, set([id2]))

    def test_remove(self):
        tmp1 = TempDir()
        tmp2 = TempDir()
        id1 = random_id()
        id2 = random_id()
        fs1 = FileStore(tmp1.dir, id1)
        fs2 = FileStore(tmp2.dir, id2)
        inst = local.LocalStores()

        # Test when fs.id not present
        with self.assertRaises(KeyError) as cm:
            inst.remove(fs1)
        self.assertEqual(str(cm.exception), repr(id1))

        # Test when fs.parentdir not present
        inst.ids[id1] = None
        with self.assertRaises(KeyError) as cm:
            inst.remove(fs1)
        self.assertEqual(str(cm.exception), repr(tmp1.dir))
        inst.ids.clear()

        # Test when it's all good
        inst.ids[id1] = fs1
        inst.parentdirs[tmp1.dir] = fs1
        inst.slow.add(id1)

        inst.ids[id2] = fs2
        inst.parentdirs[tmp2.dir] = fs2
        inst.fast.add(id2)

        self.assertIsNone(inst.remove(fs1))
        self.assertEqual(inst.ids, {id2: fs2})
        self.assertEqual(inst.parentdirs, {tmp2.dir: fs2})
        self.assertEqual(inst.fast, set([id2]))
        self.assertEqual(inst.slow, set())

        self.assertIsNone(inst.remove(fs2))
        self.assertEqual(inst.ids, {})
        self.assertEqual(inst.parentdirs, {})
        self.assertEqual(inst.fast, set())
        self.assertEqual(inst.slow, set())


class TestLocalBase(CouchCase):
    create_databases = ['dmedia', 'dmedia_log']

    def test_get_doc(self):
        inst = local.LocalBase(self.env)

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
        inst = local.LocalBase(self.env)

        # When doc doesn't exist
        with self.assertRaises(local.NoSuchFile) as cm:
            doc = inst.content_hash(ch.id)
        self.assertEqual(cm.exception.id, ch.id)

        # When doc does exist
        doc = schema.create_file(ch.id, ch.file_size, ch.leaf_hashes, {})
        inst.db.save(doc)
        self.assertEqual(inst.content_hash(ch.id), unpacked)
        self.assertEqual(inst.content_hash(ch.id, False), ch)

        # Test when root hash is wrong:
        doc['bytes'] += 1
        inst.db.save(doc)
        with self.assertRaises(filestore.RootHashError) as cm:
            inst.content_hash(ch.id)
        self.assertEqual(cm.exception.id, ch.id)

    def test_local_path(self):
        src = TempDir()
        (file, ch) = src.random_file()
        dst1 = TempDir()
        dst2 = TempDir()

        fs1 = FileStore(dst1.dir)
        fs2 = FileStore(dst2.dir)
        assert fs1.import_file(open(file.name, 'rb')) == ch
        assert fs2.import_file(open(file.name, 'rb')) == ch

        store1 = random_id()
        store2 = random_id()
        store3 = random_id()

        inst = local.LocalBase(self.env)


