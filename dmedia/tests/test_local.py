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

from filestore import FileStore, DIGEST_B32LEN
from microfiber import random_id

from .base import TempDir
from .couch import CouchCase

from dmedia import local


class TestFunctions(TestCase):
    def test_get_store_id(self):
        internal = tuple(random_id() for i in range(3))
        remote = tuple(random_id() for i in range(4))

        # Empty stores and stored
        doc = {'_id': random_id(), 'stored': {}}
        with self.assertRaises(local.FileNotLocal) as cm:
            local.get_store_id([], doc)
        self.assertEqual(cm.exception.id, doc['_id'])

        # disjoint set
        doc = {'_id': random_id(), 'stored': remote}
        with self.assertRaises(local.FileNotLocal) as cm:
            local.get_store_id(internal, doc)
        self.assertEqual(cm.exception.id, doc['_id'])

        doc = {'_id': random_id(), 'stored': internal + remote}
        _id = local.get_store_id(internal, doc)
        self.assertIn(_id, internal)
        self.assertEqual(_id, Random(doc['_id']).choice(sorted(internal)))
        self.assertEqual(local.get_store_id(internal, doc), _id)
        
        doc = {'_id': random_id(), 'stored': (internal[0], remote[0])}
        self.assertEqual(local.get_store_id(internal, doc), internal[0])       
        self.assertEqual(local.get_store_id(internal, doc), internal[0])


class TestStores(CouchCase):
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

        inst = local.Stores(self.env)
        
        
