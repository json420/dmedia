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
Unit tests for `dmedia.metastore`.
"""

from unittest import TestCase
import time
import os
from random import SystemRandom

from filestore import FileStore, DIGEST_BYTES
import microfiber
from microfiber import random_id

from dmedia.tests.base import TempDir
from dmedia.tests.couch import CouchCase
from dmedia import util, schema, metastore


random = SystemRandom()


def make_stored(_id, *filestores):
    return dict(
        (
            fs.id,
            {
                'copies': fs.copies,
                'mtime': fs.stat(_id).mtime,
            }
        )
        for fs in filestores
    )


class DummyStat:
    def __init__(self, mtime):
        self.mtime = mtime


class DummyFileStore:
    def __init__(self):
        self.id = random_id()
        self.copies = 1
        self._mtime = 1234567890

    def stat(self, _id):
        self._file_id = _id
        self._mtime += 1
        return DummyStat(self._mtime)


class TestFunctions(TestCase):
    def test_get_dict(self):
        doc = {}
        ret = metastore.get_dict(doc, 'foo')
        self.assertEqual(ret, {})
        self.assertEqual(doc, {'foo': {}})
        self.assertIs(doc['foo'], ret)

        doc = {'foo': None}
        ret = metastore.get_dict(doc, 'foo')
        self.assertEqual(ret, {})
        self.assertEqual(doc, {'foo': {}})
        self.assertIs(doc['foo'], ret)

        doc = {'foo': ['hello', 'naughty', 'nurse']}	
        ret = metastore.get_dict(doc, 'foo')
        self.assertEqual(ret, {})
        self.assertEqual(doc, {'foo': {}})
        self.assertIs(doc['foo'], ret)

        doc = {'foo': {'bar': 0, 'baz': 1}}
        ret = metastore.get_dict(doc, 'foo')
        self.assertEqual(ret, {'bar': 0, 'baz': 1})
        self.assertEqual(doc, {'foo': {'bar': 0, 'baz': 1}})
        self.assertIs(doc['foo'], ret)

    def test_update(self):
        new = {'foo': 2, 'bar': 2}

        stored = {}
        metastore.update(stored, 'one', new)
        self.assertEqual(stored,
            {'one': {'foo': 2, 'bar': 2}}
        )

        stored = {'one': {'foo': 1, 'bar': 1}}
        metastore.update(stored, 'one', new)
        self.assertEqual(stored,
            {'one': {'foo': 2, 'bar': 2}}
        )

        stored = {'one': {'foo': 1, 'bar': 1, 'baz': 1}}
        metastore.update(stored, 'one', new)
        self.assertEqual(stored,
            {'one': {'foo': 2, 'bar': 2, 'baz': 1}}
        )
        
    def test_add_to_stores(self):
        fs1 = DummyFileStore()
        fs2 = DummyFileStore()
        _id = random_id(30)

        doc = {'_id': _id}
        metastore.add_to_stores(doc, fs1)
        self.assertIs(fs1._file_id, _id)
        self.assertEqual(doc, 
            {
                '_id': _id,
                'stored': {
                    fs1.id: {
                        'copies': 1,
                        'mtime': 1234567891,
                        'verified': 0,
                    },
                },
            }
        )

        doc = {'_id': _id}
        metastore.add_to_stores(doc, fs1, fs2)
        self.assertIs(fs2._file_id, _id)
        self.assertEqual(doc, 
            {
                '_id': _id,
                'stored': {
                    fs1.id: {
                        'copies': 1,
                        'mtime': 1234567892,
                        'verified': 0,
                    },
                    fs2.id: {
                        'copies': 1,
                        'mtime': 1234567891,
                        'verified': 0,
                    },
                },
            }
        )

        doc = {'_id': _id, 'stored': {fs1.id: {'pin': True}}} 
        metastore.add_to_stores(doc, fs1, fs2)
        self.assertIs(fs2._file_id, _id)
        self.assertEqual(doc, 
            {
                '_id': _id,
                'stored': {
                    fs1.id: {
                        'copies': 1,
                        'mtime': 1234567893,
                        'verified': 0,
                        'pin': True,
                    },
                    fs2.id: {
                        'copies': 1,
                        'mtime': 1234567892,
                        'verified': 0,
                    },
                },
            }
        )

    def test_remove_from_stores(self):
        fs1 = DummyFileStore()
        fs2 = DummyFileStore()

        doc = {}
        metastore.remove_from_stores(doc, fs1, fs2)
        self.assertEqual(doc, {'stored': {}})

        doc = {'stored': {}}
        metastore.remove_from_stores(doc, fs1, fs2)
        self.assertEqual(doc, {'stored': {}})

        doc = {'stored': {fs1.id: 'foo', fs2.id: 'bar'}}
        metastore.remove_from_stores(doc, fs1)
        self.assertEqual(doc, {'stored': {fs2.id: 'bar'}})

        doc = {'stored': {fs1.id: 'foo', fs2.id: 'bar'}}
        metastore.remove_from_stores(doc, fs1, fs2)
        self.assertEqual(doc, {'stored': {}})

    def test_mark_verified(self):
        fs = DummyFileStore()
        ts = time.time()
        _id = random_id(30)

        doc = {'_id': _id}
        metastore.mark_verified(doc, fs, ts)
        self.assertEqual(doc,
            {
                '_id': _id,
                'stored': {
                    fs.id: {
                        'copies': 1,
                        'mtime': 1234567891,
                        'verified': ts,      
                    },
                },
            }
        )
        self.assertIs(fs._file_id, _id)

        fs_id2 = random_id()
        doc = {
            '_id': _id, 
            'stored': {
                fs.id: {
                    'copies': 2,
                    'mtime': 1234567890,
                    'verified': 4,
                    'pin': True,
                },
                fs_id2: 'foo',
            },
        }
        metastore.mark_verified(doc, fs, ts)
        self.assertEqual(doc,
            {
                '_id': _id,
                'stored': {
                    fs.id: {
                        'copies': 1,
                        'mtime': 1234567892,
                        'verified': ts,    
                        'pin': True,  
                    },
                    fs_id2: 'foo',
                },
            }
        )

    def test_mark_corrupt(self):
        fs = DummyFileStore()
        ts = time.time()

        doc = {}
        metastore.mark_corrupt(doc, fs, ts)
        self.assertEqual(doc, 
            {
                'stored': {},
                'corrupt': {fs.id: {'time': ts}},
            }
        )

        id2 = random_id()
        id3 = random_id()
        doc = {
            'stored': {fs.id: 'foo', id2: 'bar'},
            'corrupt': {id3: 'baz'},
        }
        metastore.mark_corrupt(doc, fs, ts)
        self.assertEqual(doc, 
            {
                'stored': {id2: 'bar'},
                'corrupt': {id3: 'baz', fs.id: {'time': ts}},
            }
        )

    def test_relink_iter(self):
        tmp = TempDir()
        fs = FileStore(tmp.dir)

        def create():
            _id = random_id(DIGEST_BYTES)
            data = b'N' * random.randint(1, 1776)
            open(fs.path(_id), 'wb').write(data)
            st = fs.stat(_id)
            assert st.size == len(data)
            return st

        # Test when empty
        self.assertEqual(
            list(metastore.relink_iter(fs)),
            []
        )

        # Test with only 1
        items = [create()]
        self.assertEqual(
            list(metastore.relink_iter(fs)),
            [items]
        )

        # Test with 25
        items.extend(create() for i in range(24))
        assert len(items) == 25
        items.sort(key=lambda st: st.id)
        self.assertEqual(
            list(metastore.relink_iter(fs)),
            [items]
        )

        # Test with 26
        items.append(create())
        assert len(items) == 26
        items.sort(key=lambda st: st.id)
        self.assertEqual(
            list(metastore.relink_iter(fs)),
            [
                items[:25],
                items[25:],
            ]
        )

        # Test with 49
        items.extend(create() for i in range(23))
        assert len(items) == 49
        items.sort(key=lambda st: st.id)
        self.assertEqual(
            list(metastore.relink_iter(fs)),
            [
                items[:25],
                items[25:],
            ]
        )

        # Test with 100
        items.extend(create() for i in range(51))
        assert len(items) == 100
        items.sort(key=lambda st: st.id)
        self.assertEqual(
            list(metastore.relink_iter(fs)),
            [
                items[0:25],
                items[25:50],
                items[50:75],
                items[75:100],
            ]
        )

        # Test with 118
        items.extend(create() for i in range(18))
        assert len(items) == 118
        items.sort(key=lambda st: st.id)
        self.assertEqual(
            list(metastore.relink_iter(fs)),
            [
                items[0:25],
                items[25:50],
                items[50:75],
                items[75:100],
                items[100:118],
            ]
        )


class TestMetaStore(CouchCase):
    def test_init(self):
        db = util.get_db(self.env, True)
        ms = metastore.MetaStore(db)
        self.assertIs(ms.db, db)

    def test_remove(self):
        db = util.get_db(self.env, True)
        ms = metastore.MetaStore(db)
        tmp1 = TempDir()
        fs1 = util.init_filestore(tmp1.dir)[0]
        tmp2 = TempDir()
        fs2 = util.init_filestore(tmp2.dir)[0]

        (file, ch) = tmp1.random_file()
        self.assertEqual(fs1.import_file(open(file.name, 'rb')), ch)
        self.assertEqual(fs2.import_file(open(file.name, 'rb')), ch)

        # Test when file doc isn't in dmedia-0
        with self.assertRaises(microfiber.NotFound) as cm:
            ms.remove(fs1, ch.id)
        with self.assertRaises(microfiber.NotFound) as cm:
            ms.remove(fs2, ch.id)
        fs1.verify(ch.id)
        fs2.verify(ch.id)

        # Test when doc and file are present
        stored = make_stored(ch.id, fs1, fs2)
        doc = schema.create_file(time.time(), ch, stored)
        db.save(doc)
        doc = ms.remove(fs1, ch.id)
        self.assertTrue(doc['_rev'].startswith('2-'))
        self.assertEqual(doc, db.get(ch.id))
        self.assertEqual(doc['stored'],
            {
                fs2.id: {
                    'mtime': fs2.stat(ch.id).mtime,
                    'copies': 1,
                },   
            }
        )

        # Test when file isn't present
        doc['stored'] = stored
        db.save(doc)
        with self.assertRaises(OSError) as cm:
            ms.remove(fs1, ch.id)
        doc = db.get(ch.id)
        self.assertTrue(doc['_rev'].startswith('4-'))
        self.assertEqual(doc['stored'],
            {
                fs2.id: {
                    'mtime': fs2.stat(ch.id).mtime,
                    'copies': 1,
                },   
            }
        )

