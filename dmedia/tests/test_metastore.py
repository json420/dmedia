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

from microfiber import random_id

from dmedia import metastore


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
 
