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
        
        
        
        
        
        
