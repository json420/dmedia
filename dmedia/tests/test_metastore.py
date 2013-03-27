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
from os import path
import io
from random import SystemRandom
from copy import deepcopy

from filestore import FileStore, DIGEST_BYTES
from filestore.misc import TempFileStore
import microfiber
from microfiber import random_id, dumps, Conflict

from dmedia.tests.base import TempDir, write_random, random_file_id
from dmedia.tests.couch import CouchCase
from dmedia import util, schema, metastore
from dmedia.metastore import create_stored, get_mtime
from dmedia.constants import TYPE_ERROR


random = SystemRandom()


def create_random_file(fs, db):
    tmp_fp = fs.allocate_tmp()
    ch = write_random(tmp_fp)
    tmp_fp = open(tmp_fp.name, 'rb')
    fs.move_to_canonical(tmp_fp, ch.id)
    stored = create_stored(ch.id, fs)
    doc = schema.create_file(time.time(), ch, stored)
    db.save(doc)
    return db.get(ch.id)


class DummyStat:
    def __init__(self, mtime):
        self.mtime = mtime


class DummyFileStore:
    def __init__(self):
        self.id = random_id()
        self.copies = 1
        self._mtime = time.time() - random.randint(0, 10000)
        self._calls = 0

    def stat(self, _id):
        self._file_id = _id
        return DummyStat(self._mtime)


class DummyConflict(Conflict):
    def __init__(self):
        pass


class DummyDatabase:
    def __init__(self, doc, newrev):
        self._calls = []
        self._id = doc['_id']
        self._rev = doc['_rev']
        self._doc = deepcopy(doc)
        self._newrev = newrev

    def save(self, doc):
        self._calls.append(('save', deepcopy(doc)))
        if doc['_rev'] != self._rev:
            raise DummyConflict()
        doc['_rev'] = self._newrev
        return doc

    def get(self, _id):
        assert _id == self._id
        self._calls.append(('get', _id))
        return deepcopy(self._doc)

    def _func(self, doc, key, value):
        self._calls.append(('func', deepcopy(doc), key, value))
        doc[key] = value


class TestFunctions(TestCase):
    def test_get_dict(self):
        # Bad `d` type:
        bad = [random_id(), random_id()]
        with self.assertRaises(TypeError) as cm:
            metastore.get_dict(bad, random_id())
        self.assertEqual(
            str(cm.exception),
            TYPE_ERROR.format('d', dict, list, bad)
        )

        # Bad `key` type:
        bad = random.randint(0, 1000)
        with self.assertRaises(TypeError) as cm:
            metastore.get_dict({}, bad)
        self.assertEqual(
            str(cm.exception),
            TYPE_ERROR.format('key', str, int, bad)
        )

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

    def test_update_doc(self):
        _id = random_id()
        rev1 = random_id()
        rev2 = random_id()
        rev3 = random_id()
        key = random_id()
        value = random_id()
        doc1 = {
            '_id': _id,
            '_rev': rev1,
            'hello': 'world',
        }
        doc1a = {
            '_id': _id,
            '_rev': rev1,
            'hello': 'world',
            key: value,
        }
        doc2 = {
            '_id': _id,
            '_rev': rev2,
            'stuff': 'junk',
        }
        doc2a = {
            '_id': _id,
            '_rev': rev2,
            'stuff': 'junk',
            key: value,
        }

        # Test when there is a mid-flight collision:
        db = DummyDatabase(deepcopy(doc2), rev3)
        self.assertEqual(
            metastore.update_doc(db, deepcopy(doc1), db._func, key, value),
            {
                '_id': _id,
                '_rev': rev3,
                'stuff': 'junk',
                key: value,
            }
        )
        self.assertEqual(db._calls, [
            ('func', doc1, key, value),
            ('save', doc1a),
            ('get', _id),
            ('func', doc2, key, value),
            ('save', doc2a),
        ])

        # Test when there is no conflict:
        db = DummyDatabase(deepcopy(doc1), rev3)
        self.assertEqual(
            metastore.update_doc(db, deepcopy(doc1), db._func, key, value),
            {
                '_id': _id,
                '_rev': rev3,
                'hello': 'world',
                key: value,
            }
        )
        self.assertEqual(db._calls, [
            ('func', doc1, key, value),
            ('save', doc1a),
        ])

    def test_create_stored(self):
        tmp1 = TempDir()
        fs1 = util.init_filestore(tmp1.dir, copies=0)[0]
        tmp2 = TempDir()
        fs2 = util.init_filestore(tmp2.dir, copies=2)[0]
        (file, ch) = tmp1.random_file()
        self.assertEqual(fs1.import_file(open(file.name, 'rb')), ch)
        self.assertEqual(fs2.import_file(open(file.name, 'rb')), ch)

        self.assertEqual(metastore.create_stored(ch.id), {})
        self.assertEqual(metastore.create_stored(ch.id, fs1),
            {
                fs1.id: {
                    'copies': 0,
                    'mtime': get_mtime(fs1, ch.id),
                },
            }
        )
        self.assertEqual(metastore.create_stored(ch.id, fs1, fs2),
            {
                fs1.id: {
                    'copies': 0,
                    'mtime': get_mtime(fs1, ch.id),
                },
                fs2.id: {
                    'copies': 2,
                    'mtime': get_mtime(fs2, ch.id),
                }, 
            }
        )

    def test_merge_stored(self):
        id1 = random_id()
        id2 = random_id()
        id3 = random_id()
        ts1 = int(time.time())
        ts2 = ts1 - 3
        ts3 = ts1 - 5
        new = {
            id1: {
                'copies': 2,
                'mtime': ts1,
            },
            id2: {
                'copies': 1,
                'mtime': ts2,
            },
        }

        old = {}
        self.assertIsNone(metastore.merge_stored(old, deepcopy(new)))
        self.assertEqual(old, new)

        old = {
            id3: {
                'copies': 1,
                'mtime': ts3,
                'verified': ts3 + 100,
            }
        }
        self.assertIsNone(metastore.merge_stored(old, deepcopy(new)))
        self.assertEqual(old,
            {
                id1: {
                    'copies': 2,
                    'mtime': ts1,
                },
                id2: {
                    'copies': 1,
                    'mtime': ts2,
                },
                id3: {
                    'copies': 1,
                    'mtime': ts3,
                    'verified': ts3 + 100,
                }
            }
        )

        old = {
            id1: {
                'copies': 1,
                'mtime': ts1 - 100,
                'verified': ts1 - 50,  # Should be removed
            },
            id2: {
                'copies': 2,
                'mtime': ts2 - 200,
                'pinned': True,  # Should be preserved
            },
        }
        self.assertIsNone(metastore.merge_stored(old, deepcopy(new)))
        self.assertEqual(old,
            {
                id1: {
                    'copies': 2,
                    'mtime': ts1,
                },
                id2: {
                    'copies': 1,
                    'mtime': ts2,
                    'pinned': True,
                },
            }
        )

        old = {
            id1: {
                'copies': 1,
                'mtime': ts1 - 100,
                'pinned': True,  # Should be preserved
                'verified': ts1 - 50,  # Should be removed
            },
            id2: {
                'copies': 2,
                'mtime': ts2 - 200,
                'verified': ts1 - 50,  # Should be removed
                'pinned': True,  # Should be preserved
            },
            id3: {
                'copies': 1,
                'mtime': ts3,
                'verified': ts3 + 100,
                'pinned': True,
            },
        }
        self.assertIsNone(metastore.merge_stored(old, deepcopy(new)))
        self.assertEqual(old,
            {
                id1: {
                    'copies': 2,
                    'mtime': ts1,
                    'pinned': True,
                },
                id2: {
                    'copies': 1,
                    'mtime': ts2,
                    'pinned': True,
                },
                id3: {
                    'copies': 1,
                    'mtime': ts3,
                    'verified': ts3 + 100,
                    'pinned': True,
                },
            }
        )

        # Test when an existing doc['stored'][store_id] value is bad:
        old = {
            id1: 'broken1',
            id2: 'broken2',
            id3: 'broken3',
        }
        self.assertIsNone(metastore.merge_stored(old, deepcopy(new)))
        self.assertEqual(old,
            {
                id1: {
                    'copies': 2,
                    'mtime': ts1,
                },
                id2: {
                    'copies': 1,
                    'mtime': ts2,
                },
                id3: 'broken3',
            }
        )

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
                        'mtime': int(fs1._mtime),
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
                        'mtime': int(fs1._mtime),
                    },
                    fs2.id: {
                        'copies': 1,
                        'mtime': int(fs2._mtime),
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
                        'mtime': int(fs1._mtime),
                        'pin': True,
                    },
                    fs2.id: {
                        'copies': 1,
                        'mtime': int(fs2._mtime),
                    },
                },
            }
        )

    def test_mark_removed(self):
        fs1 = DummyFileStore()
        fs2 = DummyFileStore()

        doc = {}
        metastore.mark_removed(doc, fs1, fs2)
        self.assertEqual(doc, {'stored': {}})

        doc = {'stored': {}}
        metastore.mark_removed(doc, fs1, fs2)
        self.assertEqual(doc, {'stored': {}})

        doc = {'stored': {fs1.id: 'foo', fs2.id: 'bar'}}
        metastore.mark_removed(doc, fs1)
        self.assertEqual(doc, {'stored': {fs2.id: 'bar'}})

        doc = {'stored': {fs1.id: 'foo', fs2.id: 'bar'}}
        metastore.mark_removed(doc, fs1, fs2)
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
                        'mtime': int(fs._mtime),
                        'verified': int(ts),      
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
                    'mtime': int(fs._mtime),
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
                        'mtime': int(fs._mtime),
                        'verified': int(ts),    
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

    def test_mark_copied(self):
        _id = random_file_id()
        src = DummyFileStore()
        ts = time.time()
        dst1 = DummyFileStore()
        dst2 = DummyFileStore()
        other_id1 = random_id()
        other_id2 = random_id()

        # One destination, no doc['stored']:
        doc = {'_id': _id}
        self.assertIsNone(metastore.mark_copied(doc, src, ts, dst1))
        self.assertEqual(doc, 
            {
                '_id': _id,
                'stored': {
                    src.id: {
                        'copies': src.copies,
                        'mtime': int(src._mtime),
                        'verified': int(ts),
                    },
                    dst1.id: {
                        'copies': dst1.copies,
                        'mtime': int(dst1._mtime),
                    },
                }
            }
        )

        # Two destinations, no doc['stored']:
        doc = {'_id': _id}
        self.assertIsNone(metastore.mark_copied(doc, src, ts, dst1, dst2))
        self.assertEqual(doc, 
            {
                '_id': _id,
                'stored': {
                    src.id: {
                        'copies': src.copies,
                        'mtime': int(src._mtime),
                        'verified': int(ts),
                    },
                    dst1.id: {
                        'copies': dst1.copies,
                        'mtime': int(dst1._mtime),
                    },
                    dst2.id: {
                        'copies': dst2.copies,
                        'mtime': int(dst2._mtime),
                    },
                }
            }
        )

        # One destination, existing doc['stored']:
        doc = {
            '_id': _id,
            'stored': {
                src.id: {'pinned': True},
                other_id1: 'foo',
                other_id2: 'bar',
            }
        }
        self.assertIsNone(metastore.mark_copied(doc, src, ts, dst1))
        self.assertEqual(doc, 
            {
                '_id': _id,
                'stored': {
                    src.id: {
                        'copies': src.copies,
                        'mtime': int(src._mtime),
                        'verified': int(ts),
                        'pinned': True,
                    },
                    dst1.id: {
                        'copies': dst1.copies,
                        'mtime': int(dst1._mtime),
                    },
                    other_id1: 'foo',
                    other_id2: 'bar',
                }
            }
        )

        # Two destinations, existing doc['stored']:
        doc = {
            '_id': _id,
            'stored': {
                src.id: {'pinned': True},
                other_id1: 'foo',
                other_id2: 'bar',
            }
        }
        self.assertIsNone(metastore.mark_copied(doc, src, ts, dst1, dst2))
        self.assertEqual(doc, 
            {
                '_id': _id,
                'stored': {
                    src.id: {
                        'copies': src.copies,
                        'mtime': int(src._mtime),
                        'verified': int(ts),
                        'pinned': True,
                    },
                    dst1.id: {
                        'copies': dst1.copies,
                        'mtime': int(dst1._mtime),
                    },
                    dst2.id: {
                        'copies': dst2.copies,
                        'mtime': int(dst2._mtime),
                    },
                    other_id1: 'foo',
                    other_id2: 'bar',
                }
            }
        )

        # One destination, broken doc['stored'][src.id]:
        doc = {
            '_id': _id,
            'stored': {
                src.id: 'bad dog',
                other_id1: 'foo',
                other_id2: 'bar',
            }
        }
        self.assertIsNone(metastore.mark_copied(doc, src, ts, dst1))
        self.assertEqual(doc, 
            {
                '_id': _id,
                'stored': {
                    src.id: {
                        'copies': src.copies,
                        'mtime': int(src._mtime),
                        'verified': int(ts),
                    },
                    dst1.id: {
                        'copies': dst1.copies,
                        'mtime': int(dst1._mtime),
                    },
                    other_id1: 'foo',
                    other_id2: 'bar',
                }
            }
        )

        # Two destinations, broken doc['stored'][src.id]:
        doc = {
            '_id': _id,
            'stored': {
                src.id: 'still a bad dog',
                other_id1: 'foo',
                other_id2: 'bar',
            }
        }
        self.assertIsNone(metastore.mark_copied(doc, src, ts, dst1, dst2))
        self.assertEqual(doc, 
            {
                '_id': _id,
                'stored': {
                    src.id: {
                        'copies': src.copies,
                        'mtime': int(src._mtime),
                        'verified': int(ts),
                    },
                    dst1.id: {
                        'copies': dst1.copies,
                        'mtime': int(dst1._mtime),
                    },
                    dst2.id: {
                        'copies': dst2.copies,
                        'mtime': int(dst2._mtime),
                    },
                    other_id1: 'foo',
                    other_id2: 'bar',
                }
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


class TestBufferedSave(CouchCase):
    def test_init(self):
        db = microfiber.Database('foo', self.env)
        db.ensure()
        buf = metastore.BufferedSave(db, size=10)
        self.assertIs(buf.db, db)
        self.assertEqual(buf.size, 10)
        self.assertIsInstance(buf.docs, list)
        self.assertEqual(buf.docs, [])
        self.assertIsInstance(buf.count, int)
        self.assertEqual(buf.count, 0)
        self.assertIsInstance(buf.conflicts, int)
        self.assertEqual(buf.conflicts, 0)

        buf = metastore.BufferedSave(db)
        self.assertEqual(buf.size, 25)

        # Save the first 24, which should just be stored in the buffer:
        docs = []
        for i in range(24):
            doc = {'_id': random_id(), 'i': i}
            docs.append(doc)
            buf.save(doc)
            self.assertEqual(buf.docs, docs)
            self.assertEqual(buf.count, 0)
            self.assertEqual(buf.conflicts, 0)
        self.assertEqual(
            db.get_many([doc['_id'] for doc in docs]),
            [None for doc in docs]
        )
        for doc in docs:
            self.assertNotIn('_rev', doc)

        # Now save the 25th, which should trigger a flush:
        doc = {'_id': random_id()}
        docs.append(doc)
        buf.save(doc)
        self.assertEqual(buf.docs, [])
        self.assertEqual(buf.count, 25)
        self.assertEqual(buf.conflicts, 0)
        self.assertEqual(len(docs), 25)
        self.assertEqual(
            db.get_many([doc['_id'] for doc in docs]),
            docs
        )
        for doc in docs:
            self.assertTrue(doc['_rev'].startswith('1-'))

        # Create conflicts, save till a flush is triggered:
        for i in range(19):
            db.post(docs[i])
        docs2 = []
        for i in range(24):
            doc = docs[i]
            docs2.append(doc)
            buf.save(doc)
            self.assertEqual(buf.docs, docs2)
            self.assertEqual(buf.count, 25)
            self.assertEqual(buf.conflicts, 0)
        buf.save(docs[-1])
        self.assertEqual(buf.docs, [])
        self.assertEqual(buf.count, 50)
        self.assertEqual(buf.conflicts, 19)
        for (i, doc) in enumerate(docs):
            if i < 19:
                self.assertTrue(doc['_rev'].startswith('1-'))
                self.assertNotEqual(db.get(doc['_id']), doc)
            else:
                self.assertTrue(doc['_rev'].startswith('2-'))
                self.assertEqual(db.get(doc['_id']), doc)

        # Put some in the buffer directly, test flush()
        docs3 = []
        for i in range(50):
            doc = {'_id': random_id()}
            docs3.append(doc)
            if i >= 18:
                db.post(doc)
        buf.docs.extend(docs3)
        buf.flush()
        self.assertEqual(buf.docs, [])
        self.assertEqual(buf.count, 100)
        self.assertEqual(buf.conflicts, 51)
        for (i, doc) in enumerate(docs3):
            if i >= 18:
                self.assertNotIn('_rev', doc)
                self.assertNotEqual(db.get(doc['_id']), doc)
            else:
                self.assertTrue(doc['_rev'].startswith('1-'))
                self.assertEqual(db.get(doc['_id']), doc)


class TestMetaStore(CouchCase):
    def test_init(self):
        db = util.get_db(self.env, True)
        ms = metastore.MetaStore(db)
        self.assertIs(ms.db, db)
        self.assertEqual(repr(ms), 'MetaStore({!r})'.format(db))

    def test_schema_check(self):
        db = util.get_db(self.env, True)
        ms = metastore.MetaStore(db)
        self.assertEqual(ms.schema_check(), 0)
        store_id1 = random_id()
        store_id2 = random_id()

        good = []
        for i in range(30):
            doc = {
                '_id': random_file_id(),
                'time': time.time(),
                'type': 'dmedia/file',
                'stored': {
                    store_id1: {
                        'copies': 2,
                        'mtime': int(time.time()),
                    },
                    store_id2: {
                        'copies': 1,
                        'mtime': int(time.time()),
                    }, 
                },
            }
            db.save(doc)
            good.append(doc)
        self.assertEqual(len(good), 30)

        bad = []
        for i in range(30):
            doc = {
                '_id': random_file_id(),
                'time': time.time(),
                'type': 'dmedia/file',
                'stored': {
                    store_id1: {
                        'copies': 2,
                        'mtime': time.time(),
                    },
                    store_id2: {
                        'copies': 1,
                        'mtime': time.time(),
                    }, 
                },
            }
            db.save(doc)
            bad.append(doc)
        self.assertEqual(len(bad), 30)

        tricky = []
        for i in range(30):
            doc = {
                '_id': random_file_id(),
                'time': time.time(),
                'type': 'dmedia/file',
                'stored': {
                    store_id1: {
                        'copies': 2,
                        'mtime': int(time.time()),
                    },
                    store_id2: {
                        'copies': 1,
                        'mtime': time.time(),
                    }, 
                },
            }
            db.save(doc)
            tricky.append(doc)
        self.assertEqual(len(tricky), 30)

        # Now test:
        self.assertEqual(ms.schema_check(), 60)
        for doc in good:
            self.assertEqual(db.get(doc['_id']), doc)
            self.assertTrue(doc['_rev'].startswith('1-'))
        for old in bad:
            new = db.get(old['_id'])
            self.assertNotEqual(new, old)
            self.assertTrue(new['_rev'].startswith('2-'))
            self.assertEqual(new['stored'],
                {
                    store_id1: {
                        'copies': 2,
                        'mtime': int(old['stored'][store_id1]['mtime']),
                    },
                    store_id2: {
                        'copies': 1,
                        'mtime': int(old['stored'][store_id2]['mtime']),
                    },
                }
            )
        for old in tricky:
            new = db.get(old['_id'])
            self.assertNotEqual(new, old)
            self.assertTrue(new['_rev'].startswith('2-'))
            self.assertEqual(new['stored'],
                {
                    store_id1: {
                        'copies': 2,
                        'mtime': old['stored'][store_id1]['mtime'],
                    },
                    store_id2: {
                        'copies': 1,
                        'mtime': int(old['stored'][store_id2]['mtime']),
                    },
                }
            )

        # Once more with feeling:
        self.assertEqual(ms.schema_check(), 0)

    def test_downgrade_by_never_verified(self):
        db = util.get_db(self.env, True)
        ms = metastore.MetaStore(db)

        # Test when empty
        self.assertEqual(ms.downgrade_by_never_verified(), 0)
        curtime = int(time.time())
        self.assertEqual(ms.downgrade_by_never_verified(curtime), 0)

        # Populate
        base = curtime - metastore.DOWNGRADE_BY_NEVER_VERIFIED
        store_id1 = random_id()
        store_id2 = random_id()
        docs = []
        count = 10
        for i in range(count):
            doc = {
                '_id': random_file_id(),
                'type': 'dmedia/file',
                'stored': {
                    store_id1: {
                        'copies': 1,
                        'mtime': base + i,
                    },
                    store_id2: {
                        'copies': 1,
                        'mtime': base + i + count,
                    },
                },
            }
            docs.append(doc)
        db.save_many(docs)
        ids = [doc['_id'] for doc in docs]

        # Test when none should be downgraded
        self.assertEqual(ms.downgrade_by_never_verified(curtime - 1), 0)
        for (old, new) in zip(docs, db.get_many(ids)):
            self.assertEqual(old, new)

        # Test when they all should be downgraded
        self.assertEqual(ms.downgrade_by_never_verified(curtime + 19), 10)
        for (i, doc) in enumerate(db.get_many(ids)):
            rev = doc.pop('_rev')
            self.assertTrue(rev.startswith('2-'))
            _id = ids[i]
            self.assertEqual(doc,
                {
                    '_id': _id,
                    'type': 'dmedia/file',
                    'stored': {
                        store_id1: {
                            'copies': 0,
                            'mtime': base + i,
                        },
                        store_id2: {
                            'copies': 0,
                            'mtime': base + i + count,
                        },
                    },
                }
            )

        # Test when they're all already downgraded
        docs = db.get_many(ids)
        self.assertEqual(ms.downgrade_by_never_verified(curtime + 19), 0)
        for (old, new) in zip(docs, db.get_many(ids)):
            self.assertEqual(old, new)

        # Test when only one store should be downgraded
        for doc in docs:
            doc['stored'][store_id1]['copies'] = 1
            doc['stored'][store_id2]['copies'] = 1
        db.save_many(docs)
        self.assertEqual(ms.downgrade_by_never_verified(curtime + 9), 10)
        for (i, doc) in enumerate(db.get_many(ids)):
            rev = doc.pop('_rev')
            self.assertTrue(rev.startswith('4-'))
            _id = ids[i]
            self.assertEqual(doc,
                {
                    '_id': _id,
                    'type': 'dmedia/file',
                    'stored': {
                        store_id1: {
                            'copies': 0,
                            'mtime': base + i,
                        },
                        store_id2: {
                            'copies': 1,
                            'mtime': base + i + count,
                        },
                    },
                }
            )

        # Again, test when they're all already downgraded
        docs = db.get_many(ids)
        self.assertEqual(ms.downgrade_by_never_verified(curtime + 9), 0)
        for (old, new) in zip(docs, db.get_many(ids)):
            self.assertEqual(old, new)

    def test_downgrade_by_last_verified(self):
        db = util.get_db(self.env, True)
        ms = metastore.MetaStore(db)

        # Test when empty
        self.assertEqual(ms.downgrade_by_last_verified(), 0)
        curtime = int(time.time())
        self.assertEqual(ms.downgrade_by_last_verified(curtime), 0)

        # Populate
        base = curtime - metastore.DOWNGRADE_BY_LAST_VERIFIED
        store_id1 = random_id()
        store_id2 = random_id()
        docs = []
        count = 10
        for i in range(count):
            doc = {
                '_id': random_file_id(),
                'type': 'dmedia/file',
                'stored': {
                    store_id1: {
                        'copies': 1,
                        'verified': base + i,
                    },
                    store_id2: {
                        'copies': 1,
                        'verified': base + i + count,
                    },
                },
            }
            docs.append(doc)
        db.save_many(docs)
        ids = [doc['_id'] for doc in docs]

        # Test when none should be downgraded
        self.assertEqual(ms.downgrade_by_last_verified(curtime - 1), 0)
        for (old, new) in zip(docs, db.get_many(ids)):
            self.assertEqual(old, new)

        # Test when they all should be downgraded
        self.assertEqual(ms.downgrade_by_last_verified(curtime + 19), 10)
        for (i, doc) in enumerate(db.get_many(ids)):
            rev = doc.pop('_rev')
            self.assertTrue(rev.startswith('2-'))
            _id = ids[i]
            self.assertEqual(doc,
                {
                    '_id': _id,
                    'type': 'dmedia/file',
                    'stored': {
                        store_id1: {
                            'copies': 0,
                            'verified': base + i,
                        },
                        store_id2: {
                            'copies': 0,
                            'verified': base + i + count,
                        },
                    },
                }
            )

        # Test when they're all already downgraded
        docs = db.get_many(ids)
        self.assertEqual(ms.downgrade_by_last_verified(curtime + 19), 0)
        for (old, new) in zip(docs, db.get_many(ids)):
            self.assertEqual(old, new)

        # Test when only one store should be downgraded
        for doc in docs:
            doc['stored'][store_id1]['copies'] = 1
            doc['stored'][store_id2]['copies'] = 1
        db.save_many(docs)
        self.assertEqual(ms.downgrade_by_last_verified(curtime + 9), 10)
        for (i, doc) in enumerate(db.get_many(ids)):
            rev = doc.pop('_rev')
            self.assertTrue(rev.startswith('4-'))
            _id = ids[i]
            self.assertEqual(doc,
                {
                    '_id': _id,
                    'type': 'dmedia/file',
                    'stored': {
                        store_id1: {
                            'copies': 0,
                            'verified': base + i,
                        },
                        store_id2: {
                            'copies': 1,
                            'verified': base + i + count,
                        },
                    },
                }
            )

        # Again, test when they're all already downgraded
        docs = db.get_many(ids)
        self.assertEqual(ms.downgrade_by_last_verified(curtime + 9), 0)
        for (old, new) in zip(docs, db.get_many(ids)):
            self.assertEqual(old, new)

    def test_downgrade_by_store_atime(self):
        class PassThrough(metastore.MetaStore):
            def downgrade_store(self, store_id):
                self._calls.append(store_id)
                return super().downgrade_store(store_id)

        db = util.get_db(self.env, True)
        ms = PassThrough(db)
        curtime = int(time.time())
        base = curtime - metastore.DOWNGRADE_BY_STORE_ATIME

        # Test when empty:
        ms._calls = []
        self.assertEqual(ms.downgrade_by_store_atime(), {})
        self.assertEqual(ms.downgrade_by_store_atime(curtime), {})
        self.assertEqual(ms._calls, [])

        # One store that's missing its doc:
        store_id1 = random_id()
        ids1 = tuple(random_id() for i in range(17))
        docs = [
            {
                '_id': _id,
                'type': 'dmedia/file',
                'stored': {store_id1: {'copies': 1}},
            }
            for _id in ids1
        ]
        db.save_many(docs)

        # Another store with an atime old enough to trigger a downgrade:
        store_id2 = random_id()
        doc2 = {'_id': store_id2, 'atime': base}
        db.save(doc2)
        ids2 = tuple(random_id() for i in range(18))
        docs = [
            {
                '_id': _id,
                'type': 'dmedia/file',
                'stored': {store_id2: {'copies': 1}},
            }
            for _id in ids2
        ]
        db.save_many(docs)

        # A store with an atime new enough to be okay:
        store_id3 = random_id()
        doc3 = {'_id': store_id3, 'atime': base + 1}
        db.save(doc3)
        ids3 = tuple(random_id() for i in range(19))
        docs = [
            {
                '_id': _id,
                'type': 'dmedia/file',
                'stored': {store_id3: {'copies': 1}},
            }
            for _id in ids3
        ]
        db.save_many(docs)

        # And finally a store missing its doc['atime']:
        store_id4 = random_id()
        doc4 = {'_id': store_id4}
        db.save(doc4)
        ids4 = tuple(random_id() for i in range(20))
        docs = [
            {
                '_id': _id,
                'type': 'dmedia/file',
                'stored': {store_id4: {'copies': 1}},
            }
            for _id in ids4
        ]
        db.save_many(docs)

        # Test at curtime:
        self.assertEqual(ms.downgrade_by_store_atime(curtime),
            {store_id1: 17, store_id2: 18, store_id4: 20}
        )
        self.assertEqual(ms._calls,
            sorted([store_id1, store_id2, store_id4])
        )
        for doc in db.get_many(ids1):
            self.assertTrue(doc['_rev'].startswith('2-'))
            self.assertEqual(doc['stored'], {store_id1: {'copies': 0}})
        for doc in db.get_many(ids2):
            self.assertTrue(doc['_rev'].startswith('2-'))
            self.assertEqual(doc['stored'], {store_id2: {'copies': 0}})
        for doc in db.get_many(ids3):
            self.assertTrue(doc['_rev'].startswith('1-'))
            self.assertEqual(doc['stored'], {store_id3: {'copies': 1}})
        for doc in db.get_many(ids4):
            self.assertTrue(doc['_rev'].startswith('2-'))
            self.assertEqual(doc['stored'], {store_id4: {'copies': 0}})

        # Test at curtime + 1:
        ms._calls = []
        self.assertEqual(ms.downgrade_by_store_atime(curtime + 1),
            {store_id1: 0, store_id2: 0, store_id3: 19, store_id4: 0}
        )
        self.assertEqual(ms._calls,
            sorted([store_id1, store_id2, store_id3, store_id4])
        )
        for doc in db.get_many(ids1):
            self.assertTrue(doc['_rev'].startswith('2-'))
            self.assertEqual(doc['stored'], {store_id1: {'copies': 0}})
        for doc in db.get_many(ids2):
            self.assertTrue(doc['_rev'].startswith('2-'))
            self.assertEqual(doc['stored'], {store_id2: {'copies': 0}})
        for doc in db.get_many(ids3):
            self.assertTrue(doc['_rev'].startswith('2-'))
            self.assertEqual(doc['stored'], {store_id3: {'copies': 0}})
        for doc in db.get_many(ids4):
            self.assertTrue(doc['_rev'].startswith('2-'))
            self.assertEqual(doc['stored'], {store_id4: {'copies': 0}})

        # Make sure the dmedia/store docs aren't modified:
        with self.assertRaises(microfiber.NotFound) as cm:
            db.get(store_id1)
        self.assertEqual(db.get(store_id2), doc2)
        self.assertEqual(db.get(store_id3), doc3)
        self.assertEqual(db.get(store_id4), doc4)

    def test_downgrade_store(self):    
        db = util.get_db(self.env, True)
        ms = metastore.MetaStore(db)
        store_id1 = random_id()
        store_id2 = random_id()
        store_id3 = random_id()
        self.assertEqual(ms.downgrade_store(store_id1), 0)
        ids = [random_file_id() for i in range(189)]
        docs = []
        for _id in ids:
            doc = {
                '_id': _id,
                'type': 'dmedia/file',
                'stored': {
                    store_id1: {
                        'copies': 1,
                        'mtime': 123,
                        'verified': int(time.time()),
                    },
                    store_id2: {
                        'copies': 2,
                        'mtime': 456,
                    },
                },
            }
            docs.append(doc)
        db.save_many(docs)

        # Make sure downgrading an unrelated store causes no change:
        self.assertEqual(ms.downgrade_store(store_id3), 0)
        for (old, new) in zip(docs, db.get_many(ids)):
            self.assertEqual(old, new)

        # Downgrade the first store:
        self.assertEqual(ms.downgrade_store(store_id1), 189)
        for (_id, doc) in zip(ids, db.get_many(ids)):
            rev = doc.pop('_rev')
            self.assertTrue(rev.startswith('2-'))
            self.assertEqual(doc,
                {
                    '_id': _id,
                    'type': 'dmedia/file',
                    'stored': {
                        store_id1: {
                            'copies': 0,
                            'mtime': 123,
                        },
                        store_id2: {
                            'copies': 2,
                            'mtime': 456,
                        },
                    },
                }
            )

        # Downgrade the 2nd store:
        self.assertEqual(ms.downgrade_store(store_id2), 189)
        for (_id, doc) in zip(ids, db.get_many(ids)):
            rev = doc.pop('_rev')
            self.assertTrue(rev.startswith('3-'))
            self.assertEqual(doc,
                {
                    '_id': _id,
                    'type': 'dmedia/file',
                    'stored': {
                        store_id1: {
                            'copies': 0,
                            'mtime': 123,
                        },
                        store_id2: {
                            'copies': 0,
                            'mtime': 456,
                        },
                    },
                }
            )

        # Make sure downgrading both again causes no change:
        docs = db.get_many(ids)
        self.assertEqual(ms.downgrade_store(store_id1), 0)
        self.assertEqual(ms.downgrade_store(store_id2), 0)
        for (old, new) in zip(docs, db.get_many(ids)):
            self.assertEqual(old, new)

        # Test when some already have copies=0:
        sample = random.sample(ids, 23)
        docs2 = db.get_many(sample)
        for doc in docs2:
            doc['stored'][store_id1]['copies'] = 1
        db.save_many(docs2)
        self.assertEqual(ms.downgrade_store(store_id1), 23)
        for (_id, doc) in zip(ids, db.get_many(ids)):
            rev = doc.pop('_rev')
            if _id in sample:
                self.assertTrue(rev.startswith('5-'))
            else:
                self.assertTrue(rev.startswith('3-'))
            self.assertEqual(doc,
                {
                    '_id': _id,
                    'type': 'dmedia/file',
                    'stored': {
                        store_id1: {
                            'copies': 0,
                            'mtime': 123,
                        },
                        store_id2: {
                            'copies': 0,
                            'mtime': 456,
                        },
                    },
                }
            )

        # Test when some have junk values for copies:
        sample2 = list(filter(lambda _id: _id not in sample, ids))
        docs2 = db.get_many(sample2)
        for (i, doc) in enumerate(docs2):
            # `False` makes sure the file/nonzero view is using !==
            junk = ('hello', False)[i % 2 == 0]
            doc['stored'][store_id2]['copies'] = junk
        db.save_many(docs2)
        self.assertEqual(ms.downgrade_store(store_id2), 166)
        for (_id, doc) in zip(ids, db.get_many(ids)):
            rev = doc.pop('_rev')
            self.assertTrue(rev.startswith('5-'))
            self.assertEqual(doc,
                {
                    '_id': _id,
                    'type': 'dmedia/file',
                    'stored': {
                        store_id1: {
                            'copies': 0,
                            'mtime': 123,
                        },
                        store_id2: {
                            'copies': 0,
                            'mtime': 456,
                        },
                    },
                }
            )

        # Again, make sure downgrading both again causes no change:
        docs = db.get_many(ids)
        self.assertEqual(ms.downgrade_store(store_id1), 0)
        self.assertEqual(ms.downgrade_store(store_id2), 0)
        for (old, new) in zip(docs, db.get_many(ids)):
            self.assertEqual(old, new)

    def test_purge_store(self):    
        db = util.get_db(self.env, True)
        ms = metastore.MetaStore(db)
        store_id1 = random_id()
        store_id2 = random_id()
        store_id3 = random_id()

        # Test when empty:
        self.assertEqual(ms.purge_store(store_id1), 0)

        ids = [random_file_id() for i in range(189)]
        docs = []
        for _id in ids:
            doc = {
                '_id': _id,
                'type': 'dmedia/file',
                'stored': {
                    store_id1: {
                        'copies': 1,
                        'mtime': 123,
                    },
                    store_id2: {
                        'copies': 2,
                        'mtime': 456,
                    },
                },
            }
            docs.append(doc)
        db.save_many(docs)

        # Make sure purging an unrelated store causes no change:
        self.assertEqual(ms.purge_store(store_id3), 0)
        for (old, new) in zip(docs, db.get_many(ids)):
            self.assertEqual(old, new)

        # Purge the first store:
        self.assertEqual(ms.purge_store(store_id1), 189)
        for (_id, doc) in zip(ids, db.get_many(ids)):
            rev = doc.pop('_rev')
            self.assertTrue(rev.startswith('2-'))
            self.assertEqual(doc,
                {
                    '_id': _id,
                    'type': 'dmedia/file',
                    'stored': {
                        store_id2: {
                            'copies': 2,
                            'mtime': 456,
                        },
                    },
                }
            )

        # Purge the 2nd store:
        self.assertEqual(ms.purge_store(store_id2), 189)
        for (_id, doc) in zip(ids, db.get_many(ids)):
            rev = doc.pop('_rev')
            self.assertTrue(rev.startswith('3-'))
            self.assertEqual(doc,
                {
                    '_id': _id,
                    'type': 'dmedia/file',
                    'stored': {},
                }
            )

        # Make sure purging both again causes no change:
        docs = db.get_many(ids)
        self.assertEqual(ms.purge_store(store_id1), 0)
        self.assertEqual(ms.purge_store(store_id2), 0)
        for (old, new) in zip(docs, db.get_many(ids)):
            self.assertEqual(old, new)

        # Test when some already have been purged:
        sample = random.sample(ids, 23)
        docs2 = db.get_many(sample)
        for doc in docs2:
            doc['stored'] = {
                store_id1: {
                    'copies': 1,
                    'mtime': 123,
                },
            }
        db.save_many(docs2)
        self.assertEqual(ms.purge_store(store_id1), 23)
        for (_id, doc) in zip(ids, db.get_many(ids)):
            rev = doc.pop('_rev')
            if _id in sample:
                self.assertTrue(rev.startswith('5-'))
            else:
                self.assertTrue(rev.startswith('3-'))
            self.assertEqual(doc,
                {
                    '_id': _id,
                    'type': 'dmedia/file',
                    'stored': {},
                }
            )

        # Again, make sure purging both again causes no change:
        docs = db.get_many(ids)
        self.assertEqual(ms.purge_store(store_id1), 0)
        self.assertEqual(ms.purge_store(store_id2), 0)
        for (old, new) in zip(docs, db.get_many(ids)):
            self.assertEqual(old, new)

    def test_scan(self):
        db = util.get_db(self.env, True)
        ms = metastore.MetaStore(db)
        fs = TempFileStore(random_id(), 1)
        db.save({'_id': fs.id, 'type': 'dmedia/store'})

        # A few good files
        good = [create_random_file(fs, db) for i in range(10)]

        # A few files with bad mtime
        bad_mtime = [create_random_file(fs, db) for i in range(8)]
        for doc in bad_mtime:
            value = doc['stored'][fs.id]
            value['mtime'] -= 100
            value['verified'] = 1234567890
            value['pinned'] = True
            db.save(doc)

        # A few files with bad size
        bad_size = [create_random_file(fs, db) for i in range(4)]
        for doc in bad_size:
            doc['bytes'] += 1776
            db.save(doc)

        # A few missing files
        missing = [create_random_file(fs, db) for i in range(4)]
        for doc in missing:
            fs.remove(doc['_id'])

        self.assertEqual(ms.scan(fs), 26)

        for doc in good:
            self.assertEqual(db.get(doc['_id']), doc)
            self.assertTrue(doc['_rev'].startswith('1-'))

        for doc in bad_mtime:
            _id = doc['_id']
            doc = db.get(_id)
            self.assertTrue(doc['_rev'].startswith('3-'))
            self.assertEqual(doc['stored'],
                {
                    fs.id: {
                        'copies': 0,
                        'mtime': get_mtime(fs, _id),
                        'pinned': True,
                    },
                }
            )

        for doc in bad_size:
            _id = doc['_id']
            doc = db.get(_id)
            self.assertTrue(doc['_rev'].startswith('3-'))
            self.assertEqual(doc['stored'], {})
            ts = doc['corrupt'][fs.id]['time']
            self.assertIsInstance(ts, float)
            self.assertLessEqual(ts, time.time())
            self.assertEqual(doc['corrupt'],
                {
                    fs.id: {
                        'time': ts,
                    },
                }
            )
            self.assertFalse(path.exists(fs.path(_id)))
            self.assertTrue(path.isfile(fs.corrupt_path(_id)))

        for doc in missing:
            _id = doc['_id']
            doc = db.get(_id)
            self.assertTrue(doc['_rev'].startswith('2-'))
            self.assertEqual(doc['stored'], {})

        doc = db.get(fs.id)
        self.assertTrue(doc['_rev'].startswith('2-'))
        atime = doc.get('atime')
        self.assertIsInstance(atime, int)
        self.assertLessEqual(atime, int(time.time()))

    def test_relink(self):
        db = util.get_db(self.env, True)
        ms = metastore.MetaStore(db)
        fs = TempFileStore(random_id(), 1)

        # A few good files
        good = [create_random_file(fs, db) for i in range(8)]

        # A few missing files
        missing = [create_random_file(fs, db) for i in range(18)]
        for doc in missing:
            doc['stored'] = {}
            db.save(doc)

        self.assertEqual(ms.relink(fs), 18)
        for doc in good:
            _id = doc['_id']
            self.assertEqual(db.get(_id), doc)
            fs.verify(_id)
        for doc in missing:
            _id = doc['_id']
            doc = db.get(_id)
            self.assertTrue(doc['_rev'].startswith('3-'))
            fs.verify(_id)
            self.assertEqual(doc['stored'],
                {
                    fs.id: {
                        'copies': 1,
                        'mtime': get_mtime(fs, _id),
                    },
                }
            )
        self.assertEqual(ms.relink(fs), 0)

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
        stored = create_stored(ch.id, fs1, fs2)
        doc = schema.create_file(time.time(), ch, stored)
        db.save(doc)
        doc = ms.remove(fs1, ch.id)
        self.assertTrue(doc['_rev'].startswith('2-'))
        self.assertEqual(doc, db.get(ch.id))
        self.assertEqual(doc['stored'],
            {
                fs2.id: {
                    'mtime': get_mtime(fs2, ch.id),
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
                    'mtime': get_mtime(fs2, ch.id),
                    'copies': 1,
                },   
            }
        )

    def test_verify(self):
        db = util.get_db(self.env, True)
        ms = metastore.MetaStore(db)
        tmp = TempDir()
        fs = util.init_filestore(tmp.dir)[0]
        (file, ch) = tmp.random_file()

        # Test when file doc isn't in dmedia-0
        with self.assertRaises(microfiber.NotFound) as cm:
            ms.remove(fs, ch.id)

        # Test when file and doc are present
        self.assertEqual(fs.import_file(open(file.name, 'rb')), ch)
        stored = create_stored(ch.id, fs)
        doc = schema.create_file(time.time(), ch, stored)
        db.save(doc)
        self.assertEqual(ms.verify(fs, ch.id), ch)
        doc = db.get(ch.id)
        self.assertTrue(doc['_rev'].startswith('2-'))
        schema.check_file(doc)
        verified = doc['stored'][fs.id]['verified']
        self.assertIsInstance(verified, int)
        self.assertLessEqual(verified, int(time.time()))
        self.assertEqual(doc['stored'],
            {
                fs.id: {
                    'copies': 1,
                    'mtime': get_mtime(fs, ch.id),
                    'verified': verified,
                },
            }
        )

    def test_verify_all(self):
        db = util.get_db(self.env, True)
        ms = metastore.MetaStore(db)
        fs = TempFileStore(random_id(), 1)

        # 6 files need verification
        base = int(time.time())
        docs1 = [create_random_file(fs, db) for i in range(6)]
        ids = [doc['_id'] for doc in docs1]
        self.assertEqual(ms.verify_all(fs), 6)
        self.assertEqual(ms.verify_all(fs), 0)
        docs2 = db.get_many(ids)
        for (old, new) in zip(docs1, docs2):
            schema.check_file(new)
            self.assertTrue(new['_rev'].startswith('2-'))
            verified = new['stored'][fs.id]['verified']
            self.assertIsInstance(verified, int)
            self.assertLess(verified, time.time())
            self.assertEqual(new['stored'],
                {
                    fs.id: {
                        'copies': 1,
                        'mtime': old['stored'][fs.id]['mtime'],
                        'verified': verified,
                    },
                }
            )

        # Only 4 need checked
        again = docs2[:4]
        assert len(again) == 4
        for doc in again:
            doc['stored'][fs.id]['verified'] -= (metastore.VERIFY_THRESHOLD + 1)
            db.save(doc)
        self.assertEqual(ms.verify_all(fs), 4)
        self.assertEqual(ms.verify_all(fs), 0)
        again = set(doc['_id'] for doc in again)
        docs3 = db.get_many(ids)
        for doc in docs3:
            rev = doc['_rev']
            if doc['_id'] in again:
                self.assertTrue(rev.startswith('4-'))
            else:
                self.assertTrue(rev.startswith('2-'))

    def test_content_md5(self):
        db = util.get_db(self.env, True)
        ms = metastore.MetaStore(db)
        fs = TempFileStore(random_id(), 1)

        _id = random_file_id()
        with self.assertRaises(microfiber.NotFound) as cm:
            ms.content_md5(fs, _id)

        doc = create_random_file(fs, db)
        _id = doc['_id']
        self.assertNotIn('content_md5', doc)
        content_md5 = ms.content_md5(fs, _id)
        self.assertEqual(content_md5, fs.content_md5(_id)[1])
        doc = db.get(_id)
        self.assertTrue(doc['_rev'].startswith('2-'))
        self.assertEqual(doc['content_md5'], content_md5)
        verified = doc['stored'][fs.id]['verified']
        self.assertIsInstance(verified, int)
        self.assertLessEqual(verified, int(time.time()))
        self.assertEqual(doc['stored'],
            {
                fs.id: {
                    'copies': 1,
                    'mtime': get_mtime(fs, _id),
                    'verified': verified,
                },   
            }
        )

        self.assertEqual(ms.content_md5(fs, _id), content_md5)
        self.assertTrue(db.get(_id)['_rev'].startswith('2-'))
        self.assertEqual(ms.content_md5(fs, _id, force=True), content_md5)
        self.assertTrue(db.get(_id)['_rev'].startswith('3-'))

    def test_allocate_partial(self):
        db = util.get_db(self.env, True)
        ms = metastore.MetaStore(db)
        fs1 = TempFileStore(random_id(), 1)
        fs2 = TempFileStore(random_id(), 1)

        _id = random_file_id()
        with self.assertRaises(microfiber.NotFound) as cm:
            ms.allocate_partial(fs2, _id)

        doc = create_random_file(fs1, db)
        _id = doc['_id']
        tmp_fp = ms.allocate_partial(fs2, _id)
        self.assertIsInstance(tmp_fp, io.BufferedWriter)
        self.assertEqual(tmp_fp.name,
            path.join(fs2.basedir, 'partial', _id)
        )
        doc = db.get(_id)
        self.assertTrue(doc['_rev'].startswith('2-'))
        self.assertEqual(doc['stored'],
            {
                fs1.id: {
                    'copies': 1,
                    'mtime': get_mtime(fs1, _id),
                },
            }   
        )
        self.assertEqual(doc['partial'],
            {
                fs2.id: {
                    'mtime': path.getmtime(fs2.partial_path(_id)),
                },
            }   
        )

        # Also test MetaStore.verify_and_move():
        src_fp = fs1.open(_id)
        while True:
            chunk = src_fp.read(1024 * 1024)
            if not chunk:
                break
            tmp_fp.write(chunk)
        tmp_fp.close()
        tmp_fp = open(tmp_fp.name, 'rb')
        ms.verify_and_move(fs2, tmp_fp, _id)
        doc = db.get(_id)
        self.assertTrue(doc['_rev'].startswith('3-'))
        self.assertEqual(doc['stored'],
            {
                fs1.id: {
                    'copies': 1,
                    'mtime': get_mtime(fs1, _id),
                },
                fs2.id: {
                    'copies': 1,
                    'mtime': get_mtime(fs2, _id),
                },
            }   
        )
        self.assertNotIn('partial', doc)

    def test_copy(self):
        db = util.get_db(self.env, True)
        ms = metastore.MetaStore(db)
        fs1 = TempFileStore(random_id(), 1)
        fs2 = TempFileStore(random_id(), 1)
        fs3 = TempFileStore(random_id(), 1)

        # doc does not exist:
        _id = random_file_id()
        with self.assertRaises(microfiber.NotFound) as cm:
            ms.copy(fs1, _id, fs2)

        # File does not exist
        doc = {
            '_id': _id,
            'stored': {
                fs1.id: {
                    'copies': 1,
                    'mtime': int(time.time()),
                },
            }
        }
        db.save(doc)
        ret = ms.copy(fs1, _id, fs2, fs3)
        self.assertEqual(ret, db.get(_id))
        self.assertEqual(ret,
            {
                '_id': _id,
                '_rev': ret['_rev'],
                'stored': {},
            }
        )

        # File is corrupt
        doc = create_random_file(fs1, db)
        _id = doc['_id']
        filename = fs1.path(_id)
        os.chmod(filename, 0o600)
        open(filename, 'ab').write(os.urandom(16))
        os.chmod(filename, 0o444)
        ret = ms.copy(fs1, _id, fs2, fs3)
        self.assertEqual(ret, db.get(_id))
        self.assertEqual(ret,
            {
                '_id': _id,
                '_rev': ret['_rev'],
                '_attachments': ret['_attachments'],
                'time': doc['time'],
                'atime': doc['atime'],
                'type': 'dmedia/file',
                'bytes': doc['bytes'],
                'origin': 'user',
                'stored': {},
                'corrupt': {
                    fs1.id: {'time': ret['corrupt'][fs1.id]['time']}
                }
            }
        )

        doc = create_random_file(fs1, db)
        _id = doc['_id']
        ms.copy(fs1, _id, fs2)
        doc = db.get(_id)
        self.assertTrue(doc['_rev'].startswith('2-'))
        verified = doc['stored'][fs1.id]['verified']
        self.assertIsInstance(verified, int)
        self.assertLessEqual(verified, int(time.time()))
        self.assertEqual(doc['stored'],
            {
                fs1.id: {
                    'copies': 1,
                    'mtime': get_mtime(fs1, _id),
                    'verified': verified,
                },
                fs2.id: {
                    'copies': 1,
                    'mtime': get_mtime(fs2, _id),
                }, 
            }
        )
        fs1.verify(_id)
        fs2.verify(_id)

        doc = create_random_file(fs1, db)
        _id = doc['_id']
        ms.copy(fs1, _id, fs2, fs3)
        doc = db.get(_id)
        self.assertTrue(doc['_rev'].startswith('2-'))
        verified = doc['stored'][fs1.id]['verified']
        self.assertIsInstance(verified, int)
        self.assertLessEqual(verified, int(time.time()))
        self.assertEqual(doc['stored'],
            {
                fs1.id: {
                    'copies': 1,
                    'mtime': get_mtime(fs1, _id),
                    'verified': verified,
                },
                fs2.id: {
                    'copies': 1,
                    'mtime': get_mtime(fs2, _id),
                },
                fs3.id: {
                    'copies': 1,
                    'mtime': get_mtime(fs3, _id),
                },
            }
        )
        fs1.verify(_id)
        fs2.verify(_id)
        fs3.verify(_id)
