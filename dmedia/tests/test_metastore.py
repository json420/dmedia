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
from random import SystemRandom
from copy import deepcopy

from filestore import TempFileStore, FileStore, DIGEST_BYTES
import microfiber
from microfiber import random_id

from dmedia.tests.base import TempDir, write_random, random_file_id
from dmedia.tests.couch import CouchCase
from dmedia import util, schema, metastore
from dmedia.metastore import create_stored


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
                    'mtime': fs1.stat(ch.id).mtime,
                },
            }
        )
        self.assertEqual(metastore.create_stored(ch.id, fs1, fs2),
            {
                fs1.id: {
                    'copies': 0,
                    'mtime': fs1.stat(ch.id).mtime,
                },
                fs2.id: {
                    'copies': 2,
                    'mtime': fs2.stat(ch.id).mtime,
                }, 
            }
        )

    def test_merge_stored(self):
        id1 = random_id()
        id2 = random_id()
        id3 = random_id()
        ts1 = time.time()
        ts2 = time.time() - 2.5
        ts3 = time.time() - 5
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
                'verified': int(ts3 + 100),
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
                    'verified': int(ts3 + 100),
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
                'verified': int(ts3 + 100),
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
                    'verified': int(ts3 + 100),
                    'pinned': True,
                },
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
        self.assertEqual(repr(ms), 'MetaStore({!r})'.format(db))

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
                        'mtime': fs.stat(_id).mtime,
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
                        'mtime': fs.stat(_id).mtime,
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
                    'mtime': fs.stat(ch.id).mtime,
                    'verified': verified,
                },
            }
        )

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
                    'mtime': fs.stat(_id).mtime,
                    'verified': verified,
                },   
            }
        )

        self.assertEqual(ms.content_md5(fs, _id), content_md5)
        self.assertTrue(db.get(_id)['_rev'].startswith('2-'))
        self.assertEqual(ms.content_md5(fs, _id, force=True), content_md5)
        self.assertTrue(db.get(_id)['_rev'].startswith('3-'))

