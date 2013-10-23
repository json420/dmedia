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
import shutil

import filestore
from filestore.misc import TempFileStore
from dbase32 import random_id, isdb32
import microfiber
from microfiber import dumps, Conflict

from dmedia.tests.base import TempDir, write_random, random_file_id
from dmedia.tests.couch import CouchCase
from dmedia.local import LocalStores
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


def random_time():
    return time.time() - random.randint(0, 1234567890)


def random_int_time():
    return random.randint(1, 1234567890)


class DummyStat:
    def __init__(self, mtime):
        self.mtime = mtime


class DummyFileStore:
    def __init__(self):
        self.id = random_id()
        self.copies = 1
        self._mtime = random_time()
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


class TestConstants(TestCase):
    def test_order_of_time_constants(self):
        """
        Test the relative magnitude of the time constants.

        The exact values will continue to be tuned, but there is import logic
        in the inequalities.  For example, this should always be true:

        >>> metastore.DOWNGRADE_BY_STORE_ATIME < metastore.PURGE_BY_STORE_ATIME
        True

        And this should always be true:

        >>> metastore.VERIFY_BY_MTIME < metastore.DOWNGRADE_BY_MTIME
        True

        This test ensures that we don't accidentally break these relationships
        as we tune the values.
        """
        order = (
            metastore.VERIFY_BY_MTIME,
            metastore.DOWNGRADE_BY_MTIME,
            metastore.DOWNGRADE_BY_STORE_ATIME,
            metastore.VERIFY_BY_VERIFIED,
            metastore.PURGE_BY_STORE_ATIME,
            metastore.DOWNGRADE_BY_VERIFIED,
        )
        for value in order:
            self.assertIsInstance(value, int)
            self.assertGreater(value, 0)
        self.assertEqual(tuple(sorted(order)), order)

    def test_DAY(self):
        self.assertIsInstance(metastore.DAY, int)
        self.assertEqual(metastore.DAY, 60 * 60 * 24)

    def check_day_multiple(self, value):
        """
        Check that *value* is a multiple of `metastore.DAY` seconds.
        """
        self.assertIsInstance(value, int)
        self.assertGreaterEqual(value, metastore.DAY)
        self.assertEqual(value % metastore.DAY, 0)

    def test_DOWNGRADE_BY_MTIME(self):
        self.check_day_multiple(metastore.DOWNGRADE_BY_MTIME)

    def test_DOWNGRADE_BY_STORE_ATIME(self):
        self.check_day_multiple(metastore.DOWNGRADE_BY_STORE_ATIME)
        self.assertGreater(
            metastore.DOWNGRADE_BY_STORE_ATIME // metastore.DAY,
            metastore.DOWNGRADE_BY_MTIME // metastore.DAY
        )

    def test_PURGE_BY_STORE_ATIME(self):
        self.check_day_multiple(metastore.PURGE_BY_STORE_ATIME)
        self.assertGreater(
            metastore.PURGE_BY_STORE_ATIME // metastore.DAY,
            metastore.DOWNGRADE_BY_STORE_ATIME // metastore.DAY
        )

    def test_DOWNGRADE_BY_VERIFIED(self):
        self.check_day_multiple(metastore.DOWNGRADE_BY_VERIFIED)
        self.assertGreater(
            metastore.DOWNGRADE_BY_VERIFIED // metastore.DAY,
            metastore.PURGE_BY_STORE_ATIME // metastore.DAY
        )

    def test_VERIFY_BY_MTIME(self):
        self.assertIsInstance(metastore.VERIFY_BY_MTIME, int)
        parent = metastore.DOWNGRADE_BY_MTIME
        self.assertTrue(
            parent // 24 <= metastore.VERIFY_BY_MTIME <= parent // 2
        ) 

    def test_VERIFY_BY_VERIFIED(self):
        self.assertIsInstance(metastore.VERIFY_BY_VERIFIED, int)
        parent = metastore.DOWNGRADE_BY_VERIFIED
        self.assertTrue(
            parent // 4 <= metastore.VERIFY_BY_VERIFIED <= parent // 2
        )
        self.assertGreater(metastore.VERIFY_BY_VERIFIED, metastore.VERIFY_BY_MTIME) 


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

    def test_create_stored(self):
        tmp = TempDir()
        fs1 = TempFileStore(copies=0)
        fs2 = TempFileStore(copies=2)
        (file, ch) = tmp.random_file()
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

    def test_mark_added(self):
        _id = random_file_id()
        fs1_id = random_id()
        fs2_id = random_id()
        mtime1 = random_int_time()
        mtime2 = random_int_time()

        # Empty, broken doc:
        doc = {'_id': _id}
        new = {
            fs1_id: {'copies': 1, 'mtime': mtime1},
        }
        self.assertIsNone(metastore.mark_added(doc, new))
        self.assertEqual(doc, 
            {
                '_id': _id,
                'stored': {
                    fs1_id: {
                        'copies': 1,
                        'mtime': mtime1,
                    },
                },
            }
        )

        # Bad doc['stored'] type:
        doc = {'_id': _id, 'stored': 'naughty'}
        new = {
            fs1_id: {'copies': 1, 'mtime': mtime1},
            fs2_id: {'copies': 0, 'mtime': mtime2},
        }
        self.assertIsNone(metastore.mark_added(doc, new))
        self.assertEqual(doc, 
            {
                '_id': _id,
                'stored': {
                    fs1_id: {
                        'copies': 1,
                        'mtime': mtime1,
                    },
                    fs2_id: {
                        'copies': 0,
                        'mtime': mtime2,
                    }
                },
            }
        )

        # Ensure `new` is properly merged into existing doc['stored'][fs1_id]:
        doc = {
            '_id': _id,
            'stored': {
                fs1_id: {
                    'copies': 17,  # Replaced
                    'mtime': random_int_time(),  # Replaced
                    'verified': random_int_time(),  # Removed
                    'pinned': True,  # Preserved
                },
            },
        }
        new = {
            fs1_id: {'copies': 1, 'mtime': mtime1},
            fs2_id: {'copies': 0, 'mtime': mtime2},
        }
        self.assertIsNone(metastore.mark_added(doc, new))
        self.assertEqual(doc, 
            {
                '_id': _id,
                'stored': {
                    fs1_id: {
                        'copies': 1,
                        'mtime': mtime1,
                        'pinned': True,
                    },
                    fs2_id: {
                        'copies': 0,
                        'mtime': mtime2,
                    }
                },
            }
        )

        # Ensure unrelated entries in doc['stored'] are not changed:
        doc = {
            '_id': _id,
            'stored': {
                fs2_id: 'truly junk',
            },
        }
        new = {
            fs1_id: {'copies': 1, 'mtime': mtime1},
        }
        self.assertIsNone(metastore.mark_added(doc, new))
        self.assertEqual(doc, 
            {
                '_id': _id,
                'stored': {
                    fs1_id: {
                        'copies': 1,
                        'mtime': mtime1,
                    },
                    fs2_id: 'truly junk',
                },
            }
        )

    def test_mark_downloading(self):
        _id = random_file_id()
        fs1_id = random_id()
        fs2_id = random_id()
        timestamp = random_time()

        # Empty, broken doc:
        doc = {'_id': _id}
        self.assertIsNone(metastore.mark_downloading(doc, timestamp, fs1_id))
        self.assertEqual(doc,
            {
                '_id': _id,
                'partial': {
                    fs1_id: {'time': timestamp},
                }
            }
        )

        # doc['partial'] isn't a dict:
        doc = {'_id': _id, 'partial': 'hello'}
        self.assertIsNone(metastore.mark_downloading(doc, timestamp, fs1_id))
        self.assertEqual(doc,
            {
                '_id': _id,
                'partial': {
                    fs1_id: {'time': timestamp},
                }
            }
        )

        # make sure existing doc['partial'][fs1_id] is replaced:
        doc = {
            '_id': _id,
            'partial': {
                fs1_id: {'time': random_time(), 'foo': 'bar'},
            }
        }
        self.assertIsNone(metastore.mark_downloading(doc, timestamp, fs1_id))
        self.assertEqual(doc,
            {
                '_id': _id,
                'partial': {
                    fs1_id: {'time': timestamp},
                },
            }
        )

        # make sure other items in doc['partial'] aren't disturbed:
        doc = {
            '_id': _id,
            'partial': {
                fs1_id: 'junk',
                fs2_id: 'also junk',
            }
        }
        self.assertIsNone(metastore.mark_downloading(doc, timestamp, fs1_id))
        self.assertEqual(doc,
            {
                '_id': _id,
                'partial': {
                    fs1_id: {'time': timestamp},
                    fs2_id: 'also junk',
                },
            }
        )

    def test_mark_downloaded(self):
        _id = random_file_id()
        fs1_id = random_id()
        fs2_id = random_id()
        mtime1 = random_int_time()
        mtime2 = random_int_time()

        # Empty, broken doc:
        doc = {'_id': _id}
        new = {fs1_id: {'mtime': mtime1, 'copies': 1}}
        self.assertIsNone(metastore.mark_downloaded(doc, fs1_id, new))
        self.assertEqual(doc,
            {
                '_id': _id,
                'stored': {
                    fs1_id: {
                        'copies': 1,
                        'mtime': mtime1,
                    },
                },
            }
        )

        # doc['stored'] and doc['partial'] are the wrong type:
        doc = {'_id': _id, 'stored': 'dirty', 'partial': 'bad'}
        new = {fs1_id: {'mtime': mtime1, 'copies': 1}}
        self.assertIsNone(metastore.mark_downloaded(doc, fs1_id, new))
        self.assertEqual(doc,
            {
                '_id': _id,
                'stored': {
                    fs1_id: {
                        'copies': 1,
                        'mtime': mtime1,
                    },
                },
            }
        )

        # doc['partial'] only contains fs1_id:
        doc = {
            '_id': _id,
            'partial': {fs1_id: {}},
        }
        new = {fs1_id: {'mtime': mtime1, 'copies': 1}}
        self.assertIsNone(metastore.mark_downloaded(doc, fs1_id, new))
        self.assertEqual(doc,
            {
                '_id': _id,
                'stored': {
                    fs1_id: {
                        'copies': 1,
                        'mtime': mtime1,
                    },
                },
            }
        )

        # doc['partial'] contains both fs1_id and fs2_id:
        doc = {
            '_id': _id,
            'partial': {fs1_id: {}, fs2_id: {}},
        }
        new = {fs1_id: {'mtime': mtime1, 'copies': 1}}
        self.assertIsNone(metastore.mark_downloaded(doc, fs1_id, new))
        self.assertEqual(doc,
            {
                '_id': _id,
                'stored': {
                    fs1_id: {
                        'copies': 1,
                        'mtime': mtime1,
                    },
                },
                'partial': {fs2_id: {}},
            }
        )

        # Make sure new is properly merged into existing doc['stored']:
        doc = {
            '_id': _id,
            'stored': {
                fs1_id: {
                    'copies': 2,
                    'mtime': random_int_time(),
                    'verified': random_int_time(),
                    'pinned': True,
                },
                fs2_id: {
                    'copies': 1,
                    'mtime': mtime2,
                    'verified': mtime2 + 1,
                },
            },
            'partial': {
                fs1_id: {},
                fs2_id: {},
            },
        }
        new = {fs1_id: {'mtime': mtime1, 'copies': 1}}
        self.assertIsNone(metastore.mark_downloaded(doc, fs1_id, new))
        self.assertEqual(doc,
            {
                '_id': _id,
                'stored': {
                    fs1_id: {
                        'copies': 1,
                        'mtime': mtime1,
                        'pinned': True,
                    },
                    fs2_id: {
                        'copies': 1,
                        'mtime': mtime2,
                        'verified': mtime2 + 1,
                    }
                },
                'partial': {
                    fs2_id: {},
                },
            }
        )

    def test_mark_removed(self):
        _id = random_file_id()
        fs1_id = random_id()
        fs2_id = random_id()
        fs3_id = random_id()
        mtime1 = random_int_time()

        # Empty, broken doc:
        doc = {'_id': _id}
        self.assertIsNone(metastore.mark_removed(doc, fs1_id, fs2_id))
        self.assertEqual(doc,
            {
                '_id': _id,
                'stored': {},   
            }
        )

        # doc['stored'] is wrong type:
        doc = {'_id': _id, 'stored': 'very very bad'}
        self.assertIsNone(metastore.mark_removed(doc, fs1_id, fs2_id))
        self.assertEqual(doc,
            {
                '_id': _id,
                'stored': {},   
            }
        )

        # Ensure that entries are removed, even if broken:
        doc = {
            '_id': _id,
            'stored': {
                fs1_id: {
                    'copies': 1,
                    'mtime': random_int_time(),
                    'verified': random_int_time(),
                    'pinned': True,
                },
                fs2_id: 'Ich bin ein Broken',
            },
        }
        self.assertIsNone(metastore.mark_removed(doc, fs1_id, fs2_id, fs3_id))
        self.assertEqual(doc,
            {
                '_id': _id,
                'stored': {},   
            }
        )

        # Ensure that unrelated entries are not changed:
        doc = {
            '_id': _id,
            'stored': {
                fs1_id: {
                    'copies': 1,
                    'mtime': mtime1,
                },
                fs2_id: {
                    'copies': 1,
                    'mtime': random_int_time(),
                },
                fs3_id: 'Ich bin ein Broken',
            },
        }
        self.assertIsNone(metastore.mark_removed(doc, fs2_id))
        self.assertEqual(doc,
            {
                '_id': _id,
                'stored': {
                    fs1_id: {
                        'copies': 1,
                        'mtime': mtime1,
                    },
                    fs3_id: 'Ich bin ein Broken',
                },   
            }
        )

    def test_mark_verified(self):
        _id = random_file_id()
        fs_id = random_id()
        mtime = random_int_time()
        verified = random_int_time()
        fs2_id = random_id()
        mtime2 = random_int_time()
        bar = random_id()

        # Empty, broken doc:
        doc = {'_id': _id}
        value = {'copies': 1, 'mtime': mtime, 'verified': verified}
        self.assertIsNone(metastore.mark_verified(doc, fs_id, value))
        self.assertEqual(doc,
            {
                '_id': _id,
                'stored': {
                    fs_id: {
                        'copies': 1,
                        'mtime': mtime,
                        'verified': verified,
                    },
                },
            }
        )

        # doc['stored'] is wrong type:
        doc = {'_id': _id, 'stored': 'naught nurse'}
        value = {'copies': 2, 'mtime': mtime, 'verified': verified}
        self.assertIsNone(metastore.mark_verified(doc, fs_id, value))
        self.assertEqual(doc,
            {
                '_id': _id,
                'stored': {
                    fs_id: {
                        'copies': 2,
                        'mtime': mtime,
                        'verified': verified,
                    },
                },
            }
        )

        # old_value is wrong type, plus ensure unrelated entries are not changed:
        doc = {
            '_id': _id,
            'stored': {
                fs_id: 'dirty dirty',
                fs2_id: {
                    'copies': 3,
                    'mtime': mtime2,
                }
            },
        }
        value = {'copies': 1, 'mtime': mtime, 'verified': verified}
        self.assertIsNone(metastore.mark_verified(doc, fs_id, value))
        self.assertEqual(doc,
            {
                '_id': _id,
                'stored': {
                    fs_id: {
                        'copies': 1,
                        'mtime': mtime,
                        'verified': verified,
                    },
                    fs2_id: {
                        'copies': 3,
                        'mtime': mtime2,
                    },
                },
            }
        )

        # Ensure new_value is properly merged in with old_value:
        doc = {
            '_id': _id,
            'stored': {
                fs_id: {
                    'copies': 18,  # Replaced
                    'mtime': random_int_time(),  # Replaced
                    'verified': random_int_time(),  # Replaced
                    'pinned': True,  # Preserved
                    'foo': bar,  # Preserved
                },
                fs2_id: {
                    'copies': 1,
                    'mtime': mtime2,
                }
            },
        }
        value = {'copies': 2, 'mtime': mtime, 'verified': verified}
        self.assertIsNone(metastore.mark_verified(doc, fs_id, value))
        self.assertEqual(doc,
            {
                '_id': _id,
                'stored': {
                    fs_id: {
                        'copies': 2,
                        'mtime': mtime,
                        'verified': verified,
                        'pinned': True,
                        'foo': bar,
                    },
                    fs2_id: {
                        'copies': 1,
                        'mtime': mtime2,
                    },
                },
            }
        )

    def test_mark_corrupt(self):
        _id = random_file_id()
        timestamp = random_time()
        fs_id = random_id()
        fs2_id = random_id()

        # Empty, broken doc:
        doc = {'_id': _id}
        self.assertIsNone(metastore.mark_corrupt(doc, timestamp, fs_id))
        self.assertEqual(doc, 
            {
                '_id': _id,
                'stored': {},
                'corrupt': {
                    fs_id: {'time': timestamp},
                },
            }
        )

        # doc['corrupt'] and doc['stored'] both have wrong type:
        doc = {'_id': _id, 'stored': 'very', 'corrupt': 'bad'}
        self.assertIsNone(metastore.mark_corrupt(doc, timestamp, fs_id))
        self.assertEqual(doc, 
            {
                '_id': _id,
                'stored': {},
                'corrupt': {
                    fs_id: {'time': timestamp},
                },
            }
        )

        # Ensure that doc['stored'] entry is deleted, doc['corrupt'] is replaced:
        doc = {
            '_id': _id,
            'stored': {
                fs_id: {
                    'copies': 1,
                    'mtime': random_int_time(),
                    'verified': random_int_time(),
                    'pinned': True,
                },
            },
            'corrupt': {
                fs_id: {
                    'time': random_time(),
                    'foo': random_id(),
                    'bar': random_id(),
                }
            }
        }
        self.assertIsNone(metastore.mark_corrupt(doc, timestamp, fs_id))
        self.assertEqual(doc, 
            {
                '_id': _id,
                'stored': {},
                'corrupt': {
                    fs_id: {'time': timestamp},
                },
            }
        )

        # Ensure that unrelated entries are not changed:
        doc = {
            '_id': _id,
            'stored': {
                fs_id: {
                    'copies': 1,
                    'mtime': random_int_time(),
                    'verified': random_int_time(),
                    'pinned': True,
                },
                fs2_id: 'hello',
            },
            'corrupt': {
                fs_id: {
                    'time': random_time(),
                    'foo': random_id(),
                    'bar': random_id(),
                },
                fs2_id: 'world',
            }
        }
        self.assertIsNone(metastore.mark_corrupt(doc, timestamp, fs_id))
        self.assertEqual(doc, 
            {
                '_id': _id,
                'stored': {
                    fs2_id: 'hello',
                },
                'corrupt': {
                    fs_id: {'time': timestamp},
                    fs2_id: 'world',
                },
            }
        )

    def test_mark_copied(self):
        _id = random_file_id()
        timestamp = random_time()
        src_id = random_id()
        src_mtime = random_int_time()
        dst1_id = random_id()
        dst1_mtime = random_int_time()
        dst2_id = random_id()
        dst2_mtime = random_int_time()
        other_id1 = random_id()
        other_id2 = random_id()

        # One destination, no doc['stored']:
        doc = {'_id': _id}
        new = {
            src_id: {'copies': 1, 'mtime': src_mtime},
            dst1_id: {'copies': 1, 'mtime': dst1_mtime},
        }
        self.assertIsNone(metastore.mark_copied(doc, timestamp, src_id, new))
        self.assertEqual(doc, 
            {
                '_id': _id,
                'stored': {
                    src_id: {
                        'copies': 1,
                        'mtime': src_mtime,
                        'verified': int(timestamp),
                    },
                    dst1_id: {
                        'copies': 1,
                        'mtime': dst1_mtime,
                    },
                }
            }
        )

        # Two destinations, no doc['stored']:
        doc = {'_id': _id}
        new = {
            src_id: {'copies': 2, 'mtime': src_mtime},
            dst1_id: {'copies': 1, 'mtime': dst1_mtime},
            dst2_id: {'copies': 3, 'mtime': dst2_mtime},
        }
        self.assertIsNone(metastore.mark_copied(doc, timestamp, src_id, new))
        self.assertEqual(doc, 
            {
                '_id': _id,
                'stored': {
                    src_id: {
                        'copies': 2,
                        'mtime': src_mtime,
                        'verified': int(timestamp),
                    },
                    dst1_id: {
                        'copies': 1,
                        'mtime': dst1_mtime,
                    },
                    dst2_id: {
                        'copies': 3,
                        'mtime': dst2_mtime,
                    },
                }
            }
        )

        # One destination, existing doc['stored']:
        doc = {
            '_id': _id,
            'stored': {
                src_id: {
                    'copies': 21,
                    'mtime': random_int_time(),
                    'verified': random_int_time(),
                    'pinned': True,
                },
                dst1_id: {
                    'copies': 17,
                    'mtime': random_int_time(),
                    'verified': random_int_time(),
                    'pinned': True,
                },
                other_id1: 'foo',
                other_id2: 'bar',
            },
        }
        new = {
            src_id: {'copies': 1, 'mtime': src_mtime},
            dst1_id: {'copies': 2, 'mtime': dst1_mtime},
        }
        self.assertIsNone(metastore.mark_copied(doc, timestamp, src_id, new))
        self.assertEqual(doc, 
            {
                '_id': _id,
                'stored': {
                    src_id: {
                        'copies': 1,
                        'mtime': src_mtime,
                        'verified': int(timestamp),
                        'pinned': True,
                    },
                    dst1_id: {
                        'copies': 2,
                        'mtime': dst1_mtime,
                        'pinned': True,
                    },
                    other_id1: 'foo',
                    other_id2: 'bar',
                },
            }
        )

        # Two destinations, existing doc['stored']:
        doc = {
            '_id': _id,
            'stored': {
                src_id: {'pinned': True},
                dst1_id: 'junk value',
                dst2_id: {'verified': random_int_time()},
                other_id1: 'foo',
                other_id2: 'bar',
            }
        }
        new = {
            src_id: {'copies': 3, 'mtime': src_mtime},
            dst1_id: {'copies': 2, 'mtime': dst1_mtime},
            dst2_id: {'copies': 1, 'mtime': dst2_mtime},
        }
        self.assertIsNone(metastore.mark_copied(doc, timestamp, src_id, new))
        self.assertEqual(doc, 
            {
                '_id': _id,
                'stored': {
                    src_id: {
                        'copies': 3,
                        'mtime': src_mtime,
                        'verified': int(timestamp),
                        'pinned': True,
                    },
                    dst1_id: {
                        'copies': 2,
                        'mtime': dst1_mtime,
                    },
                    dst2_id: {
                        'copies': 1,
                        'mtime': dst2_mtime,
                    },
                    other_id1: 'foo',
                    other_id2: 'bar',
                },
            }
        )

        # One destination, broken doc['stored'][src_id]:
        doc = {
            '_id': _id,
            'stored': {
                src_id: 'bad dog',
                other_id1: 'foo',
                other_id2: 'bar',
            }
        }
        new = {
            src_id: {'copies': 1, 'mtime': src_mtime},
            dst1_id: {'copies': 1, 'mtime': dst1_mtime},
        }
        self.assertIsNone(metastore.mark_copied(doc, timestamp, src_id, new))
        self.assertEqual(doc, 
            {
                '_id': _id,
                'stored': {
                    src_id: {
                        'copies': 1,
                        'mtime': src_mtime,
                        'verified': int(timestamp),
                    },
                    dst1_id: {
                        'copies': 1,
                        'mtime': dst1_mtime,
                    },
                    other_id1: 'foo',
                    other_id2: 'bar',
                },
            }
        )

        # Two destinations, broken doc['stored'][src_id]:
        doc = {
            '_id': _id,
            'stored': {
                src_id: 'still a bad dog',
                dst1_id: ['hello', 'naughty'],
                dst2_id: 18,
                other_id1: 'foo',
                other_id2: 'bar',
            }
        }
        new = {
            src_id: {'copies': 3, 'mtime': src_mtime},
            dst1_id: {'copies': 3, 'mtime': dst1_mtime},
            dst2_id: {'copies': 3, 'mtime': dst2_mtime},
        }
        self.assertIsNone(metastore.mark_copied(doc, timestamp, src_id, new))
        self.assertEqual(doc, 
            {
                '_id': _id,
                'stored': {
                    src_id: {
                        'copies': 3,
                        'mtime': src_mtime,
                        'verified': int(timestamp),
                    },
                    dst1_id: {
                        'copies': 3,
                        'mtime': dst1_mtime,
                    },
                    dst2_id: {
                        'copies': 3,
                        'mtime': dst2_mtime,
                    },
                    other_id1: 'foo',
                    other_id2: 'bar',
                },
            }
        )

    def test_mark_mismatched(self):
        _id = random_file_id()
        fs1_id = random_id()
        fs1_mtime = random_int_time()
        fs2_id = random_id()
        fs2_mtime = random_int_time()
        fs2_verified = random_int_time()

        # Empty, broken doc:
        doc = {'_id': _id}
        self.assertIsNone(metastore.mark_mismatched(doc, fs1_id, fs1_mtime))
        self.assertEqual(doc, 
            {
                '_id': _id,
                'stored': {
                    fs1_id: {
                        'copies': 0,
                        'mtime': fs1_mtime,
                    },
                },
            }
        )

        # Wrong doc['stored'] type:
        doc = {'_id': _id, 'stored': 'naughty naughty'}
        self.assertIsNone(metastore.mark_mismatched(doc, fs1_id, fs1_mtime))
        self.assertEqual(doc, 
            {
                '_id': _id,
                'stored': {
                    fs1_id: {
                        'copies': 0,
                        'mtime': fs1_mtime,
                    },
                },
            }
        )

        # Wrong doc['stored'][fs1_id] type:
        doc = {
            '_id': _id,
            'stored': {
                fs1_id: 'junk',
                fs2_id: 'also junk',
            },
        }
        self.assertIsNone(metastore.mark_mismatched(doc, fs1_id, fs1_mtime))
        self.assertEqual(doc, 
            {
                '_id': _id,
                'stored': {
                    fs1_id: {
                        'copies': 0,
                        'mtime': fs1_mtime,
                    },
                    fs2_id: 'also junk',
                },
            }
        )

        # Make sure value is properly updated, others left alone:
        doc = {
            '_id': _id,
            'stored': {
                fs1_id: {
                    'copies': 17,
                    'mtime': random_int_time(),
                    'verified': random_int_time(),
                    'pinned': True,
                },
                fs2_id: {
                    'copies': 18,
                    'mtime': fs2_mtime,
                    'verified': fs2_verified,
                    'pinned': True,
                },
            },
        }
        self.assertIsNone(metastore.mark_mismatched(doc, fs1_id, fs1_mtime))
        self.assertEqual(doc, 
            {
                '_id': _id,
                'stored': {
                    fs1_id: {
                        'copies': 0,
                        'mtime': fs1_mtime,
                        'pinned': True,
                    },
                    fs2_id: {
                        'copies': 18,
                        'mtime': fs2_mtime,
                        'verified': fs2_verified,
                        'pinned': True,
                    },
                },
            }
        )

    def test_relink_iter(self):
        fs = TempFileStore()

        def create():
            _id = random_file_id()
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
        self.assertIsInstance(ms.log_db, microfiber.Database)
        self.assertEqual(ms.log_db.name, 'log-1')
        self.assertIs(ms.log_db.ctx, ms.db.ctx)
        self.assertIs(ms.machine_id, self.env['machine_id'])

        log_db = db.database('log-1')
        ms = metastore.MetaStore(db, log_db=log_db)
        self.assertIs(ms.db, db)
        self.assertIs(ms.log_db, log_db)
        self.assertEqual(repr(ms), 'MetaStore({!r})'.format(db))
        self.assertIs(ms.machine_id, self.env['machine_id'])

    def test_log(self):
        db = util.get_db(self.env, True)
        log_db = db.database('log-1')
        self.assertTrue(log_db.ensure())
        ms = metastore.MetaStore(db, log_db)

        ts = time.time()
        doc = ms.log(ts, 'dmedia/test')
        self.assertEqual(doc, log_db.get(doc['_id']))
        self.assertEqual(doc['_rev'][:2], '1-')
        self.assertEqual(doc,
            {
                '_id': doc['_id'],
                '_rev': doc['_rev'],
                'time': ts,
                'type': 'dmedia/test',
                'machine_id': self.env['machine_id'],
            }
        )

        ts = time.time()
        doc = ms.log(ts, 'dmedia/test2', foo='bar', stuff='junk')
        self.assertEqual(doc, log_db.get(doc['_id']))
        self.assertEqual(doc['_rev'][:2], '1-')
        self.assertEqual(doc,
            {
                '_id': doc['_id'],
                '_rev': doc['_rev'],
                'time': ts,
                'type': 'dmedia/test2',
                'machine_id': self.env['machine_id'],
                'foo': 'bar',
                'stuff': 'junk',
            }
        )

    def test_log_file_corrupt(self):
        db = util.get_db(self.env, True)
        log_db = db.database('log-1')
        self.assertTrue(log_db.ensure())
        ms = metastore.MetaStore(db, log_db)
        fs = TempFileStore()
        _id = random_file_id()

        ts = time.time()
        doc = ms.log_file_corrupt(ts, fs, _id)
        self.assertEqual(doc, log_db.get(doc['_id']))
        self.assertEqual(doc['_rev'][:2], '1-')
        self.assertEqual(doc,
            {
                '_id': doc['_id'],
                '_rev': doc['_rev'],
                'time': ts,
                'type': 'dmedia/file/corrupt',
                'machine_id': self.env['machine_id'],
                'file_id': _id,
                'store_id': fs.id,
            }
        )

    def test_doc_and_id(self):
        db = util.get_db(self.env, True)
        ms = metastore.MetaStore(db)
        _id = random_id()
        doc = {'_id': _id}

        # Doc doesn't exist:
        self.assertEqual(ms.doc_and_id(doc), (doc, _id))
        with self.assertRaises(microfiber.NotFound) as cm:
            ms.doc_and_id(_id)
        with self.assertRaises(TypeError) as cm:
            ms.doc_and_id([_id])
        self.assertEqual(str(cm.exception),
            'obj must be a doc or _id (a dict or str)'
        )

        # Doc does exist:
        db.save(doc)
        self.assertEqual(ms.doc_and_id(doc), (doc, _id))
        self.assertEqual(ms.doc_and_id(_id), (doc, _id))
        with self.assertRaises(TypeError) as cm:
            ms.doc_and_id([_id])
        self.assertEqual(str(cm.exception),
            'obj must be a doc or _id (a dict or str)'
        )

    def test_content_hash(self):
        db = util.get_db(self.env, True)
        ms = metastore.MetaStore(db)

        # doc doesn't exist
        _id = random_file_id()
        doc = {'_id': _id}
        with self.assertRaises(microfiber.NotFound) as cm:
            ms.content_hash(doc)
        with self.assertRaises(microfiber.NotFound) as cm:
            ms.content_hash(_id)
        with self.assertRaises(TypeError) as cm:
            ms.content_hash([_id])
        self.assertEqual(str(cm.exception),
            'obj must be a doc or _id (a dict or str)'
        )

        #############
        # doc exists:
        fs = TempFileStore()
        doc = create_random_file(fs, db)
        _id = doc['_id']
        att = db.get_att(_id, 'leaf_hashes')
        leaf_hashes = att.data
        leaf_hashes_unpacked = tuple(filestore.iter_leaf_hashes(leaf_hashes))

        # By doc:
        ch = ms.content_hash(doc)
        self.assertIsInstance(ch, filestore.ContentHash)
        self.assertEqual(ch,
            filestore.ContentHash(_id, doc['bytes'], leaf_hashes_unpacked)
        )
        ch = ms.content_hash(doc, unpack=True)
        self.assertIsInstance(ch, filestore.ContentHash)
        self.assertEqual(ch,
            filestore.ContentHash(_id, doc['bytes'], leaf_hashes_unpacked)
        )
        ch = ms.content_hash(doc, unpack=False)
        self.assertIsInstance(ch, filestore.ContentHash)
        self.assertEqual(ch,
            filestore.ContentHash(_id, doc['bytes'], leaf_hashes)
        )

        # By _id:
        ch = ms.content_hash(_id)
        self.assertIsInstance(ch, filestore.ContentHash)
        self.assertEqual(ch,
            filestore.ContentHash(_id, doc['bytes'], leaf_hashes_unpacked)
        )
        ch = ms.content_hash(_id, unpack=True)
        self.assertIsInstance(ch, filestore.ContentHash)
        self.assertEqual(ch,
            filestore.ContentHash(_id, doc['bytes'], leaf_hashes_unpacked)
        )
        ch = ms.content_hash(_id, unpack=False)
        self.assertIsInstance(ch, filestore.ContentHash)
        self.assertEqual(ch,
            filestore.ContentHash(_id, doc['bytes'], leaf_hashes)
        )

        # Wrong type:
        with self.assertRaises(TypeError) as cm:
            ms.content_hash([_id])
        self.assertEqual(str(cm.exception),
            'obj must be a doc or _id (a dict or str)'
        )

        # Integrity issue:
        doc['bytes'] += 1
        db.save(doc)
        with self.assertRaises(filestore.RootHashError):
            ms.content_hash(doc)
        with self.assertRaises(filestore.RootHashError):
            ms.content_hash(_id)

        # leaf_hashes attachement is missing:
        doc['bytes'] -= 1
        db.save(doc)
        ms.content_hash(doc)
        ms.content_hash(_id)
        _rev = db.delete(_id, 'leaf_hashes', rev=doc['_rev'])['rev']
        with self.assertRaises(microfiber.NotFound) as cm:
            ms.content_hash(doc)
        with self.assertRaises(microfiber.NotFound) as cm:
            ms.content_hash(_id)

        # Badly-formed leaf_hashes:
        bad = os.urandom(75)
        db.put_att('application/octet-stream', bad, _id, 'leaf_hashes', rev=_rev)
        with self.assertRaises(ValueError) as cm:
            ms.content_hash(doc)
        self.assertEqual(str(cm.exception),
            'len(leaf_hashes) is 75, not multiple of 30'
        )
        with self.assertRaises(ValueError) as cm:
            ms.content_hash(_id)
        self.assertEqual(str(cm.exception),
            'len(leaf_hashes) is 75, not multiple of 30'
        )

    def test_get_machine(self):
        db = util.get_db(self.env, True)
        ms = metastore.MetaStore(db)

        # Machine doc is missing:
        self.assertEqual(ms.get_machine(), {})

        # Machine doc exists:
        machine = {'_id': self.env['machine_id']}
        db.save(machine)
        self.assertEqual(ms.get_machine(), machine)

        # Machine doc is updated:
        machine['type'] = 'dmedia/machine'
        db.save(machine)
        self.assertEqual(ms.get_machine(), machine)

    def test_get_local_stores(self):
        db = util.get_db(self.env, True)
        ms = metastore.MetaStore(db)

        # machine doc is missing:
        ls = ms.get_local_stores()
        self.assertIsInstance(ls, LocalStores)
        self.assertEqual(ls.local_stores(), {})

        # machine doc exists, but is missing 'stores':
        machine = {'_id': self.env['machine_id']}
        db.save(machine)
        ls = ms.get_local_stores()
        self.assertIsInstance(ls, LocalStores)
        self.assertEqual(ls.local_stores(), {})

        # machine has 'stores':
        fs1 = TempFileStore()
        stores1 = {
            fs1.id: {'parentdir': fs1.parentdir, 'copies': fs1.copies},
        }
        machine['stores'] = stores1
        db.save(machine)
        ls = ms.get_local_stores()
        self.assertIsInstance(ls, LocalStores)
        self.assertEqual(ls.local_stores(), stores1)

        # machine['stores'] has changed
        fs2 = TempFileStore()
        stores2 = {
            fs1.id: {'parentdir': fs1.parentdir, 'copies': fs1.copies},
            fs2.id: {'parentdir': fs2.parentdir, 'copies': fs2.copies},
        }
        machine['stores'] = stores2
        db.save(machine)
        ls = ms.get_local_stores()
        self.assertIsInstance(ls, LocalStores)
        self.assertNotEqual(ls.local_stores(), stores1)
        self.assertEqual(ls.local_stores(), stores2)

    def test_get_local_peers(self):
        db = util.get_db(self.env, True)
        ms = metastore.MetaStore(db)

        # machine doc is missing:
        self.assertEqual(ms.get_local_peers(), {})

        # machine doc exists, but is missing 'peers':
        machine = {'_id': self.env['machine_id']}
        db.save(machine)
        self.assertEqual(ms.get_local_peers(), {})

        # machine has 'peers':
        peers = {
            random_id(30): {'url': random_id()},
            random_id(30): {'url': random_id()},
        }
        machine['peers'] = peers
        db.save(machine)
        self.assertEqual(ms.get_local_peers(), peers)

    def test_iter_stores(self):
        db = util.get_db(self.env, True)
        ms = metastore.MetaStore(db)
        store_ids = sorted(random_id() for i in range(6))

        # Test when empty:
        self.assertEqual(list(ms.iter_stores()), [])

        # Test with one file, 3 stores:
        doc1 = {
            '_id': random_file_id(),
            'type': 'dmedia/file',
            'stored': {
                store_ids[0]: {},
                store_ids[1]: {},
                store_ids[2]: {},
            },
        }
        db.save(doc1)
        self.assertEqual(list(ms.iter_stores()),
            [store_ids[0], store_ids[1], store_ids[2]]
        )

        # Add 3 more docs, 3 more stores:
        doc2 = {
            '_id': random_file_id(),
            'type': 'dmedia/file',
            'stored': {
                store_ids[3]: {},
                store_ids[0]: {},
                store_ids[1]: {},
            },
        }
        doc3 = {
            '_id': random_file_id(),
            'type': 'dmedia/file',
            'stored': {
                store_ids[4]: {},
                store_ids[0]: {},
            },
        }
        doc4 = {
            '_id': random_file_id(),
            'type': 'dmedia/file',
            'stored': {
                store_ids[5]: {},
            },
        }
        db.save_many([doc2, doc3, doc4])
        self.assertEqual(list(ms.iter_stores()), store_ids)

        # Make sure doc['type'] is checked:
        del doc1['type']
        db.save(doc1)
        self.assertEqual(list(ms.iter_stores()),
            [store_ids[0], store_ids[1], store_ids[3], store_ids[4], store_ids[5]]
        )

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

    def test_downgrade_by_mtime(self):
        db = util.get_db(self.env, True)
        ms = metastore.MetaStore(db)

        # Test when empty
        curtime = int(time.time())
        self.assertEqual(ms.downgrade_by_mtime(curtime), 0)

        # Populate
        base = curtime - metastore.DOWNGRADE_BY_MTIME
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
        self.assertEqual(ms.downgrade_by_mtime(curtime - 1), 0)
        for (old, new) in zip(docs, db.get_many(ids)):
            self.assertEqual(old, new)

        # Test when they all should be downgraded
        self.assertEqual(ms.downgrade_by_mtime(curtime + 19), 10)
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
        self.assertEqual(ms.downgrade_by_mtime(curtime + 19), 0)
        for (old, new) in zip(docs, db.get_many(ids)):
            self.assertEqual(old, new)

        # Test when only one store should be downgraded
        for doc in docs:
            doc['stored'][store_id1]['copies'] = 1
            doc['stored'][store_id2]['copies'] = 1
        db.save_many(docs)
        self.assertEqual(ms.downgrade_by_mtime(curtime + 9), 10)
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
        self.assertEqual(ms.downgrade_by_mtime(curtime + 9), 0)
        for (old, new) in zip(docs, db.get_many(ids)):
            self.assertEqual(old, new)

    def test_downgrade_by_verified(self):
        db = util.get_db(self.env, True)
        ms = metastore.MetaStore(db)

        # Test when empty
        curtime = int(time.time())
        self.assertEqual(ms.downgrade_by_verified(curtime), 0)

        # Populate
        base = curtime - metastore.DOWNGRADE_BY_VERIFIED
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
        self.assertEqual(ms.downgrade_by_verified(curtime - 1), 0)
        for (old, new) in zip(docs, db.get_many(ids)):
            self.assertEqual(old, new)

        # Test when they all should be downgraded
        self.assertEqual(ms.downgrade_by_verified(curtime + 19), 10)
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
        self.assertEqual(ms.downgrade_by_verified(curtime + 19), 0)
        for (old, new) in zip(docs, db.get_many(ids)):
            self.assertEqual(old, new)

        # Test when only one store should be downgraded
        for doc in docs:
            doc['stored'][store_id1]['copies'] = 1
            doc['stored'][store_id2]['copies'] = 1
        db.save_many(docs)
        self.assertEqual(ms.downgrade_by_verified(curtime + 9), 10)
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
        self.assertEqual(ms.downgrade_by_verified(curtime + 9), 0)
        for (old, new) in zip(docs, db.get_many(ids)):
            self.assertEqual(old, new)

    def test_purge_or_downgrade_by_store_atime(self):
        class PassThrough(metastore.MetaStore):
            def purge_store(self, store_id):
                self._calls.append(('purge', store_id))
                return super().purge_store(store_id)

            def downgrade_store(self, store_id):
                self._calls.append(('downgrade', store_id))
                return super().downgrade_store(store_id)

        db = util.get_db(self.env, True)
        ms = PassThrough(db)
        curtime = int(time.time())
        purge_base = curtime - metastore.PURGE_BY_STORE_ATIME
        downgrade_base = curtime - metastore.DOWNGRADE_BY_STORE_ATIME
        docs = []
        store_ids = sorted(random_id() for i in range(7))

        # Test when empty:
        ms._calls = []
        self.assertEqual(ms.purge_or_downgrade_by_store_atime(curtime), {})
        self.assertEqual(ms._calls, [])

        # purge: missing dmedia/store doc
        ids0 = tuple(random_file_id() for i in range(17))
        docs.extend(
            {
                '_id': _id,
                'type': 'dmedia/file',
                'stored': {store_ids[0]: {'copies': 1}},
            }
            for _id in ids0
        )

        # purge at curtime
        doc1 = {'_id': store_ids[1], 'atime': purge_base}
        docs.append(doc1)
        ids1 = tuple(random_file_id() for i in range(18))
        docs.extend(
            {
                '_id': _id,
                'type': 'dmedia/file',
                'stored': {store_ids[1]: {'copies': 1}},
            }
            for _id in ids1
        )

        # downgrade at curtime, purge at curtime + 1
        doc2 = {'_id': store_ids[2], 'atime': purge_base + 1}
        docs.append(doc2)
        ids2 = tuple(random_file_id() for i in range(19))
        docs.extend(
            {
                '_id': _id,
                'type': 'dmedia/file',
                'stored': {store_ids[2]: {'copies': 1}},
            }
            for _id in ids2
        )

        # downgrade at curtime
        doc3 = {'_id': store_ids[3], 'atime': downgrade_base}
        docs.append(doc3)
        ids3 = tuple(random_file_id() for i in range(20))
        docs.extend(
            {
                '_id': _id,
                'type': 'dmedia/file',
                'stored': {store_ids[3]: {'copies': 1}},
            }
            for _id in ids3
        )

        # downgrade at curtime + 1
        doc4 = {'_id': store_ids[4], 'atime': downgrade_base + 1}
        docs.append(doc4)
        ids4 = tuple(random_file_id() for i in range(21))
        docs.extend(
            {
                '_id': _id,
                'type': 'dmedia/file',
                'stored': {store_ids[4]: {'copies': 1}},
            }
            for _id in ids4
        )

        # purge: missing atime
        doc5 = {'_id': store_ids[5]}
        docs.append(doc5)
        ids5= tuple(random_file_id() for i in range(22))
        docs.extend(
            {
                '_id': _id,
                'type': 'dmedia/file',
                'stored': {store_ids[5]: {'copies': 1}},
            }
            for _id in ids5
        )

        # purge: atime not an int
        doc6 = {'_id': store_ids[6], 'atime': 'hello'}
        docs.append(doc6)
        ids6= tuple(random_file_id() for i in range(23))
        docs.extend(
            {
                '_id': _id,
                'type': 'dmedia/file',
                'stored': {store_ids[6]: {'copies': 1}},
            }
            for _id in ids6
        )

        # Randomize order, save docs:
        random.shuffle(docs)
        db.save_many(docs)
        self.assertEqual(db.get_many(store_ids),
            [None, doc1, doc2, doc3, doc4, doc5, doc6]
        )

        # Test at curtime:
        self.assertEqual(ms.purge_or_downgrade_by_store_atime(curtime), {
            store_ids[0]: ('purge', 17),
            store_ids[1]: ('purge', 18),
            store_ids[2]: ('downgrade', 19),
            store_ids[3]: ('downgrade', 20),
            store_ids[5]: ('purge', 22),
            store_ids[6]: ('purge', 23),
        })
        self.assertEqual(ms._calls, [
            ('purge', store_ids[0]),
            ('purge', store_ids[1]),
            ('downgrade', store_ids[2]),
            ('downgrade', store_ids[3]),
            ('purge', store_ids[5]),
            ('purge', store_ids[6]),
        ])
        for doc in db.get_many(ids0):
            self.assertEqual(doc['_rev'][:2], '2-')
            self.assertEqual(doc['stored'], {})
        for doc in db.get_many(ids1):
            self.assertEqual(doc['_rev'][:2], '2-')
            self.assertEqual(doc['stored'], {})
        for doc in db.get_many(ids2):
            self.assertEqual(doc['_rev'][:2], '2-')
            self.assertEqual(doc['stored'], {store_ids[2]: {'copies': 0}})
        for doc in db.get_many(ids3):
            self.assertEqual(doc['_rev'][:2], '2-')
            self.assertEqual(doc['stored'], {store_ids[3]: {'copies': 0}})
        for doc in db.get_many(ids4):
            self.assertEqual(doc['_rev'][:2], '1-')
            self.assertEqual(doc['stored'], {store_ids[4]: {'copies': 1}})
        for doc in db.get_many(ids5):
            self.assertEqual(doc['_rev'][:2], '2-')
            self.assertEqual(doc['stored'], {})
        for doc in db.get_many(ids6):
            self.assertEqual(doc['_rev'][:2], '2-')
            self.assertEqual(doc['stored'], {})
        self.assertEqual(db.get_many(store_ids),
            [None, None, doc2, doc3, doc4, None, None]
        )

        # Test at curtime + 1:
        ms._calls = []
        self.assertEqual(ms.purge_or_downgrade_by_store_atime(curtime + 1), {
            store_ids[2]: ('purge', 19),
            store_ids[3]: ('downgrade', 0),
            store_ids[4]: ('downgrade', 21),
        })
        self.assertEqual(ms._calls, [
            ('purge', store_ids[2]),
            ('downgrade', store_ids[3]),
            ('downgrade', store_ids[4]),
        ])
        for doc in db.get_many(ids0):
            self.assertEqual(doc['_rev'][:2], '2-')
            self.assertEqual(doc['stored'], {})
        for doc in db.get_many(ids1):
            self.assertEqual(doc['_rev'][:2], '2-')
            self.assertEqual(doc['stored'], {})
        for doc in db.get_many(ids2):
            self.assertEqual(doc['_rev'][:2], '3-')
            self.assertEqual(doc['stored'], {})
        for doc in db.get_many(ids3):
            self.assertEqual(doc['_rev'][:2], '2-')
            self.assertEqual(doc['stored'], {store_ids[3]: {'copies': 0}})
        for doc in db.get_many(ids4):
            self.assertEqual(doc['_rev'][:2], '2-')
            self.assertEqual(doc['stored'], {store_ids[4]: {'copies': 0}})
        for doc in db.get_many(ids5):
            self.assertEqual(doc['_rev'][:2], '2-')
            self.assertEqual(doc['stored'], {})
        for doc in db.get_many(ids6):
            self.assertEqual(doc['_rev'][:2], '2-')
            self.assertEqual(doc['stored'], {})
        self.assertEqual(db.get_many(store_ids),
            [None, None, None, doc3, doc4, None, None]
        )

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
        store1 = {'_id': store_id1, 'type': 'dmedia/store'}
        store2 = {'_id': store_id2, 'type': 'dmedia/store'}
        store3 = {'_id': store_id3, 'type': 'dmedia/store'}

        # Test when empty:
        self.assertEqual(ms.purge_store(store_id1), 0)

        db.save_many([store1, store2, store3])
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
        self.assertEqual(ms.purge_store(random_id()), 0)
        for (old, new) in zip(docs, db.get_many(ids)):
            self.assertEqual(old, new)
        self.assertEqual(db.get_many([store_id1, store_id2, store_id3]),
            [store1, store2, store3]
        )

        # Purge the 3rd store, make sure dmedia/store doc is deleted even though
        # no files exist in the store:
        self.assertEqual(ms.purge_store(store_id3), 0)
        for (old, new) in zip(docs, db.get_many(ids)):
            self.assertEqual(old, new)
        self.assertEqual(db.get_many([store_id1, store_id2, store_id3]),
            [store1, store2, None]
        )

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
        self.assertEqual(db.get_many([store_id1, store_id2, store_id3]),
            [None, store2, None]
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
        self.assertEqual(db.get_many([store_id1, store_id2, store_id3]),
            [None, None, None]
        )

        # Make sure purging both again causes no change:
        docs = db.get_many(ids)
        self.assertEqual(ms.purge_store(store_id1), 0)
        self.assertEqual(ms.purge_store(store_id2), 0)
        for (old, new) in zip(docs, db.get_many(ids)):
            self.assertEqual(old, new)
        self.assertEqual(db.get_many([store_id1, store_id2, store_id3]),
            [None, None, None]
        )

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
        self.assertEqual(db.get_many([store_id1, store_id2, store_id3]),
            [None, None, None]
        )

        # Again, make sure purging both again causes no change:
        docs = db.get_many(ids)
        self.assertEqual(ms.purge_store(store_id1), 0)
        self.assertEqual(ms.purge_store(store_id2), 0)
        for (old, new) in zip(docs, db.get_many(ids)):
            self.assertEqual(old, new)
        self.assertEqual(db.get_many([store_id1, store_id2, store_id3]),
            [None, None, None]
        )

    def test_purge_all(self):    
        db = util.get_db(self.env, True)
        ms = metastore.MetaStore(db)
        
        # Test when empty:
        self.assertEqual(ms.purge_all(), 0)

        store_id1 = random_id()
        store_id2 = random_id()
        store_id3 = random_id()
        stores = [
            {'_id': _id, 'type': 'dmedia/store'}
            for _id in (store_id1, store_id2, store_id3)
        ]
        db.save_many(stores)
        docs = []

        # In 3 stores:
        for i in range(20):
            doc = {
                '_id': random_file_id(),
                'type': 'dmedia/file',
                'stored': {
                    store_id1: {'copies': 1, 'mtime': 1234567890},
                    store_id2: {'copies': 1, 'mtime': 1234567890},
                    store_id1: {'copies': 1, 'mtime': 1234567890},
                },
            }
            docs.append(doc)

        # In 2 stores:
        for i in range(20):
            doc = {
                '_id': random_file_id(),
                'type': 'dmedia/file',
                'stored': {
                    store_id1: {'copies': 1, 'mtime': 1234567890},
                    store_id2: {'copies': 1, 'mtime': 1234567890},
                },
            }
            docs.append(doc)

        # In 1 store:
        for i in range(20):
            doc = {
                '_id': random_file_id(),
                'type': 'dmedia/file',
                'stored': {
                    store_id1: {'copies': 1, 'mtime': 1234567890},
                },
            }
            docs.append(doc)

        # doc['stored'] is empty:
        for i in range(20):
            doc = {
                '_id': random_file_id(),
                'type': 'dmedia/file',
                'stored': {},
            }
            docs.append(doc)

        # doc['stored'] is missing:
        for i in range(20):
            doc = {
                '_id': random_file_id(),
                'type': 'dmedia/file',
            }
            docs.append(doc)

        ids = [d['_id'] for d in docs]
        db.save_many(docs)
        self.assertEqual(ms.purge_all(), 100)
        self.assertEqual(db.get_many([store_id1, store_id2, store_id3]),
            [None, None, None]
        )
        for doc in db.get_many(ids):
            self.assertTrue(doc['_rev'].startswith('2-'))
            self.assertEqual(doc['stored'], {})

    def test_scan(self):
        db = util.get_db(self.env, True)
        ms = metastore.MetaStore(db)
        fs = TempFileStore()
        db.save(fs.doc)

        # A few good files
        good = [create_random_file(fs, db) for i in range(45)]

        # A few files with bad mtime
        bad_mtime = [create_random_file(fs, db) for i in range(20)]
        for doc in bad_mtime:
            value = doc['stored'][fs.id]
            value['mtime'] -= 100
            value['verified'] = 1234567890
            value['pinned'] = True
        db.save_many(bad_mtime)

        # A few files with bad size
        bad_size = [create_random_file(fs, db) for i in range(30)]
        for doc in bad_size:
            doc['bytes'] += 1776
        db.save_many(bad_size)

        # A few missing files
        missing = [create_random_file(fs, db) for i in range(15)]
        for doc in missing:
            fs.remove(doc['_id'])

        # Note that MetaStore.scan() gets 50 docs at a time, so we need to test
        # roughly 100 docs to make sure the skip value is correctly adjusted
        # when files with the wrong size get marked as corrupt, moving them
        # out of the file/stored view:
        self.assertEqual(ms.scan(fs), 110)

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
        fs = TempFileStore()

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
                        'copies': 0,
                        'mtime': get_mtime(fs, _id),
                    },
                }
            )
        self.assertEqual(ms.relink(fs), 0)

    def test_remove(self):
        db = util.get_db(self.env, True)
        ms = metastore.MetaStore(db)
        tmp = TempDir()
        fs1 = TempFileStore()
        fs2 = TempFileStore()

        (file, ch) = tmp.random_file()
        self.assertEqual(fs1.import_file(open(file.name, 'rb')), ch)
        self.assertEqual(fs2.import_file(open(file.name, 'rb')), ch)

        # Test when file doc isn't in dmedia-1
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
 
    def test_copy(self):
        db = util.get_db(self.env, True)
        log_db = db.database('log-1')
        self.assertTrue(log_db.ensure())
        ms = metastore.MetaStore(db, log_db)
        fs1 = TempFileStore()
        fs2 = TempFileStore()
        fs3 = TempFileStore()

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
        timestamp = ret['corrupt'][fs1.id]['time']
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
                    fs1.id: {'time': timestamp}
                }
            }
        )

        # Now check log doc:
        rows = log_db.get('_all_docs')['rows']
        self.assertEqual(len(rows), 1)
        log = log_db.get(rows[0]['id'])
        self.assertTrue(isdb32(log['_id']))
        self.assertEqual(len(log['_id']), 24)
        self.assertEqual(log['_rev'][:2], '1-')
        self.assertEqual(log,
            {
                '_id': log['_id'],
                '_rev': log['_rev'],
                'time': timestamp,
                'type': 'dmedia/file/corrupt',
                'machine_id': self.env['machine_id'],
                'file_id': _id,
                'store_id': fs1.id,
                
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

    def test_verify(self):
        db = util.get_db(self.env, True)
        log_db = db.database('log-1')
        log_db.ensure()
        ms = metastore.MetaStore(db, log_db)
        tmp = TempDir()
        fs = TempFileStore()
        (file, ch) = tmp.random_file()

        # Test when doc is missing:
        with self.assertRaises(microfiber.NotFound) as cm:
            ms.verify(fs, ch.id)

        # Test when file and doc are present
        self.assertEqual(fs.import_file(open(file.name, 'rb')), ch)
        stored = create_stored(ch.id, fs)
        doc = schema.create_file(time.time(), ch, stored)
        db.save(doc)
        self.assertEqual(ms.verify(fs, ch.id), ch)
        doc = db.get(ch.id)
        self.assertEqual(doc['_rev'][:2], '2-')
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

        # Test when file is missing:
        canonical = fs.path(ch.id)
        os.remove(canonical)
        self.assertIsNone(ms.verify(fs, ch.id))
        doc = db.get(ch.id)
        self.assertEqual(doc['_rev'][:2], '3-')
        schema.check_file(doc)
        self.assertEqual(doc['stored'], {})

        # Test when file is in FileStore, but not in doc['stored']:
        shutil.copyfile(file.name, canonical)
        self.assertEqual(ms.verify(fs, ch.id), ch)
        doc = db.get(ch.id)
        self.assertEqual(doc['_rev'][:2], '4-')
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

        # Test when file is corrupt:
        self.assertEqual(log_db.get('_all_docs')['rows'], [])
        fp = open(canonical, 'rb+')
        fp.write(os.urandom(16))
        fp.close()
        start = time.time()
        self.assertIsNone(ms.verify(fs, ch.id))
        end = time.time()
        doc = db.get(ch.id)
        self.assertEqual(doc['_rev'][:2], '5-')
        schema.check_file(doc)
        self.assertEqual(doc['stored'], {})
        self.assertEqual(set(doc['corrupt']), set([fs.id]))
        self.assertEqual(set(doc['corrupt'][fs.id]), set(['time']))
        timestamp = doc['corrupt'][fs.id]['time']
        self.assertIsInstance(timestamp, float)
        self.assertTrue(start <= timestamp <= end)
        self.assertFalse(path.exists(canonical))
        self.assertTrue(path.isfile(fs.corrupt_path(ch.id)))

        # Now check log doc:
        rows = log_db.get('_all_docs')['rows']
        self.assertEqual(len(rows), 1)
        log = log_db.get(rows[0]['id'])
        self.assertTrue(isdb32(log['_id']))
        self.assertEqual(len(log['_id']), 24)
        self.assertEqual(log['_rev'][:2], '1-')
        self.assertEqual(log,
            {
                '_id': log['_id'],
                '_rev': log['_rev'],
                'time': timestamp,
                'type': 'dmedia/file/corrupt',
                'machine_id': self.env['machine_id'],
                'file_id': ch.id,
                'store_id': fs.id,
                
            }
        )

    def test_verify_by_downgraded(self):
        db = util.get_db(self.env, True)
        ms = metastore.MetaStore(db)
        fs = TempFileStore()

        # Test when empty:
        self.assertEqual(ms.verify_by_downgraded(fs), (0, 0))

        # Test when no files are downgraded:
        docs = [create_random_file(fs, db) for i in range(3)]
        ids = [d['_id'] for d in docs]
        self.assertEqual(ms.verify_by_downgraded(fs), (0, 0))
        self.assertEqual(db.get_many(ids), docs)
        for doc in docs:
            self.assertTrue(doc['_rev'].startswith('1-'))
            self.assertEqual(set(doc['stored']), set([fs.id]))
            self.assertEqual(
                set(doc['stored'][fs.id]),
                set(['copies', 'mtime'])
            )
            self.assertEqual(doc['stored'][fs.id]['copies'], 1)
            doc['stored'][fs.id]['copies'] = 0  # For next test

        # Test when all files are downgraded
        db.save_many(docs)
        start_time = int(time.time())
        self.assertEqual(ms.verify_by_downgraded(fs),
            (3, sum(d['bytes'] for d in docs))
        )
        end_time = int(time.time())
        docs = db.get_many(ids)
        for doc in docs:
            self.assertTrue(doc['_rev'].startswith('3-'), doc['_rev'])
            self.assertEqual(set(doc['stored']), set([fs.id]))
            self.assertEqual(
                set(doc['stored'][fs.id]),
                set(['copies', 'mtime', 'verified'])
            )
            self.assertEqual(doc['stored'][fs.id]['copies'], 1)
            self.assertTrue(
                start_time <= doc['stored'][fs.id]['verified'] <= end_time
            ) 
            doc['stored'][fs.id]['copies'] = 0  # For next test

        # Test that a numeric 'verified' excludes files with {'copies': 0}:
        db.save_many(docs)
        self.assertEqual(ms.verify_by_downgraded(fs), (0, 0))
        self.assertEqual(db.get_many(ids), docs)

        # Test when just one doc needs to be verified:
        (doc1, doc2, doc3) = docs
        doc2['stored'][fs.id]['verified'] = '1776'
        self.assertTrue(doc2['_rev'].startswith('4-'), doc2['_rev'])
        db.save(doc2)
        start_time = int(time.time())
        self.assertEqual(ms.verify_by_downgraded(fs), (1, doc2['bytes']))
        end_time = int(time.time())
        self.assertEqual(db.get(doc1['_id']), doc1)
        self.assertEqual(db.get(doc3['_id']), doc3)
        doc2 = db.get(doc2['_id'])
        self.assertTrue(doc2['_rev'].startswith('6-'), doc2['_rev'])
        self.assertEqual(set(doc2['stored']), set([fs.id]))
        self.assertEqual(
            set(doc2['stored'][fs.id]),
            set(['copies', 'mtime', 'verified'])
        )
        self.assertEqual(doc2['stored'][fs.id]['copies'], 1)
        self.assertTrue(
            start_time <= doc2['stored'][fs.id]['verified'] <= end_time
        )

    def test_verify_by_mtime(self):
        db = util.get_db(self.env, True)
        ms = metastore.MetaStore(db)
        fs = TempFileStore()

        # Test when empty:
        self.assertEqual(ms.verify_by_mtime(fs), (0, 0))

        # Test when no files need to be verified:
        docs = [create_random_file(fs, db) for i in range(6)]
        ids = [d['_id'] for d in docs]
        self.assertEqual(ms.verify_by_mtime(fs), (0, 0))
        self.assertEqual(db.get_many(ids), docs)
        for doc in docs:
            self.assertTrue(doc['_rev'].startswith('1-'))
            self.assertEqual(set(doc['stored']), set([fs.id]))
            self.assertEqual(
                set(doc['stored'][fs.id]),
                set(['copies', 'mtime'])
            )
            self.assertEqual(doc['stored'][fs.id]['copies'], 1)

        # Again test when no files need to be verified, but to the second: 
        curtime = int(time.time())
        base = curtime - metastore.VERIFY_BY_MTIME + 1
        for (i, doc) in enumerate(docs):
            doc['stored'][fs.id]['mtime'] = base + i
        db.save_many(docs)
        self.assertEqual(ms.verify_by_mtime(fs, curtime), (0, 0))
        self.assertEqual(db.get_many(ids), docs)

        # Test when the first 4 files need to be verified:
        start_time = int(time.time())
        self.assertEqual(ms.verify_by_mtime(fs, curtime + 4),
            (4, sum(d['bytes'] for d in docs[:4]))
        )
        end_time = int(time.time())
        docs = db.get_many(ids)
        for doc in docs[:4]:
            self.assertTrue(doc['_rev'].startswith('3-'))
            self.assertEqual(set(doc['stored']), set([fs.id]))
            self.assertEqual(
                set(doc['stored'][fs.id]),
                set(['copies', 'mtime', 'verified'])
            )
            self.assertEqual(doc['stored'][fs.id]['copies'], 1)
            verified = doc['stored'][fs.id]['verified']
            self.assertIsInstance(verified, int)
            self.assertTrue(start_time <= verified <= end_time)
        for doc in docs[4:]:
            self.assertTrue(doc['_rev'].startswith('2-'))

        # Test when the last 2 files need to be verified:
        start_time = int(time.time())
        self.assertEqual(ms.verify_by_mtime(fs, curtime + 6),
            (2, sum(d['bytes'] for d in docs[4:]))
        )
        end_time = int(time.time())
        docs = db.get_many(ids)
        for doc in docs[:4]:
            self.assertTrue(doc['_rev'].startswith('3-'))
        for doc in docs[4:]:
            self.assertTrue(doc['_rev'].startswith('3-'))
            self.assertEqual(set(doc['stored']), set([fs.id]))
            self.assertEqual(
                set(doc['stored'][fs.id]),
                set(['copies', 'mtime', 'verified'])
            )
            self.assertEqual(doc['stored'][fs.id]['copies'], 1)
            verified = doc['stored'][fs.id]['verified']
            self.assertIsInstance(verified, int)
            self.assertTrue(start_time <= verified <= end_time)

        # None should need to be verified now:
        self.assertEqual(ms.verify_by_mtime(fs, curtime + 7), (0, 0))
        self.assertEqual(db.get_many(ids), docs)
 
    def test_verify_by_verified(self):
        db = util.get_db(self.env, True)
        ms = metastore.MetaStore(db)
        fs = TempFileStore()

        # Test when empty:
        self.assertEqual(ms.verify_by_verified(fs), (0, 0))

        # Test when no files need to be verified:
        docs = [create_random_file(fs, db) for i in range(6)]
        ids = [d['_id'] for d in docs]
        self.assertEqual(ms.verify_by_verified(fs), (0, 0))
        self.assertEqual(db.get_many(ids), docs)
        for doc in docs:
            self.assertTrue(doc['_rev'].startswith('1-'))
            self.assertEqual(set(doc['stored']), set([fs.id]))
            self.assertEqual(
                set(doc['stored'][fs.id]),
                set(['copies', 'mtime'])
            )
            self.assertEqual(doc['stored'][fs.id]['copies'], 1)

        # Again test when no files need to be verified, but to the second: 
        curtime = int(time.time())
        base = curtime - metastore.VERIFY_BY_VERIFIED + 1
        for (i, doc) in enumerate(docs):
            doc['stored'][fs.id]['verified'] = base + i
        db.save_many(docs)
        self.assertEqual(ms.verify_by_verified(fs, curtime), (0, 0))
        self.assertEqual(db.get_many(ids), docs)

        # Test when the first 4 files need to be verified:
        start_time = int(time.time())
        self.assertEqual(ms.verify_by_verified(fs, curtime + 4),
            (4, sum(d['bytes'] for d in docs[:4]))
        )
        end_time = int(time.time())
        docs = db.get_many(ids)
        for doc in docs[:4]:
            self.assertTrue(doc['_rev'].startswith('3-'))
            self.assertEqual(set(doc['stored']), set([fs.id]))
            self.assertEqual(
                set(doc['stored'][fs.id]),
                set(['copies', 'mtime', 'verified'])
            )
            self.assertEqual(doc['stored'][fs.id]['copies'], 1)
            verified = doc['stored'][fs.id]['verified']
            self.assertIsInstance(verified, int)
            self.assertTrue(start_time <= verified <= end_time)
        for doc in docs[4:]:
            self.assertTrue(doc['_rev'].startswith('2-'))

        # Test when the last 2 files need to be verified:
        start_time = int(time.time())
        self.assertEqual(ms.verify_by_verified(fs, curtime + 6),
            (2, sum(d['bytes'] for d in docs[4:]))
        )
        end_time = int(time.time())
        docs = db.get_many(ids)
        for doc in docs[:4]:
            self.assertTrue(doc['_rev'].startswith('3-'))
        for doc in docs[4:]:
            self.assertTrue(doc['_rev'].startswith('3-'))
            self.assertEqual(set(doc['stored']), set([fs.id]))
            self.assertEqual(
                set(doc['stored'][fs.id]),
                set(['copies', 'mtime', 'verified'])
            )
            self.assertEqual(doc['stored'][fs.id]['copies'], 1)
            verified = doc['stored'][fs.id]['verified']
            self.assertIsInstance(verified, int)
            self.assertTrue(start_time <= verified <= end_time)

        # None should need to be verified now:
        self.assertEqual(ms.verify_by_verified(fs, curtime + 7), (0, 0))
        self.assertEqual(db.get_many(ids), docs)

    def test_verify_all(self):
        db = util.get_db(self.env, True)
        ms = metastore.MetaStore(db)
        fs = TempFileStore()

        # Test when empty:
        self.assertEqual(ms.verify_all(fs), (0, 0))

        docs = [create_random_file(fs, db) for i in range(6)]
        ids = [d['_id'] for d in docs]
        curtime = int(time.time())
        base_mtime = curtime - metastore.VERIFY_BY_MTIME + 1
        base_verified = curtime - metastore.VERIFY_BY_VERIFIED + 1

        # VERIFY_BY_MTIME threshold:
        for (i, doc) in enumerate(docs):
            doc['stored'][fs.id]['mtime'] = base_mtime + i
        db.save_many(docs)
        self.assertEqual(ms.verify_all(fs, curtime), (0, 0))
        for doc in db.get_many(ids):
            self.assertTrue(doc['_rev'].startswith('2-'))

        self.assertEqual(ms.verify_all(fs, curtime + 4),
            (4, sum(d['bytes'] for d in docs[:4]))
        )
        docs = db.get_many(ids)
        for doc in docs[:4]:
            self.assertTrue(doc['_rev'].startswith('3-'))
            verified = doc['stored'][fs.id]['verified']
            self.assertIsInstance(verified, int)
            self.assertGreaterEqual(verified, curtime)
        for doc in docs[4:]:
            self.assertTrue(doc['_rev'].startswith('2-'))

        self.assertEqual(ms.verify_all(fs, curtime + 6),
            (2, sum(d['bytes'] for d in docs[4:]))
        )
        docs = db.get_many(ids)
        for doc in docs:
            self.assertTrue(doc['_rev'].startswith('3-'))
            verified = doc['stored'][fs.id]['verified']
            self.assertIsInstance(verified, int)
            self.assertGreaterEqual(verified, curtime)

        self.assertEqual(ms.verify_all(fs, curtime + 6), (0, 0))
        for doc in db.get_many(ids):
            self.assertTrue(doc['_rev'].startswith('3-'))

        # VERIFY_BY_VERIFIED threshold:
        for (i, doc) in enumerate(docs):
            doc['stored'][fs.id]['verified'] = base_verified + i
        db.save_many(docs)
        self.assertEqual(ms.verify_all(fs, curtime), (0, 0))
        for doc in db.get_many(ids):
            self.assertTrue(doc['_rev'].startswith('4-'))

        self.assertEqual(ms.verify_all(fs, curtime + 2),
            (2, sum(d['bytes'] for d in docs[:2]))
        )
        docs = db.get_many(ids)
        for doc in docs[:2]:
            self.assertTrue(doc['_rev'].startswith('5-'))
            verified = doc['stored'][fs.id]['verified']
            self.assertIsInstance(verified, int)
            self.assertGreaterEqual(verified, curtime)
        for doc in docs[2:]:
            self.assertTrue(doc['_rev'].startswith('4-'))

        self.assertEqual(ms.verify_all(fs, curtime + 6),
            (4, sum(d['bytes'] for d in docs[2:]))
        )
        docs = db.get_many(ids)
        for doc in docs:
            self.assertTrue(doc['_rev'].startswith('5-'))
            verified = doc['stored'][fs.id]['verified']
            self.assertIsInstance(verified, int)
            self.assertGreaterEqual(verified, curtime)

        self.assertEqual(ms.verify_all(fs, curtime + 6), (0, 0))
        for doc in db.get_many(ids):
            self.assertTrue(doc['_rev'].startswith('5-'))

        # Both thresholds:
        for (i, doc) in enumerate(docs):
            if i % 2 == 0:
                doc['stored'][fs.id]['verified'] = base_verified + (i // 2)
            else:
                del doc['stored'][fs.id]['verified']
                doc['stored'][fs.id]['mtime'] = base_mtime + (i // 2)
        db.save_many(docs)
        self.assertEqual(ms.verify_all(fs, curtime), (0, 0))
        for doc in db.get_many(ids):
            self.assertTrue(doc['_rev'].startswith('6-'))
        self.assertEqual(ms.verify_all(fs, curtime + 1),
            (2, docs[0]['bytes'] + docs[1]['bytes'])
        )
        self.assertEqual(ms.verify_all(fs, curtime + 2),
            (2, docs[2]['bytes'] + docs[3]['bytes'])
        )
        self.assertEqual(ms.verify_all(fs, curtime + 3),
            (2, docs[4]['bytes'] + docs[5]['bytes'])
        )
        self.assertEqual(ms.verify_all(fs, curtime + 100), (0, 0))
        docs = db.get_many(ids)
        for doc in docs:
            self.assertTrue(doc['_rev'].startswith('7-'))

        # All downgraded files should be verified, no matter the timestamps:
        curtime = int(time.time())
        for doc in docs:
            doc['stored'][fs.id]['mtime'] = curtime
            doc['stored'][fs.id]['copies'] = 0
            del doc['stored'][fs.id]['verified']
        db.save_many(docs)
        start_time = int(time.time())
        self.assertEqual(ms.verify_all(fs),
            (6, sum(d['bytes'] for d in docs))
        )
        end_time = int(time.time())
        docs = db.get_many(ids)
        for doc in docs:
            self.assertTrue(doc['_rev'].startswith('9-'))
            copies = doc['stored'][fs.id]['copies']
            self.assertIsInstance(copies, int)
            self.assertEqual(copies, 1)
            verified = doc['stored'][fs.id]['verified']
            self.assertIsInstance(verified, int)
            self.assertTrue(start_time <= verified <= end_time)
        self.assertEqual(ms.verify_all(fs), (0, 0))
        self.assertEqual(db.get_many(ids), docs)

    def test_content_md5(self):
        db = util.get_db(self.env, True)
        ms = metastore.MetaStore(db)
        fs = TempFileStore()

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
        fs1 = TempFileStore()
        fs2 = TempFileStore()

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

    def test_iter_fragile(self):
        db = util.get_db(self.env, True)
        ms = metastore.MetaStore(db)

        # Test when no files are in the library:
        self.assertEqual(list(ms.iter_fragile()), [])

        # Create rank=(0 through 6) test data:
        ids = tuple(random_file_id() for i in range(7))
        stores = tuple(random_id() for i in range(3))
        docs = [
            {
                '_id': ids[0],
                'type': 'dmedia/file',
                'origin': 'user',
                'stored': {},
            },
            {
                '_id': ids[1],
                'type': 'dmedia/file',
                'origin': 'user',
                'stored': {
                    stores[0]: {'copies': 0},
                },
            },
            {
                '_id': ids[2],
                'type': 'dmedia/file',
                'origin': 'user',
                'stored': {
                    stores[0]: {'copies': 1},
                },
            },
            {
                '_id': ids[3],
                'type': 'dmedia/file',
                'origin': 'user',
                'stored': {
                    stores[0]: {'copies': 1},
                    stores[1]: {'copies': 0},
                },
            },
            {
                '_id': ids[4],
                'type': 'dmedia/file',
                'origin': 'user',
                'stored': {
                    stores[0]: {'copies': 1},
                    stores[1]: {'copies': 1},
                },
            },
            {
                '_id': ids[5],
                'type': 'dmedia/file',
                'origin': 'user',
                'stored': {
                    stores[0]: {'copies': 1},
                    stores[1]: {'copies': 1},
                    stores[2]: {'copies': 0},
                },
            },
            {
                '_id': ids[6],
                'type': 'dmedia/file',
                'origin': 'user',
                'stored': {
                    stores[0]: {'copies': 1},
                    stores[1]: {'copies': 1},
                    stores[2]: {'copies': 1},
                },
            },
        ]
        db.save_many(docs)

        # We should get docs[0:6]:
        self.assertEqual(list(ms.iter_fragile()), docs[0:-1])

        # Docs should not be changed:
        self.assertEqual(db.get_many(ids), docs)

    def test_iter_actionable_fragile(self):
        db = util.get_db(self.env, True)
        ms = metastore.MetaStore(db)

        store_id1 = random_id()
        store_id2 = random_id()
        store_id3 = random_id()
        store_id4 = random_id()
        empty = frozenset()
        one = frozenset([store_id1])
        two = frozenset([store_id1, store_id2])
        three = frozenset([store_id1, store_id2, store_id3])

        id1 = random_file_id()
        id2 = random_file_id()
        id3 = random_file_id()
        doc1 = {
            '_id': id1,
            'type': 'dmedia/file',
            'origin': 'user',
            'stored': {
                store_id1: {'copies': 0},
            },
        }
        doc2 = {
            '_id': id2,
            'type': 'dmedia/file',
            'origin': 'user',
            'stored': {
                store_id1: {'copies': 0},
                store_id4: {'copies': 1},
            },
        }
        doc3 = {
            '_id': id3,
            'type': 'dmedia/file',
            'origin': 'user',
            'stored': {
                store_id1: {'copies': 1},
                store_id2: {'copies': 1},
            },
        }

        # Test when no files are in the library:
        self.assertEqual(list(ms.iter_actionable_fragile(empty)), [])
        self.assertEqual(list(ms.iter_actionable_fragile(one)), [])
        self.assertEqual(list(ms.iter_actionable_fragile(two)), [])
        self.assertEqual(list(ms.iter_actionable_fragile(three)), [])

        # All 3 docs should be included:
        db.save_many([doc1, doc2, doc3])
        self.assertEqual(list(ms.iter_actionable_fragile(three)), [
            (doc1, set([store_id1])),
            (doc2, set([store_id1, store_id4])),
            (doc3, set([store_id1, store_id2])),
        ])

        # If only store_id1, store_id2 are connected, doc3 shouldn't be
        # actionable:
        self.assertEqual(list(ms.iter_actionable_fragile(two)), [
            (doc1, set([store_id1])),
            (doc2, set([store_id1, store_id4])),
        ])

        # All files have a copy in store_id1, so nothing should be returned:
        self.assertEqual(list(ms.iter_actionable_fragile(one)), [])

        # If doc2 was only stored on a non-connected store:
        doc1['stored'] = {
            store_id4: {'copies': 1},
        }
        db.save(doc1)
        self.assertEqual(list(ms.iter_actionable_fragile(one)), [
            (doc1, set([store_id4]))
        ])

        # If doc2 has sufficent durablity, it shouldn't be included, even though
        # there is a free drive where a copy could be created:
        doc2['stored'] = {
            store_id1: {'copies': 1},
            store_id2: {'copies': 1},
            store_id4: {'copies': 1},
        }
        db.save(doc2)
        self.assertEqual(list(ms.iter_actionable_fragile(three)), [
            (doc1, set([store_id4])),
            (doc3, set([store_id1, store_id2])),
        ])

    def test_reclaim(self):
        # FIXME: Till we have a nice way of mocking FileStore.statvfs(), this is
        # a lame test that covers gross function without doing anything real:
        db = util.get_db(self.env, True)
        ms = metastore.MetaStore(db)
        fs = TempFileStore()
        self.assertEqual(ms.reclaim(fs), (0, 0))

    def test_reclaim_all(self):
        # FIXME: Till we have a nice way of mocking FileStore.statvfs(), this is
        # a lame test that covers gross function without doing anything real:
        db = util.get_db(self.env, True)
        ms = metastore.MetaStore(db)
        self.assertEqual(ms.reclaim_all(), (0, 0, 0))
