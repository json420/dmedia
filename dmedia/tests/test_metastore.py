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
from dbase32 import random_id
import microfiber
from microfiber import dumps, Conflict

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

