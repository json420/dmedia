# Authors:
#   Jason Gerard DeRose <jderose@novacut.com>
#   David Green <david4dev@gmail.com>
#
# dmedia: distributed media library
# Copyright (C) 2011 Jason Gerard DeRose <jderose@novacut.com>
#
# This file is part of `dmedia`.
#
# `dmedia` is free software: you can redistribute it and/or modify it under the
# terms of the GNU Affero General Public License as published by the Free
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

"""
Unit tests for `dmedia.core` module.
"""

from unittest import TestCase
import json
import os
from os import path
import time
from copy import deepcopy
from base64 import b64encode
import multiprocessing
from collections import OrderedDict

import microfiber
from dbase32 import random_id
import filestore
from filestore import FileStore
from filestore.misc import TempFileStore
from filestore.migration import Migration, b32_to_db32
from usercouch.misc import CouchTestCase

from dmedia.local import LocalStores
from dmedia.metastore import MetaStore, get_mtime
from dmedia.schema import DB_NAME, create_filestore, project_db_name
from dmedia import util, core

from .couch import CouchCase
from .base import TempDir, random_file_id, write_random


class TestFunctions(TestCase):
    def test_has_thumbnail(self):
        self.assertIs(core.has_thumbnail({}), False)
        doc = {'_attachments': {}}
        self.assertIs(core.has_thumbnail(doc), False)
        doc = {'_attachments': {'foo': 'bar'}}
        self.assertIs(core.has_thumbnail(doc), False)
        doc = {'_attachments': {'thumbnail': 'yup'}}
        self.assertIs(core.has_thumbnail(doc), True)

    def test_encode_attachment(self):
        data = os.urandom(1776)
        self.assertEqual(
            core.encode_attachment('image/jpeg', data),
            {
                'content_type': 'image/jpeg',
                'data': b64encode(data).decode('utf-8'),
            }
        )


class TestCouchFunctions(CouchCase):
    def test_db_dump_iter(self):
        server = microfiber.Server(self.env)
        self.assertEqual(list(core.db_dump_iter(server)), [])
        server.put(None, 'thumbnails')
        self.assertEqual(list(core.db_dump_iter(server)), [])
        server.put(None, 'foo')
        self.assertEqual(list(core.db_dump_iter(server)), ['foo'])
        server.put(None, 'bar')
        self.assertEqual(list(core.db_dump_iter(server)), ['bar', 'foo'])

    def test_projects_iter(self):
        server = microfiber.Server(self.env)
        self.assertEqual(list(core.projects_iter(server)), [])
        ids = tuple(random_id() for i in range(20))
        for _id in ids:
            db_name = project_db_name(_id)
            server.put(None, db_name)
        self.assertEqual(
            list(core.projects_iter(server)),
            [(project_db_name(_id), _id) for _id in sorted(ids)]
        )

    def test_mark_machine_start(self):
        doc = {}
        atime = int(time.time())
        self.assertIsNone(core.mark_machine_start(doc, atime))
        self.assertEqual(doc, {
            'atime': atime,
            'stores': {},
            'peers': {},
        })

    def test_mark_add_filestore(self):
        doc = {}
        atime = int(time.time())
        fs = TempFileStore()
        info = {'parentdir': fs.parentdir}
        self.assertIsNone(core.mark_add_filestore(doc, atime, fs.id, info))
        self.assertEqual(doc, {
            'atime': atime,
            'stores': {
                fs.id: {'parentdir': fs.parentdir},
            }
        })

    def test_mark_remove_filestore(self):
        doc = {}
        atime = int(time.time())
        fs1 = TempFileStore()
        fs2 = TempFileStore()
        self.assertIsNone(core.mark_remove_filestore(doc, atime, fs1.id))
        self.assertEqual(doc, {
            'atime': atime,
            'stores': {},
        })

        doc = {
            'atime': atime - 123456,
            'stores': {
                fs1.id: {'parentdir': fs1.parentdir},
                fs2.id: {'parentdir': fs2.parentdir},
            },
        }
        self.assertIsNone(core.mark_remove_filestore(doc, atime, fs1.id))
        self.assertEqual(doc, {
            'atime': atime,
            'stores': {
                fs2.id: {'parentdir': fs2.parentdir},
            },
        })
        self.assertIsNone(core.mark_remove_filestore(doc, atime, fs2.id))
        self.assertEqual(doc, {
            'atime': atime,
            'stores': {},
        })

    def test_mark_connected_stores(self):
        atime = int(time.time())
        fs1 = TempFileStore()
        fs2 = TempFileStore()

        doc = {}
        stores = {
            fs1.id: {'parentdir': fs1.parentdir}
        }
        self.assertIsNone(core.mark_connected_stores(doc, atime, stores))
        self.assertEqual(doc, {
            'atime': atime,
            'stores': {
                fs1.id: {'parentdir': fs1.parentdir}
            },
        })
        self.assertIs(doc['stores'], stores)

        doc = {
            'atime': atime - 123456,
            'stores': {
                fs1.id: {'parentdir': fs1.parentdir},
                fs2.id: {'parentdir': fs2.parentdir},
            },
        }
        stores = {}
        self.assertIsNone(core.mark_connected_stores(doc, atime, stores))
        self.assertEqual(doc, {
            'atime': atime,
            'stores': {},
        })
        self.assertIs(doc['stores'], stores)

    def test_mark_add_peer(self):
        doc = {}
        atime = int(time.time())
        peer_id = random_id(30)
        url = random_id()
        info = {'url': url}
        self.assertIsNone(core.mark_add_peer(doc, atime, peer_id, info))
        self.assertEqual(doc, {
            'atime': atime,
            'peers': {
                peer_id: {'url': url},
            },
        })

    def test_mark_remove_peer(self):
        doc = {}
        atime = int(time.time())
        peer_id1 = random_id(30)
        url1 = random_id()
        peer_id2 = random_id(30)
        url2 = random_id()
        self.assertIsNone(core.mark_remove_peer(doc, atime, peer_id1))
        self.assertEqual(doc, {
            'atime': atime,
            'peers': {},
        })

        doc = {
            'atime': atime - 23456,
            'peers': {
                peer_id1: {'url': url1},
                peer_id2: {'url': url2},
            },
        }
        self.assertIsNone(core.mark_remove_peer(doc, atime, peer_id1))
        self.assertEqual(doc, {
            'atime': atime,
            'peers': {
                peer_id2: {'url': url2},
            },
        })
        self.assertIsNone(core.mark_remove_peer(doc, atime, peer_id2))
        self.assertEqual(doc, {
            'atime': atime,
            'peers': {},
        })


class TestVigilanceMocked(TestCase):
    def test_up_rank(self):
        class Mocked(core.Vigilance):
            def __init__(self, local, remote):
                self.local = frozenset(local)
                self.remote = frozenset(remote)
                self._calls = []

            def up_rank_by_verifying(self, doc, downgraded):
                self._calls.extend(('verify', doc, downgraded))
                return doc

            def up_rank_by_copying(self, doc, free, threshold):
                self._calls.extend(('copy', doc, free, threshold))
                return doc

            def up_rank_by_downloading(self, doc, remote, threshold):
                self._calls.extend(('download', doc, remote, threshold))
                return doc

        local = tuple(random_id() for i in range(2))
        remote = tuple(random_id() for i in range(2))
        mocked = Mocked(local, remote)

        # Verify, one local:
        doc = {
            'stored': {
                local[0]: {'copies': 0},
            },
        }
        self.assertIs(mocked.up_rank(doc, 17), doc)
        self.assertEqual(mocked._calls,
            ['verify', doc, {local[0]}]
        )

        # Verify, two local:
        doc = {
            'stored': {
                local[0]: {'copies': 0},
                local[1]: {'copies': 1},
            },
        }
        mocked._calls.clear()
        self.assertIs(mocked.up_rank(doc, 17), doc)
        self.assertEqual(mocked._calls,
            ['verify', doc, {local[0]}]
        )

        # Verify, one local, one remote:
        doc = {
            'stored': {
                local[0]: {'copies': 0},
                remote[0]: {'copies': 1},
            },
        }
        mocked._calls.clear()
        self.assertIs(mocked.up_rank(doc, 17), doc)
        self.assertEqual(mocked._calls,
            ['verify', doc, {local[0]}]
        )

        # Copy, one local, one remote:
        doc = {
            'stored': {
                local[0]: {'copies': 1},
                remote[0]: {'copies': 1},
            },
        }
        mocked._calls.clear()
        self.assertIs(mocked.up_rank(doc, 17), doc)
        self.assertEqual(mocked._calls,
            ['copy', doc, {local[1]}, 17]
        )

        # Copy, two local, one remote:
        doc = {
            'stored': {
                local[0]: {'copies': 1},
                local[1]: {'copies': 1},
                remote[0]: {'copies': 1},
            },
        }
        mocked._calls.clear()
        self.assertIsNone(mocked.up_rank(doc, 17))
        self.assertEqual(mocked._calls, [])

        # Download, one remote:
        doc = {
            'stored': {
                remote[0]: {'copies': 0},
            },
        }
        mocked._calls.clear()
        self.assertIs(mocked.up_rank(doc, 17), doc)
        self.assertEqual(mocked._calls,
            ['download', doc, {remote[0]}, 17]
        )

        # Download, two remote:
        doc = {
            'stored': {
                remote[0]: {'copies': 0},
                remote[1]: {'copies': 1},
            },
        }
        mocked._calls.clear()
        self.assertIs(mocked.up_rank(doc, 17), doc)
        self.assertEqual(mocked._calls,
            ['download', doc, set(remote), 17]
        )

        # Available in neither local nor remote:
        doc = {
            'stored': {
                random_id(): {'copies': 0},
                random_id(): {'copies': 1},
            },
        }
        mocked._calls.clear()
        self.assertIsNone(mocked.up_rank(doc, 17))
        self.assertEqual(mocked._calls, [])

        # Empty doc['stored']:
        doc = {'stored': {}}
        mocked._calls.clear()
        self.assertIsNone(mocked.up_rank(doc, 17))
        self.assertEqual(mocked._calls, [])


class TestVigilance(CouchCase):
    def test_init(self):
        db = util.get_db(self.env, True)
        ms = MetaStore(db)
        inst = core.Vigilance(ms, None)
        self.assertIs(inst.ms, ms)
        self.assertIsInstance(inst.stores, LocalStores)
        self.assertIsInstance(inst.local, frozenset)
        self.assertEqual(inst.local, frozenset())
        self.assertIsInstance(inst.remote, frozenset)
        self.assertEqual(inst.remote, frozenset())
        self.assertEqual(inst.clients, {})
        self.assertEqual(inst.store_to_client, {})


class TestTaskQueue(TestCase):
    def test_init(self):
        tq = core.TaskQueue()
        self.assertIs(tq.running, False)
        self.assertIsNone(tq.task)
        self.assertIsInstance(tq.pending, OrderedDict)
        self.assertEqual(tq.pending, OrderedDict())

    def test_append(self):
        tq = core.TaskQueue()

        tq.append('a', 'aye', 0)
        self.assertEqual(tq.pending, OrderedDict([
            ('a', ('aye', 0)),
        ]))
        self.assertEqual(list(tq.pending), ['a'])

        tq.append('b', 'bee', 0)
        self.assertEqual(tq.pending, OrderedDict([
            ('a', ('aye', 0)),
            ('b', ('bee', 0)),
        ]))
        self.assertEqual(list(tq.pending), ['a', 'b'])

        tq.append('c', 'see', 0)
        self.assertEqual(tq.pending, OrderedDict([
            ('a', ('aye', 0)),
            ('b', ('bee', 0)),
            ('c', ('see', 0)),
        ]))
        self.assertEqual(list(tq.pending), ['a', 'b', 'c'])

        tq.append('a', 'aye', 1)
        self.assertEqual(tq.pending, OrderedDict([
            ('a', ('aye', 1)),
            ('b', ('bee', 0)),
            ('c', ('see', 0)),
        ]))
        self.assertEqual(list(tq.pending), ['a', 'b', 'c'])

        tq.append('d', 'dee', 0)
        self.assertEqual(tq.pending, OrderedDict([
            ('a', ('aye', 1)),
            ('b', ('bee', 0)),
            ('c', ('see', 0)),
            ('d', ('dee', 0)),
        ]))
        self.assertEqual(list(tq.pending), ['a', 'b', 'c', 'd'])

        tq.append('c', 'see', 1)
        self.assertEqual(tq.pending, OrderedDict([
            ('a', ('aye', 1)),
            ('b', ('bee', 0)),
            ('c', ('see', 1)),
            ('d', ('dee', 0)),
        ]))
        self.assertEqual(list(tq.pending), ['a', 'b', 'c', 'd'])

        tq.append('a', 'aye', 2)
        self.assertEqual(tq.pending, OrderedDict([
            ('a', ('aye', 2)),
            ('b', ('bee', 0)),
            ('c', ('see', 1)),
            ('d', ('dee', 0)),
        ]))
        self.assertEqual(list(tq.pending), ['a', 'b', 'c', 'd'])
        self.assertEqual(tq.popitem(), ('a', ('aye', 2)))


class TestCore(CouchTestCase):
    def create(self):
        self.machine_id = random_id(30)
        self.user_id = random_id(30)
        self.machine = {'_id': self.machine_id}
        self.user = {'_id': self.user_id}
        return core.Core(self.env, self.machine, self.user)

    def test_init(self):
        machine_id = random_id(30)
        user_id = random_id(30)
        machine = {'_id': machine_id}
        user = {'_id': user_id}

        inst = core.Core(self.env, machine, user)
        self.assertIs(inst.env, self.env)
        self.assertEqual(inst.env['machine_id'], machine_id)
        self.assertEqual(inst.env['user_id'], user_id)
        self.assertIsInstance(inst.db, microfiber.Database)
        self.assertEqual(inst.db.name, 'dmedia-1')
        self.assertIsInstance(inst.log_db, microfiber.Database)
        self.assertEqual(inst.log_db.name, 'log-1')
        self.assertIsInstance(inst.server, microfiber.Server)
        self.assertIsInstance(inst.ms, MetaStore)
        self.assertIs(inst.ms.db, inst.db)
        self.assertIsInstance(inst.stores, LocalStores)
        self.assertIsInstance(inst.task_manager, core.TaskManager)
        self.assertIsNone(inst.ssl_config)
        self.assertEqual(inst.db.get('_local/dmedia'), {
            '_id': '_local/dmedia',
            '_rev': '0-1',
            'machine_id': machine_id,
            'user_id': user_id,
        })
        self.assertIs(inst.machine, machine)
        self.assertEqual(inst.db.get(machine_id), machine)
        self.assertEqual(inst.machine['_rev'][:2], '1-')
        self.assertEqual(inst.machine['stores'], {})
        self.assertEqual(inst.machine['peers'], {})
        self.assertIs(inst.user, user)
        self.assertEqual(inst.db.get(user_id), user)
        self.assertEqual(inst.user['_rev'][:2], '1-')

        ssl_config = random_id()
        inst = core.Core(self.env, machine, user, ssl_config)
        self.assertIs(inst.env, self.env)
        self.assertEqual(inst.env['machine_id'], machine_id)
        self.assertEqual(inst.env['user_id'], user_id)
        self.assertIsInstance(inst.db, microfiber.Database)
        self.assertEqual(inst.db.name, 'dmedia-1')
        self.assertIsInstance(inst.log_db, microfiber.Database)
        self.assertEqual(inst.log_db.name, 'log-1')
        self.assertIsInstance(inst.server, microfiber.Server)
        self.assertIsInstance(inst.ms, MetaStore)
        self.assertIs(inst.ms.db, inst.db)
        self.assertIsInstance(inst.stores, LocalStores)
        self.assertIsInstance(inst.task_manager, core.TaskManager)
        self.assertIs(inst.ssl_config, ssl_config)
        self.assertEqual(inst.db.get('_local/dmedia'), {
            '_id': '_local/dmedia',
            '_rev': '0-2',
            'machine_id': machine_id,
            'user_id': user_id,
        })
        self.assertIsNot(inst.machine, machine)
        self.assertEqual(inst.db.get(machine_id), inst.machine)
        self.assertEqual(inst.machine['_rev'][:2], '2-')
        self.assertEqual(inst.machine['stores'], {})
        self.assertEqual(inst.machine['peers'], {})
        self.assertIsNot(inst.user, user)
        self.assertEqual(inst.db.get(user_id), inst.user)
        self.assertEqual(inst.user['_rev'][:2], '2-')

    def test_add_peer(self):
        inst = self.create()
        id1 = random_id(30)
        info1 = {
            'host': 'jderose-Gazelle-Professional',
            'url': 'https://192.168.1.139:41326/',
            'user': 'jderose',
            'version': '13.05.0'
        }
        id2 = random_id(30)
        info2 = {'url': 'https://192.168.1.77:9872/'}

        # id1 is not yet a peer:
        self.assertIsNone(inst.add_peer(id1, info1))
        self.assertEqual(inst.machine['peers'], {id1: info1})
        self.assertEqual(inst.db.get(self.machine_id), inst.machine)

        # id2 is not yet a peer:
        self.assertIsNone(inst.add_peer(id2, info2))
        self.assertEqual(inst.machine['peers'], {id1: info1, id2: info2})
        self.assertEqual(inst.db.get(self.machine_id), inst.machine)

        # id1 is already a peer, make sure info is replaced
        new1 = {'url': random_id()}
        self.assertIsNone(inst.add_peer(id1, new1))
        self.assertEqual(inst.machine['peers'], {id1: new1, id2: info2})
        self.assertEqual(inst.db.get(self.machine_id), inst.machine)

    def test_remove_peer(self):
        inst = self.create()
        id1 = random_id(30)
        id2 = random_id(30)
        info1 = {'url': random_id()}
        info2 = {'url': random_id()}
        inst.machine['peers'] = {id1: info1, id2: info2}
        inst.db.save(inst.machine)
        self.assertEqual(inst.machine['_rev'][:2], '2-')

        # Test with a peer_id that doesn't exist:
        nope = random_id(30)
        self.assertIs(inst.remove_peer(nope), False)
        self.assertEqual(inst.db.get(self.machine_id), inst.machine)
        self.assertEqual(inst.machine['peers'], {id1: info1, id2: info2})
        self.assertEqual(inst.machine['_rev'][:2], '2-')

        # id1 is present
        self.assertIs(inst.remove_peer(id1), True)
        self.assertEqual(inst.db.get(self.machine_id), inst.machine)
        self.assertEqual(inst.machine['peers'], {id2: info2})
        self.assertEqual(inst.machine['_rev'][:2], '3-')

        # id1 is missing
        self.assertIs(inst.remove_peer(id1), False)
        self.assertEqual(inst.db.get(self.machine_id), inst.machine)
        self.assertEqual(inst.machine['peers'], {id2: info2})
        self.assertEqual(inst.machine['_rev'][:2], '3-')

        # id2 is present
        self.assertIs(inst.remove_peer(id2), True)
        self.assertEqual(inst.db.get(self.machine_id), inst.machine)
        self.assertEqual(inst.machine['peers'], {})
        self.assertEqual(inst.machine['_rev'][:2], '4-')

        # id2 is missing
        self.assertIs(inst.remove_peer(id2), False)
        self.assertEqual(inst.db.get(self.machine_id), inst.machine)
        self.assertEqual(inst.machine['peers'], {})
        self.assertEqual(inst.machine['_rev'][:2], '4-')

    def test_create_filestore(self):
        inst = self.create()

        # Test when a FileStore already exists
        tmp = TempDir()
        fs = FileStore.create(tmp.dir)
        with self.assertRaises(Exception) as cm:
            inst.create_filestore(tmp.dir)
        self.assertEqual(
            str(cm.exception),
            'Already contains a FileStore: {!r}'.format(tmp.dir)
        )

        # Test when no FileStore yet exists in the parentdir:
        tmp = TempDir()
        fs = inst.create_filestore(tmp.dir)
        self.assertIsInstance(fs, filestore.FileStore)
        self.assertEqual(fs.parentdir, tmp.dir)
        self.assertEqual(fs.copies, 1)
        self.assertIs(inst.stores.by_id(fs.id), fs)
        self.assertIs(inst.stores.by_parentdir(fs.parentdir), fs)
        self.assertEqual(inst.db.get(self.machine_id), inst.machine)
        self.assertEqual(inst.machine['stores'], {
            fs.id: {'parentdir': fs.parentdir, 'copies': 1},
        })
        self.assertEqual(inst.machine['_rev'][:2], '2-')

        # Make sure we can disconnect a store that was just created
        inst.disconnect_filestore(fs.parentdir)
        self.assertEqual(inst.db.get(self.machine_id), inst.machine)
        self.assertEqual(inst.machine['stores'], {})
        self.assertEqual(inst.machine['_rev'][:2], '3-')

    def test_connect_filestore(self):
        tmp = TempDir()
        basedir = tmp.join(filestore.DOTNAME)
        inst = self.create()

        # Test when .dmedia/ doesn't exist
        with self.assertRaises(FileNotFoundError) as cm:
            inst.connect_filestore(tmp.dir, random_id())
        self.assertEqual(cm.exception.filename, basedir)

        # Test when FileStore has been initialized:
        fs = FileStore.create(tmp.dir)
        fs_a = inst.connect_filestore(tmp.dir)
        self.assertIsInstance(fs_a, FileStore)
        self.assertEqual(fs_a.parentdir, tmp.dir)
        self.assertEqual(fs_a.id, fs.id)
        self.assertEqual(fs_a.doc, fs.doc)
        doc = inst.db.get(fs.id)
        _rev = doc.pop('_rev')
        self.assertTrue(_rev.startswith('1-'))
        self.assertEqual(doc, fs.doc)

        # Test when store is already connected:
        with self.assertRaises(Exception) as cm:
            inst.connect_filestore(tmp.dir)
        self.assertEqual(
            str(cm.exception),
            'already have ID {!r}'.format(fs.id)
        )

        # Test when expected_id is provided and does *not* match:
        bad_id = random_id()
        with self.assertRaises(ValueError) as cm:
            inst.connect_filestore(tmp.dir, expected_id=bad_id)
        self.assertEqual(str(cm.exception),
            "doc['_id']: expected {!r}; got {!r}".format(bad_id, fs.id)
        )

        # Test when expected_id is provided and matches:
        inst = self.create()
        fs_b = inst.connect_filestore(tmp.dir, expected_id=fs.id)
        self.assertIsInstance(fs_b, FileStore)
        self.assertEqual(fs_b.parentdir, tmp.dir)
        self.assertEqual(fs_b.id, fs.id)
        self.assertEqual(fs_b.doc, fs.doc)

        # Connect another store
        tmp2 = TempDir()
        fs2 = FileStore.create(tmp2.dir)
        fs2_a = inst.connect_filestore(tmp2.dir)
        self.assertIsInstance(fs2_a, FileStore)
        self.assertEqual(fs2_a.parentdir, tmp2.dir)
        self.assertEqual(fs2_a.id, fs2.id)
        self.assertEqual(fs2_a.copies, 1)
        self.assertIs(inst.stores.by_id(fs2.id), fs2_a)
        self.assertIs(inst.stores.by_parentdir(fs2.parentdir), fs2_a)

        self.assertEqual(inst.machine, inst.db.get(self.machine_id))
        self.assertEqual(inst.machine['stores'], 
            {
                fs.id: {'parentdir': fs.parentdir, 'copies': 1},
                fs2.id: {'parentdir': fs2.parentdir, 'copies': 1},
            },
        )
        self.assertEqual(inst.machine['_rev'][:2], '3-')

        # Test when migration is needed
        tmp = TempDir()
        m = Migration(tmp.dir)
        old = m.build_v0_simulation()
        fs = inst.connect_filestore(tmp.dir)
        self.assertEqual(b32_to_db32(old['_id']), fs.id)

    def test_disconnect_filestore(self):
        inst = self.create()
        fs1 = TempFileStore()
        fs2 = TempFileStore()

        # Test when not connected:
        with self.assertRaises(KeyError) as cm:
            inst.disconnect_filestore(fs1.parentdir)
        self.assertEqual(str(cm.exception), repr(fs1.parentdir))

        # Connect both, then disconnect one by one
        inst.connect_filestore(fs1.parentdir, fs1.id)
        inst.connect_filestore(fs2.parentdir, fs2.id)
        self.assertEqual(inst.machine, inst.db.get(self.machine_id))
        self.assertEqual(inst.machine['stores'], 
            {
                fs1.id: {'parentdir': fs1.parentdir, 'copies': 1},
                fs2.id: {'parentdir': fs2.parentdir, 'copies': 1},
            },
        )
        self.assertEqual(inst.machine['_rev'][:2], '3-')

        # Disconnect fs1
        inst.disconnect_filestore(fs1.parentdir)
        self.assertEqual(inst.machine, inst.db.get(self.machine_id))
        self.assertEqual(inst.machine['stores'], 
            {
                fs2.id: {'parentdir': fs2.parentdir, 'copies': 1},
            },
        )
        self.assertEqual(inst.machine['_rev'][:2], '4-')

        # Disconnect fs2
        inst.disconnect_filestore(fs2.parentdir)
        self.assertEqual(inst.machine, inst.db.get(self.machine_id))
        self.assertEqual(inst.machine['stores'], {})
        self.assertEqual(inst.machine['_rev'][:2], '5-')

        # Again test when not connected:
        with self.assertRaises(KeyError) as cm:
            inst.disconnect_filestore(fs2.parentdir)
        self.assertEqual(str(cm.exception), repr(fs2.parentdir))
        with self.assertRaises(KeyError) as cm:
            inst.disconnect_filestore(fs1.parentdir)
        self.assertEqual(str(cm.exception), repr(fs1.parentdir))

    def test_resolve(self):
        inst = self.create()

        bad_id1 = random_id(25)  # Wrong length
        self.assertEqual(inst.resolve(bad_id1),
            (bad_id1, 3, '')
        )
        bad_id2 = random_id(30)[:-1] + '0'  # Invalid letter
        self.assertEqual(inst.resolve(bad_id2),
            (bad_id2, 3, '')
        )

        unknown_id = random_id(30)
        self.assertEqual(inst.resolve(unknown_id),
            (unknown_id, 2, '')
        )

        good_id = random_id(30)
        doc = {
            '_id': good_id,
            'stored': {
                random_id(): {},
                random_id(): {},
            },
        }
        inst.db.save(doc)
        self.assertEqual(inst.resolve(good_id),
            (good_id, 1, '')
        )
        tmp = TempDir()
        fs = inst.create_filestore(tmp.dir)
        self.assertEqual(inst.resolve(good_id),
            (good_id, 1, '')
        )
        doc['stored'][fs.id] = {}
        inst.db.save(doc)
        self.assertEqual(inst.resolve(good_id),
            (good_id, 1, '')
        )

        filename = fs.path(good_id)
        open(filename, 'xb').close()
        self.assertEqual(inst.resolve(good_id),
            (good_id, 1, '')
        )
        open(filename, 'wb').write(b'non empty')
        self.assertEqual(inst.resolve(good_id),
            (good_id, 0, filename)
        )

    def test_resolve_many(self):
        inst = self.create()
        tmp = TempDir()
        fs = inst.create_filestore(tmp.dir)

        bad_id1 = random_id(25)  # Wrong length
        bad_id2 = random_id(30)[:-1] + '0'  # Invalid letter
        unknown_id = random_id(30)
        nonlocal_id = random_id(30)
        missing_id = random_id(30)
        empty_id = random_id(30)
        empty_filename = fs.path(empty_id)
        open(empty_filename, 'xb').close()
        good_id = random_id(30)
        good_filename = fs.path(good_id)
        open(good_filename, 'xb').write(b'non empty')

        doc1 = {
            '_id': nonlocal_id,
            'stored': {
                random_id(): {},
                random_id(): {},
            }
        }
        doc2 = {
            '_id': missing_id,
            'stored': {
                fs.id: {},
                random_id(): {},
            }
        }
        doc3 = {
            '_id': empty_id,
            'stored': {
                fs.id: {},
                random_id(): {},
            }
        }
        doc4 = {
            '_id': good_id,
            'stored': {
                fs.id: {},
                random_id(): {},
            }
        }
        inst.db.save_many([doc1, doc2, doc3, doc4])

        ids = [bad_id1, bad_id2, unknown_id, nonlocal_id, missing_id, empty_id, good_id]
        self.assertEqual(inst.resolve_many(ids),
            [
                (bad_id1, 3, ''),
                (bad_id2, 3, ''),
                (unknown_id, 2, ''),
                (nonlocal_id, 1, ''),
                (missing_id, 1, ''),
                (empty_id, 1, ''),
                (good_id, 0, good_filename)
            ]
        )

    def test_allocate_tmp(self):
        inst = self.create()

        with self.assertRaises(Exception) as cm:        
            inst.allocate_tmp()
        self.assertEqual(str(cm.exception), 'no file-stores present')

        tmp = TempDir()
        fs = inst.create_filestore(tmp.dir)
        name = inst.allocate_tmp()
        self.assertEqual(path.dirname(name), fs.tmp)
        self.assertEqual(path.getsize(name), 0)

    def test_hash_and_move(self):
        inst = self.create()
        tmp = TempDir()
        fs = inst.create_filestore(tmp.dir)
        tmp_fp = fs.allocate_tmp()
        ch = write_random(tmp_fp)

        self.assertEqual(
            inst.hash_and_move(tmp_fp.name, 'render'),
            {
                'file_id': ch.id,
                'file_path': fs.path(ch.id),
            }
        )

        doc = inst.db.get(ch.id)
        rev = doc.pop('_rev')
        self.assertTrue(rev.startswith('1-'))
        att = doc.pop('_attachments')
        self.assertIsInstance(att, dict)
        self.assertEqual(set(att), set(['leaf_hashes']))
        ts = doc.pop('time')
        self.assertIsInstance(ts, float)
        self.assertLessEqual(ts, time.time())
        self.assertEqual(doc,
            {
                '_id': ch.id,
                'type': 'dmedia/file',
                'atime': int(ts),
                'bytes': ch.file_size,
                'origin': 'render',
                'stored': {
                    fs.id: {
                        'copies': 1,
                        'mtime': get_mtime(fs, ch.id),
                    },
                },
            }
        )
