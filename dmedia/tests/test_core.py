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

import microfiber
from microfiber import random_id
import filestore

from dmedia.local import LocalStores
from dmedia.metastore import MetaStore
from dmedia.schema import DB_NAME, create_filestore, project_db_name
from dmedia import util, core

from .couch import CouchCase
from .base import TempDir


class TestCouchFunctions(CouchCase):
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


class TestCore(CouchCase):
    def test_init(self):
        inst = core.Core(self.env)
        self.assertIsInstance(inst.db, microfiber.Database)
        self.assertEqual(inst.db.name, DB_NAME)
        self.assertIsInstance(inst.server, microfiber.Server)
        self.assertIs(inst.db.ctx, inst.server.ctx)
        self.assertIsInstance(inst.stores, LocalStores)
        self.assertEqual(inst.local, {'_id': '_local/dmedia', 'stores': {}})

    def test_load_identity(self):
        machine_id = random_id(30)
        user_id = random_id(30)
        inst = core.Core(self.env)
        inst.load_identity({'_id': machine_id}, {'_id': user_id})

        machine = inst.db.get(machine_id)
        self.assertEqual(set(machine), set(['_id', '_rev']))
        self.assertTrue(machine['_rev'].startswith('1-'))
        user = inst.db.get(user_id)
        self.assertEqual(set(user), set(['_id', '_rev']))
        self.assertTrue(user['_rev'].startswith('1-'))

        self.assertEqual(
            inst.db.get('_local/dmedia'),
            {
                '_id': '_local/dmedia',
                '_rev': '0-1',
                'stores': {},
                'machine_id': machine_id,
                'user_id': user_id,
            }
        )
        self.assertEqual(inst.local, inst.db.get('_local/dmedia'))

        self.assertEqual(self.env['machine_id'], machine_id)
        self.assertEqual(self.env['user_id'], user_id)

        inst = core.Core(self.env)
        inst.load_identity({'_id': machine_id}, {'_id': user_id})
        self.assertTrue(inst.db.get(machine_id)['_rev'].startswith('1-'))
        self.assertTrue(inst.db.get(user_id)['_rev'].startswith('1-'))

        self.assertEqual(set(machine), set(['_id', '_rev']))
        self.assertTrue(machine['_rev'].startswith('1-'))
        self.assertEqual(
            inst.db.get('_local/dmedia'),
            {
                '_id': '_local/dmedia',
                '_rev': '0-1',
                'stores': {},
                'machine_id': machine_id,
                'user_id': user_id,
            }
        )

    def test_init_default_store(self):
        private = TempDir()
        shared = TempDir()
        machine_id = random_id()

        # Test when default_store is missing
        inst = core.Core(self.env, private.dir, shared.dir)
        self.assertEqual(inst._private, private.dir)
        self.assertEqual(inst._shared, shared.dir)
        inst.init_default_store()
        self.assertIsNone(inst.default)
        self.assertEqual(inst.local,
            {
                '_id': '_local/dmedia',
                'stores': {},
            }
        )

        # Test when default_store is 'private'
        inst = core.Core(self.env, private.dir, shared.dir)
        self.assertEqual(inst._private, private.dir)
        self.assertEqual(inst._shared, shared.dir)
        inst.local['default_store'] = 'private'
        self.assertFalse(util.isfilestore(private.dir))
        inst.init_default_store()
        self.assertEqual(
            set(inst.local['stores']),
            set([private.dir])
        )
        store_id = inst.local['stores'][private.dir]['id']
        (fs1, doc) = util.get_filestore(private.dir, store_id)
        self.assertEqual(inst.local,
            {
                '_id': '_local/dmedia',
                '_rev': '0-1',
                'default_store': 'private',
                'stores': {
                    fs1.parentdir: {
                        'id': fs1.id,
                        'copies': fs1.copies,
                    }
                }
            }
        )

        # Again test when default_store is 'private' to make sure local isn't
        # updated needlessly
        inst = core.Core(self.env, private.dir, shared.dir)
        self.assertEqual(inst._private, private.dir)
        self.assertEqual(inst._shared, shared.dir)
        inst.init_default_store()
        self.assertEqual(inst.local,
            {
                '_id': '_local/dmedia',
                '_rev': '0-1',
                'default_store': 'private',
                'stores': {
                    fs1.parentdir: {
                        'id': fs1.id,
                        'copies': fs1.copies,
                    }
                }
            }
        )

        # Test when default_store is 'shared' (which we're assuming exists)
        self.assertFalse(util.isfilestore(shared.dir))
        (fs2, doc) = util.init_filestore(shared.dir)
        inst = core.Core(self.env, private.dir, shared.dir)
        self.assertEqual(inst._private, private.dir)
        self.assertEqual(inst._shared, shared.dir)
        inst.local['default_store'] = 'shared'
        inst.init_default_store()
        self.assertEqual(inst.local,
            {
                '_id': '_local/dmedia',
                '_rev': '0-2',
                'default_store': 'shared',
                'stores': {
                    fs2.parentdir: {
                        'id': fs2.id,
                        'copies': fs2.copies,
                    }
                }
            }
        )

        # Test when default_store is 'shared' and it doesn't exist, in which
        # case it should automatically switch to 'private' instead
        nope = TempDir()
        inst = core.Core(self.env, private.dir, nope.dir)
        self.assertEqual(inst._private, private.dir)
        self.assertEqual(inst._shared, nope.dir)
        inst.local['default_store'] = 'shared'
        inst.init_default_store()
        self.assertEqual(inst.local,
            {
                '_id': '_local/dmedia',
                '_rev': '0-4',
                'default_store': 'private',
                'stores': {
                    fs1.parentdir: {
                        'id': fs1.id,
                        'copies': fs1.copies,
                    }
                }
            }
        )

    def test_set_default_store(self):
        private = TempDir()
        shared = TempDir()
        (fs1, doc1) = util.init_filestore(private.dir)
        (fs2, doc2) = util.init_filestore(shared.dir)
        inst = core.Core(self.env, private.dir, shared.dir)
        self.assertEqual(inst._private, private.dir)
        self.assertEqual(inst._shared, shared.dir)

        with self.assertRaises(ValueError) as cm:
            inst.set_default_store('foobar')
        self.assertEqual(
            str(cm.exception),
            "need 'private', 'shared', or 'none'; got 'foobar'"
        )
        self.assertEqual(inst.local,
            {
                '_id': '_local/dmedia',
                'stores': {},
            }
        )

        # Test with 'private'
        inst.set_default_store('private')
        self.assertEqual(inst.local,
            {
                '_id': '_local/dmedia',
                '_rev': '0-2',
                'default_store': 'private',
                'stores': {
                    fs1.parentdir: {'id': fs1.id, 'copies': 1},
                },
            }
        )

        # Test with 'shared'
        inst.set_default_store('shared')
        self.assertEqual(inst.local,
            {
                '_id': '_local/dmedia',
                '_rev': '0-5',
                'default_store': 'shared',
                'stores': {
                    fs2.parentdir: {'id': fs2.id, 'copies': 1},
                },
            }
        )

        # Test with 'none'
        inst.set_default_store('none')
        self.assertEqual(inst.local,
            {
                '_id': '_local/dmedia',
                '_rev': '0-7',
                'default_store': 'none',
                'stores': {},
            }
        )

        # Test with 'shared' when it doesn't exist
        nope = TempDir()
        inst = core.Core(self.env, private.dir, nope.dir)
        self.assertEqual(inst._private, private.dir)
        self.assertEqual(inst._shared, nope.dir)
        inst.set_default_store('shared')
        self.assertEqual(inst.local,
            {
                '_id': '_local/dmedia',
                '_rev': '0-10',
                'default_store': 'private',
                'stores': {
                    fs1.parentdir: {'id': fs1.id, 'copies': 1},
                },
            }
        )

    def test_create_filestore(self):
        inst = core.Core(self.env)

        # Test when a filestore already exists
        tmp = TempDir()
        (fs, doc) = util.init_filestore(tmp.dir)
        with self.assertRaises(Exception) as cm:
            inst.create_filestore(tmp.dir)
        self.assertEqual(
            str(cm.exception),
            'Already contains a FileStore: {!r}'.format(tmp.dir)
        )

        # Test when .dmedia doesn't exist
        tmp = TempDir()
        fs = inst.create_filestore(tmp.dir)
        self.assertIsInstance(fs, filestore.FileStore)
        self.assertEqual(fs.parentdir, tmp.dir)
        self.assertEqual(fs.copies, 1)
        self.assertIs(inst.stores.by_id(fs.id), fs)
        self.assertIs(inst.stores.by_parentdir(fs.parentdir), fs)
        self.assertEqual(
            inst.db.get('_local/dmedia'),
            {
                '_id': '_local/dmedia',
                '_rev': '0-1',
                'stores': {
                    fs.parentdir: {'id': fs.id, 'copies': fs.copies},
                }
            }
        )

        # Make sure we can disconnect a store that was just created
        inst.disconnect_filestore(fs.parentdir, fs.id)
        self.assertEqual(
            inst.db.get('_local/dmedia'),
            {
                '_id': '_local/dmedia',
                '_rev': '0-2',
                'stores': {},
            }
        )

    def test_connect_filestore(self):
        inst = core.Core(self.env)
        tmp = TempDir()
        doc = create_filestore(1)

        # Test when .dmedia/ doesn't exist
        with self.assertRaises(IOError) as cm:
            inst.connect_filestore(tmp.dir, doc['_id'])

        # Test when .dmedia/ exists, but store.json doesn't:
        tmp.makedirs('.dmedia')
        with self.assertRaises(IOError) as cm:
            inst.connect_filestore(tmp.dir, doc['_id'])

        # Test when .dmedia/store.json exists
        store = tmp.join('.dmedia', 'store.json')
        json.dump(doc, open(store, 'w'))

        fs = inst.connect_filestore(tmp.dir, doc['_id'])
        self.assertIsInstance(fs, filestore.FileStore)
        self.assertEqual(fs.parentdir, tmp.dir)
        self.assertEqual(fs.id, doc['_id'])
        self.assertEqual(fs.copies, 1)
        self.assertIs(inst.stores.by_id(fs.id), fs)
        self.assertIs(inst.stores.by_parentdir(fs.parentdir), fs)
        self.assertEqual(
            inst.db.get('_local/dmedia'),
            {
                '_id': '_local/dmedia',
                '_rev': '0-1',
                'stores': {
                    fs.parentdir: {'id': fs.id, 'copies': fs.copies},
                }
            }
        )

        # Test when store_id doesn't match
        store_id = random_id()
        with self.assertRaises(Exception) as cm:
            inst.connect_filestore(tmp.dir, store_id)
        self.assertEqual(
            str(cm.exception),
            'expected store_id {!r}; got {!r}'.format(store_id, doc['_id'])
        )

        # Test when store is already connected:
        with self.assertRaises(Exception) as cm:
            inst.connect_filestore(tmp.dir, doc['_id'])
        self.assertEqual(
            str(cm.exception),
            'already have ID {!r}'.format(doc['_id'])
        )

        # Connect another store
        tmp2 = TempDir()
        doc2 = create_filestore(0)
        tmp2.makedirs('.dmedia')
        store2 = tmp2.join('.dmedia', 'store.json')
        json.dump(doc2, open(store2, 'w'))

        fs2 = inst.connect_filestore(tmp2.dir, doc2['_id'])
        self.assertIsInstance(fs2, filestore.FileStore)
        self.assertEqual(fs2.parentdir, tmp2.dir)
        self.assertEqual(fs2.id, doc2['_id'])
        self.assertEqual(fs2.copies, 0)
        self.assertIs(inst.stores.by_id(fs2.id), fs2)
        self.assertIs(inst.stores.by_parentdir(fs2.parentdir), fs2)
        self.assertEqual(
            inst.db.get('_local/dmedia'),
            {
                '_id': '_local/dmedia',
                '_rev': '0-2',
                'stores': {
                    fs.parentdir: {'id': fs.id, 'copies': 1},
                    fs2.parentdir: {'id': fs2.id, 'copies': 0},
                }
            }
        )

    def test_disconnect_filestore(self):
        inst = core.Core(self.env)

        tmp1 = TempDir()
        (fs1, doc1) = util.init_filestore(tmp1.dir)
        tmp2 = TempDir()
        (fs2, doc2) = util.init_filestore(tmp2.dir)

        # Test when not connected:
        with self.assertRaises(KeyError) as cm:
            inst.disconnect_filestore(fs1.parentdir, fs1.id)
        self.assertEqual(str(cm.exception), repr(fs1.parentdir))

        # Connect both, then disconnect one by one
        inst.connect_filestore(fs1.parentdir, fs1.id)
        inst.connect_filestore(fs2.parentdir, fs2.id)
        self.assertEqual(
            inst.db.get('_local/dmedia'),
            {
                '_id': '_local/dmedia',
                '_rev': '0-2',
                'stores': {
                    fs1.parentdir: {'id': fs1.id, 'copies': 1},
                    fs2.parentdir: {'id': fs2.id, 'copies': 1},
                }
            }
        )

        # Disconnect fs1
        inst.disconnect_filestore(fs1.parentdir, fs1.id)
        self.assertEqual(
            inst.db.get('_local/dmedia'),
            {
                '_id': '_local/dmedia',
                '_rev': '0-3',
                'stores': {
                    fs2.parentdir: {'id': fs2.id, 'copies': 1},
                }
            }
        )

        # Disconnect fs2
        inst.disconnect_filestore(fs2.parentdir, fs2.id)
        self.assertEqual(
            inst.db.get('_local/dmedia'),
            {
                '_id': '_local/dmedia',
                '_rev': '0-4',
                'stores': {},
            }
        )

        # Again test when not connected:
        with self.assertRaises(KeyError) as cm:
            inst.disconnect_filestore(fs2.parentdir, fs2.id)
        self.assertEqual(str(cm.exception), repr(fs2.parentdir))
        with self.assertRaises(KeyError) as cm:
            inst.disconnect_filestore(fs1.parentdir, fs1.id)
        self.assertEqual(str(cm.exception), repr(fs1.parentdir))

