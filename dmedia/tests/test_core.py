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

import microfiber
from microfiber import random_id
import filestore

from dmedia.local import LocalStores
from dmedia.schema import DB_NAME
from dmedia import util, core

from .couch import CouchCase
from .base import TempDir


class TestCore(CouchCase):
    def test_init(self):
        inst = core.Core(self.env)
        self.assertIsInstance(inst.db, microfiber.Database)
        self.assertEqual(inst.db.name, DB_NAME)
        self.assertIsInstance(inst.stores, LocalStores)
        self.assertEqual(inst.local['stores'], {})

        # Add some filestores
        tmp1 = TempDir()
        tmp2 = TempDir()
        fs1 = inst.add_filestore(tmp1.dir, copies=2)
        fs2 = inst.add_filestore(tmp2.dir)

        # Test that these filestores are brought up next time
        inst = core.Core(self.env)
        self.assertEqual(inst.local['stores'],
            {
                tmp1.dir: {'id': fs1.id, 'copies': 2},
                tmp2.dir: {'id': fs2.id, 'copies': 1},
            }
        )
        self.assertEqual(inst.local, inst.db.get('_local/dmedia'))
        
        # Test that startup works even when a store is missing:
        tmp1.rmtree()
        inst = core.Core(self.env)
        self.assertEqual(inst.local['stores'],
            {
                tmp2.dir: {'id': fs2.id, 'copies': 1},
            }
        )
        self.assertEqual(inst.local, inst.db.get('_local/dmedia'))

        # Test that startup works when store is missing and it brings us down to
        # zero stores
        tmp2.rmtree()
        inst = core.Core(self.env)
        self.assertEqual(inst.local['stores'], {})
        self.assertEqual(inst.local, inst.db.get('_local/dmedia'))

    def test_init_local(self):
        inst = core.Core(self.env, bootstrap=False)
        self.assertTrue(inst.db.ensure())
        self.assertFalse(hasattr(inst, 'local'))
        self.assertIsNone(inst._init_local())
        self.assertEqual(inst.local, inst.db.get('_local/dmedia'))

    def test_add_filestore(self):
        inst = core.Core(self.env)
        tmp = TempDir()

        # Test adding new
        fs = inst.add_filestore(tmp.dir)
        self.assertIsInstance(fs, filestore.FileStore)
        self.assertEqual(fs.parentdir, tmp.dir)
        self.assertEqual(fs.copies, 1)
        self.assertEqual(
            inst.local['stores'],
            {tmp.dir: {'id': fs.id, 'copies': 1}}
        )
        self.assertEqual(inst.stores.ids, {fs.id: fs})
        self.assertEqual(inst.stores.parentdirs, {tmp.dir: fs})

        # Test adding a duplicate
        with self.assertRaises(Exception) as cm:
            inst.add_filestore(tmp.dir)
        self.assertEqual(
            str(cm.exception),
            'already have parentdir {!r}'.format(tmp.dir)
        )
        self.assertEqual(
            inst.local['stores'],
            {tmp.dir: {'id': fs.id, 'copies': 1}}
        )
        self.assertEqual(inst.stores.ids, {fs.id: fs})
        self.assertEqual(inst.stores.parentdirs, {tmp.dir: fs})

        # Test adding a duplicate on next startup
        inst = core.Core(self.env)
        with self.assertRaises(Exception) as cm:
            inst.add_filestore(tmp.dir)
        self.assertEqual(
            str(cm.exception),
            'already have parentdir {!r}'.format(tmp.dir)
        )
        self.assertEqual(
            inst.local['stores'],
            {tmp.dir: {'id': fs.id, 'copies': 1}}
        )
        self.assertEqual(set(inst.stores.ids), set([fs.id]))
        self.assertEqual(set(inst.stores.parentdirs), set([tmp.dir]))



class TestCore2(CouchCase):
    def test_init(self):
        inst = core.Core2(self.env)
        self.assertIsInstance(inst.db, microfiber.Database)
        self.assertEqual(inst.db.name, DB_NAME)
        self.assertEqual(inst.local['stores'], {})

    def test_init_default_store(self):
        private = TempDir()
        shared = TempDir()
        machine_id = random_id()

        # Test when default_store is missing
        inst = core.Core2(self.env, private.dir, shared.dir, bootstrap=False)
        self.assertEqual(inst._private, private.dir)
        self.assertEqual(inst._shared, shared.dir)
        self.assertFalse(hasattr(inst, 'local'))
        inst.local = {
            '_id': '_local/dmedia',
            'machine_id': machine_id,
        }

        inst._init_default_store()
        self.assertEqual(inst.local,
            {
                '_id': '_local/dmedia',
                '_rev': '0-1',
                'machine_id': machine_id,
                'stores': {},
            }
        )

        # Test when default_store is 'private'
        inst = core.Core2(self.env, private.dir, shared.dir, bootstrap=False)
        self.assertEqual(inst._private, private.dir)
        self.assertEqual(inst._shared, shared.dir)
        self.assertFalse(hasattr(inst, 'local'))
        inst._init_local()
        self.assertEqual(inst.local,
            {
                '_id': '_local/dmedia',
                '_rev': '0-1',
                'machine_id': machine_id,
                'stores': {},
            }
        )
        inst.local['default_store'] = 'private'

        self.assertFalse(util.isfilestore(private.dir))
        inst._init_default_store()
        self.assertEqual(
            set(inst.local['stores']),
            set([private.dir])
        )
        store_id = inst.local['stores'][private.dir]['id']
        (fs1, doc) = util.get_filestore(private.dir, store_id)
        self.assertEqual(inst.local,
            {
                '_id': '_local/dmedia',
                '_rev': '0-2',
                'machine_id': machine_id,
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
        inst = core.Core2(self.env, private.dir, shared.dir, bootstrap=False)
        self.assertEqual(inst._private, private.dir)
        self.assertEqual(inst._shared, shared.dir)
        self.assertFalse(hasattr(inst, 'local'))
        inst._init_local()
        inst._init_default_store()
        self.assertEqual(inst.local,
            {
                '_id': '_local/dmedia',
                '_rev': '0-2',
                'machine_id': machine_id,
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
        inst = core.Core2(self.env, private.dir, shared.dir, bootstrap=False)
        self.assertEqual(inst._private, private.dir)
        self.assertEqual(inst._shared, shared.dir)
        self.assertFalse(hasattr(inst, 'local'))
        inst._init_local()
        inst.local['default_store'] = 'shared'
        inst._init_default_store()
        self.assertEqual(inst.local,
            {
                '_id': '_local/dmedia',
                '_rev': '0-3',
                'machine_id': machine_id,
                'default_store': 'shared',
                'stores': {
                    fs2.parentdir: {
                        'id': fs2.id,
                        'copies': fs2.copies,
                    }
                }
            }
        )

        

