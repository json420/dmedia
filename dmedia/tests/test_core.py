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

import microfiber

from dmedia.webui.app import App
from dmedia.schema import random_id, check_store
from dmedia.filestore import FileStore
from dmedia import core

from .helpers import TempDir, mov_hash, sample_mov
from .couch import CouchCase


class TestCore(CouchCase):
    klass = core.Core

    def tearDown(self):
        super(TestCore, self).tearDown()
        if core.App is None:
            core.App = App

    def test_init(self):
        inst = self.klass(self.dbname)
        self.assertNotEqual(inst.env, self.env)
        self.assertEqual(
            set(inst.env),
            set(['url', 'dbname', 'oauth', 'basic'])
        )
        self.assertEqual(inst.env['dbname'], self.dbname)
        self.assertEqual(inst.env['url'], self.env['url'])
        self.assertEqual(inst.env['oauth'], self.env['oauth'])
        self.assertEqual(inst.home, self.home.path)
        self.assertIsInstance(inst.db, microfiber.Database)

        inst = self.klass(env_s=json.dumps(self.env))
        self.assertEqual(inst.env, self.env)

    def test_bootstrap(self):
        inst = self.klass(self.dbname)
        self.assertNotIn('machine_id', inst.env)
        self.assertIsNone(inst.bootstrap())
        self.assertEqual(inst.env['machine_id'], inst.machine_id)

    def test_init_local(self):
        inst = self.klass(self.dbname)

        # Test when _local/dmedia doesn't exist:
        (local, machine) = inst.init_local()

        self.assertIsInstance(local, dict)
        self.assertEqual(
            set(local),
            set([
                '_id',
                '_rev',
                'machine',
                'filestores',
            ])
        )
        self.assertEqual(local['filestores'], {})
        self.assertEqual(local, inst.db.get('_local/dmedia'))

        self.assertIsInstance(machine, dict)
        self.assertEqual(
            set(machine),
            set([
                '_id',
                '_rev',
                'ver',
                'type',
                'time',
                'hostname',
                'distribution',
            ])
        )
        self.assertEqual(machine, inst.db.get(local['machine']['_id']))

        # Test when _local/machine exists but 'dmedia/machine' doc doesn't:
        inst.db.delete(machine['_id'], rev=machine['_rev'])
        (local2, machine2) = inst.init_local()
        self.assertEqual(local2, local)
        self.assertTrue(machine2['_rev'].startswith('3-'))
        self.assertNotEqual(machine2['_rev'], machine['_rev'])
        d = dict(machine2)
        d.pop('_rev')
        self.assertEqual(d, local['machine'])

        # Test when both _local/dmedia and 'dmedia/machine' doc exist:
        local3 = {
            '_id': '_local/dmedia',
            '_rev': local2['_rev'],
            'machine': {
                '_id': 'foobar',
                'hello': 'world',
            }
        }
        inst.db.save(local3)
        machine3 = {
            '_id': 'foobar',
            '_rev': machine2['_rev'],
            'hello': 'naughty nurse',
        }
        inst.db.save(machine3)
        (local4, machine4) = inst.init_local()
        self.assertEqual(local4, local3)
        self.assertEqual(machine4, machine3)

    def test_init_filestores(self):
        inst = self.klass(self.dbname)
        (inst.local, inst.machine) = inst.init_local()
        inst.machine_id = inst.machine['_id']
        inst.env['machine_id'] = inst.machine_id

        self.assertEqual(inst.local['filestores'], {})
        self.assertNotIn('default_filestore', inst.local)
        lstore = inst.init_filestores()
        self.assertEqual(inst.local, inst.db.get('_local/dmedia'))
        self.assertEqual(len(inst.local['filestores']), 1)
        parentdir = inst.local['default_filestore']
        _id = lstore['_id']
        self.assertEqual(inst.local['filestores'][parentdir], lstore)
        self.assertEqual(
            set(lstore),
            set([
                '_id',
                'ver',
                'type',
                'time',
                'plugin',
                'copies',
                'path',
                'machine_id',
                'partition_id',
            ])
        )
        self.assertEqual(lstore['ver'], 0)
        self.assertEqual(lstore['type'], 'dmedia/store')
        self.assertEqual(lstore['plugin'], 'filestore')
        self.assertEqual(lstore['copies'], 1)
        self.assertEqual(lstore['path'], self.home.path)
        self.assertEqual(lstore['machine_id'], inst.machine_id)

        store = inst.db.get(_id)
        self.assertTrue(store['_rev'].startswith('1-'))
        store.pop('_rev')
        self.assertEqual(store, lstore)

        # Try again when docs already exist:
        self.assertEqual(inst.init_filestores(), lstore)

    def test_add_filestores(self):
        inst = self.klass(self.dbname)
        inst.machine_id = random_id()
        inst.local = {
            '_id': '_local/dmedia',
            'filestores': {},
        }
        tmp = TempDir()

        # Test when parentdir does not exist:
        nope = tmp.join('nope')
        with self.assertRaises(ValueError) as cm:
            store = inst.add_filestore(nope)
        self.assertEqual(
            str(cm.exception),
            'Not a directory: {!r}'.format(nope)
        )

        # Test when parentdir is a file:
        a_file = tmp.touch('a_file')
        with self.assertRaises(ValueError) as cm:
            store = inst.add_filestore(a_file)
        self.assertEqual(
            str(cm.exception),
            'Not a directory: {!r}'.format(a_file)
        )

        # Test when parentdir is okay:
        okay = tmp.makedirs('okay')
        self.assertEqual(inst._filestores, {})
        store = inst.add_filestore(okay)

        # Test the FileStore
        self.assertEqual(set(inst._filestores), set([okay]))
        fs = inst._filestores[okay]
        self.assertIsInstance(fs, FileStore)
        self.assertEqual(fs.parent, okay)

        # Test the doc
        check_store(store)
        self.assertEqual(inst.db.get(store['_id']), store)
        self.assertEqual(store['path'], okay)
        self.assertTrue(store.pop('_rev').startswith('1-'))
        self.assertEqual(list(inst.local['filestores']), [okay])
        self.assertEqual(inst.local['filestores'][okay], store)
        self.assertEqual(inst.local['default_filestore'], okay)
        self.assertEqual(inst.db.get('_local/dmedia'), inst.local)
        self.assertEqual(inst.db.get('_local/dmedia')['_rev'], '0-1')

        # Test when store already initialized:
        self.assertEqual(inst.add_filestore(okay), store)
        self.assertTrue(inst.db.get(store['_id'])['_rev'].startswith('1-'))
        self.assertEqual(inst.db.get('_local/dmedia')['_rev'], '0-1')

    def test_init_app(self):
        inst = self.klass(self.dbname)

        # App is available
        with self.assertRaises(microfiber.NotFound) as cm:
            inst.db.get('app')
        self.assertIs(inst.init_app(), True)
        self.assertEqual(inst.db.get('app')['_id'], 'app')
        self.assertIs(inst.init_app(), True)

        # App is not available
        core.App = None
        self.assertIs(inst.init_app(), False)

        # App is available again (make sure there is no state)
        core.App = App
        self.assertIs(inst.init_app(), True)
        self.assertIs(inst.init_app(), True)

    def test_has_app(self):
        class Sub(self.klass):
            _calls = 0

            def init_app(self):
                self._calls += 1
                return 'A' * self._calls

        inst = Sub(self.dbname)
        self.assertIsNone(inst._has_app)

        inst._has_app = 'foo'
        self.assertEqual(inst.has_app(), 'foo')
        self.assertEqual(inst._calls, 0)

        inst._has_app = None
        self.assertEqual(inst.has_app(), 'A')
        self.assertEqual(inst._calls, 1)
        self.assertEqual(inst._has_app, 'A')

        self.assertEqual(inst.has_app(), 'A')
        self.assertEqual(inst._calls, 1)
        self.assertEqual(inst._has_app, 'A')

        inst._has_app = None
        self.assertEqual(inst.has_app(), 'AA')
        self.assertEqual(inst._calls, 2)
        self.assertEqual(inst._has_app, 'AA')

        self.assertEqual(inst.has_app(), 'AA')
        self.assertEqual(inst._calls, 2)
        self.assertEqual(inst._has_app, 'AA')


        # Test the real thing, no App
        core.App = None
        inst = self.klass(self.dbname)
        self.assertIs(inst.has_app(), False)
        self.assertIs(inst._has_app, False)
        with self.assertRaises(microfiber.NotFound) as cm:
            inst.db.get('app')


        # Test the real thing, App available
        core.App = App
        inst = self.klass(self.dbname)

        with self.assertRaises(microfiber.NotFound) as cm:
            inst.db.get('app')
        self.assertIs(inst.has_app(), True)
        self.assertIs(inst._has_app, True)
        rev = inst.db.get('app')['_rev']
        self.assertTrue(rev.startswith('1-'))

        self.assertIs(inst.has_app(), True)
        self.assertIs(inst._has_app, True)
        self.assertEqual(inst.db.get('app')['_rev'], rev)

        inst._has_app = None
        self.assertIs(inst.has_app(), True)
        self.assertIs(inst._has_app, True)
        rev2 = inst.db.get('app')['_rev']
        self.assertNotEqual(rev2, rev)
        self.assertTrue(rev2.startswith('2-'))

    def test_get_file(self):
        inst = self.klass(self.dbname)
        doc = {
            '_id': mov_hash,
            'ext': 'mov',
        }
        inst.db.save(doc)
        self.assertIsNone(inst.get_file(mov_hash))

        tmp1 = TempDir()
        tmp2 = TempDir()
        fs1 = FileStore(tmp1.path)
        fs2 = FileStore(tmp2.path)
        inst._filestores[tmp1.path] = fs1
        inst._filestores[tmp2.path] = fs2
        self.assertIsNone(inst.get_file(mov_hash))

        src_fp = open(sample_mov, 'rb')
        fs1.import_file(src_fp)
        self.assertIsNone(inst.get_file(mov_hash))

        src_fp = open(sample_mov, 'rb')
        fs2.import_file(src_fp, 'mov')
        self.assertEqual(inst.get_file(mov_hash), fs2.path(mov_hash, 'mov'))
