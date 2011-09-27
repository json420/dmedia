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
from filestore import FileStore, DIGEST_BYTES

from dmedia.schema import random_id, check_store
from dmedia import core

from .couch import CouchCase
from .base import TempDir, TempHome


class TestCore(CouchCase):
    klass = core.Core

    def setUp(self):
        super().setUp()
        self.home = TempHome()

    def tearDown(self):
        super().tearDown()
        self.home = None

    def test_init(self):
        inst = self.klass(self.env)
        self.assertIs(inst.env, self.env)
        self.assertEqual(inst.home, self.home.dir)
        self.assertIsInstance(inst.db, microfiber.Database)

    def test_bootstrap(self):
        del self.env['machine_id']
        inst = self.klass(self.env)
        self.assertNotIn('machine_id', inst.env)
        self.assertIsNone(inst.bootstrap())
        self.assertEqual(inst.env['machine_id'], inst.machine_id)

    def test_init_local(self):
        inst = self.klass(self.env)

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
        inst = self.klass(self.env)
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
                'parentdir',
                'machine_id',
                #'partition_id',
            ])
        )
        self.assertEqual(lstore['ver'], 0)
        self.assertEqual(lstore['type'], 'dmedia/store')
        self.assertEqual(lstore['plugin'], 'filestore')
        self.assertEqual(lstore['copies'], 1)
        self.assertEqual(lstore['parentdir'], self.home.dir)
        self.assertEqual(lstore['machine_id'], inst.machine_id)

        store = inst.db.get(_id)
        self.assertTrue(store['_rev'].startswith('1-'))
        store.pop('_rev')
        self.assertEqual(store, lstore)

        # Try again when docs already exist:
        self.assertEqual(inst.init_filestores(), lstore)

    def test_add_filestores(self):
        inst = self.klass(self.env)
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
        self.assertEqual(fs.parentdir, okay)

        # Test the doc
        check_store(store)
        self.assertEqual(inst.db.get(store['_id']), store)
        self.assertEqual(store['parentdir'], okay)
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

    def test_get_file(self):
        src = TempDir()
        (file, ch) = src.random_file()

        inst = self.klass(self.env)
        self.assertIsNone(inst.get_file(ch.id))

        tmp1 = TempDir()
        tmp2 = TempDir()
        fs1 = FileStore(tmp1.dir)
        fs2 = FileStore(tmp2.dir)
        inst._filestores[tmp1.dir] = fs1
        inst._filestores[tmp2.dir] = fs2
        self.assertIsNone(inst.get_file(ch.id))

        src_fp = open(file.name, 'rb')
        fs1.import_file(src_fp)
        self.assertEqual(inst.get_file(ch.id), fs1.path(ch.id))
        fs1.remove(ch.id)

        src_fp = open(file.name, 'rb')
        fs2.import_file(src_fp)
        self.assertEqual(inst.get_file(ch.id), fs2.path(ch.id))
