# Authors:
#   Jason Gerard DeRose <jderose@novacut.com>
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

import couchdb

from dmedia.schema import random_id
from dmedia import core

from .couch import CouchCase


class TestDMedia(CouchCase):
    klass = core.DMedia

    def test_init(self):
        inst = self.klass(self.dbname)
        self.assertEqual(inst.env['dbname'], self.dbname)
        self.assertEqual(inst.env['port'], self.env['port'])
        self.assertEqual(inst.env['url'], self.env['url'])
        self.assertEqual(inst.env['oauth'], self.env['oauth'])
        self.assertIsInstance(inst.db, couchdb.Database)

        inst = self.klass(env=self.env)
        self.assertIs(inst.env, self.env)
        self.assertIsInstance(inst.db, couchdb.Database)

    def test_bootstrap(self):
        inst = self.klass(self.dbname)
        self.assertNotIn('machine_id', inst.env)
        self.assertIsNone(inst.bootstrap())
        self.assertEqual(inst.env['machine_id'], inst.machine_id)

    def test_init_local(self):
        inst = self.klass(self.dbname)

        # Test when _local/node doesn't exist:
        local = inst.init_local()
        self.assertEqual(
            set(local),
            set([
                '_id',
                '_rev',
                'machine',
                'filestores',
            ])
        )
        machine = local['machine']
        self.assertIsInstance(machine, dict)
        self.assertEqual(
            set(machine),
            set([
                '_id',
                'type',
                'time',
                'hostname',
                'distribution',
            ])
        )
        self.assertEqual(local['filestores'], {})

        loc2 = inst.db['_local/node']
        self.assertEqual(loc2['machine'], local['machine'])


    def test_init_machine(self):
        inst = self.klass(self.dbname)

        # Test when _local/machine doesn't exist:
        doc = inst.init_machine()
        self.assertEqual(
            set(doc),
            set([
                '_id',
                '_rev',
                'type',
                'time',
                'hostname',
                'distribution',
            ])
        )
        self.assertEqual(doc['type'], 'dmedia/machine')
        loc = inst.db['_local/machine']
        self.assertEqual(doc['_id'], loc['machine_id'])

        # Test when _local/machine exists but 'dmedia/machine' doc doesn't:
        old = doc
        loc['machine_id'] = 'foobar'
        inst.db.save(loc)
        doc = inst.init_machine()
        self.assertEqual(
            set(doc),
            set([
                '_id',
                '_rev',
                'type',
                'time',
                'hostname',
                'distribution',
            ])
        )
        self.assertEqual(doc['type'], 'dmedia/machine')
        self.assertEqual(doc['_id'], 'foobar')
        self.assertGreater(doc['time'], old['time'])

        # Test when both _local/machine and 'dmedia/machine' doc exist:
        inst.db.delete(loc)
        loc = {
            '_id': '_local/machine',
            'machine_id': 'HelloNaughtyNurse',
        }
        inst.db.save(loc)
        machine = {
            '_id': 'HelloNaughtyNurse',
        }
        inst.db.save(machine)
        doc = inst.init_machine()
        self.assertEqual(set(doc), set(['_id', '_rev']))
        self.assertEqual(doc['_id'], 'HelloNaughtyNurse')
        self.assertEqual(doc, inst.db['HelloNaughtyNurse'])

    def test_init_filestores(self):
        inst = self.klass(self.dbname)
        _id = '_local/filestores'
        self.assertIsNone(inst.db.get(_id))
        self.assertIsNone(inst.init_filestores())
        doc = inst.db.get(_id)
        self.assertEqual(doc['_id'], _id)
        self.assertEqual(doc['fixed'], {})
