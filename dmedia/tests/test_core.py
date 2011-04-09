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
        self.assertEqual(local, inst.db['_local/dmedia'])

        self.assertIsInstance(machine, dict)
        self.assertEqual(
            set(machine),
            set([
                '_id',
                '_rev',
                'type',
                'time',
                'hostname',
                'distribution',
            ])
        )
        self.assertEqual(machine, inst.db[local['machine']['_id']])

        # Test when _local/machine exists but 'dmedia/machine' doc doesn't:
        inst.db.delete(machine)
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
