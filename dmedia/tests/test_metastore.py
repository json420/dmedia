# Authors:
#   Jason Gerard DeRose <jderose@novacut.com>
#
# dmedia: distributed media library
# Copyright (C) 2010 Jason Gerard DeRose <jderose@novacut.com>
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
Unit tests for `dmedia.metastore` module.
"""

from unittest import TestCase
import os
import shutil
import socket
import platform

import couchdb
import desktopcouch
from desktopcouch.records.server import  CouchDatabase

from dmedia import metastore
from .helpers import TempDir, TempHome
from .couch import CouchCase


class test_functions(TestCase):
    def test_build_design_doc(self):
        f = metastore.build_design_doc
        views = (
            ('bytes', 'foo', '_sum'),
            ('mtime', 'bar', None),
        )
        self.assertEqual(f('file', views),
            (
                '_design/file',
                {
                    '_id': '_design/file',
                    'language': 'javascript',
                    'views': {
                        'bytes': {
                            'map': 'foo',
                            'reduce': '_sum',
                        },
                        'mtime': {
                            'map': 'bar',
                        },
                    }
                }
            )
        )

    def test_create_machine(self):
        f = metastore.create_machine
        doc = f()
        self.assertTrue(isinstance(doc, dict))
        self.assertEqual(
            set(doc),
            set([
                '_id',
                'machine_id',
                'type',
                'time',
                'hostname',
                'distribution',
            ])
        )
        self.assertEqual(doc['type'], 'dmedia/machine')
        self.assertEqual(doc['_id'], '_local/machine')
        self.assertEqual(doc['hostname'], socket.gethostname())
        self.assertEqual(doc['distribution'], platform.linux_distribution())


class test_MetaStore(CouchCase):
    klass = metastore.MetaStore

    def new(self):
        return self.klass(self.dbname)

    def test_init(self):
        inst = self.new()
        self.assertEqual(inst.dbname, self.dbname)
        self.assertEqual(isinstance(inst.desktop, CouchDatabase), True)
        self.assertEqual(isinstance(inst.server, couchdb.Server), True)

    def update(self):
        inst = self.new()
        '_local/app'
        inst.update(dict(_id=_id, foo='bar'))
        old = inst.db[_id]
        inst.update(dict(_id=_id, foo='bar'))
        self.assertEqual(inst.db[_id]['_rev'], old['_rev'])
        inst.update(dict(_id=_id, foo='baz'))
        self.assertNotEqual(inst.db[_id]['_rev'], old['_rev'])

    def test_create_machine(self):
        inst = self.new()
        self.assertFalse('_local/machine' in inst.db)
        _id = inst.create_machine()
        self.assertTrue('_local/machine' in inst.db)
        self.assertTrue(_id in inst.db)
        loc = inst.db['_local/machine']
        doc = inst.db[_id]
        self.assertEqual(set(loc), set(doc))
        self.assertEqual(loc['machine_id'], doc['machine_id'])
        self.assertEqual(loc['time'], doc['time'])

        self.assertEqual(inst._machine_id, None)
        self.assertEqual(inst.machine_id, _id)
        self.assertEqual(inst._machine_id, _id)

    def test_total_bytes(self):
        inst = self.new()
        self.assertEqual(inst.total_bytes(), 0)
        total = 0
        for exp in xrange(20, 31):
            size = 2 ** exp + 1
            total += size
            inst.db.create({'bytes': size, 'type': 'dmedia/file'})
            self.assertEqual(inst.total_bytes(), total)

    def test_extensions(self):
        inst = self.new()
        self.assertEqual(list(inst.extensions()), [])
        for i in xrange(17):
            inst.db.create({'ext': 'mov', 'type': 'dmedia/file'})
            inst.db.create({'ext': 'jpg', 'type': 'dmedia/file'})
            inst.db.create({'ext': 'cr2', 'type': 'dmedia/file'})
        self.assertEqual(
            list(inst.extensions()),
            [
                ('cr2', 17),
                ('jpg', 17),
                ('mov', 17),
            ]
        )
        for i in xrange(27):
            inst.db.create({'ext': 'mov', 'type': 'dmedia/file'})
            inst.db.create({'ext': 'jpg', 'type': 'dmedia/file'})
        self.assertEqual(
            list(inst.extensions()),
            [
                ('cr2', 17),
                ('jpg', 44),
                ('mov', 44),
            ]
        )
        for i in xrange(25):
            inst.db.create({'ext': 'mov', 'type': 'dmedia/file'})
        self.assertEqual(
            list(inst.extensions()),
            [
                ('cr2', 17),
                ('jpg', 44),
                ('mov', 69),
            ]
        )
