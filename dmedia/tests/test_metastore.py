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

from dmedia.abstractcouch import get_dmedia_db
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


class TestCouchFunctions(CouchCase):
    def test_update_design_doc(self):
        f = metastore.update_design_doc
        db = get_dmedia_db(self.env)

        # Test when design doesn't exist:
        doc = metastore.build_design_doc('user',
            [('video', metastore.user_video, None)]
        )
        self.assertEqual(f(db, doc), 'new')
        self.assertTrue(db['_design/user']['_rev'].startswith('1-'))

        # Test when design is same:
        doc = metastore.build_design_doc('user',
            [('video', metastore.user_video, None)]
        )
        self.assertEqual(f(db, doc), 'same')
        self.assertTrue(db['_design/user']['_rev'].startswith('1-'))

        # Test when design is changed:
        doc = metastore.build_design_doc('user',
            [('video', metastore.user_audio, None)]
        )
        self.assertEqual(f(db, doc), 'changed')
        self.assertTrue(db['_design/user']['_rev'].startswith('2-'))

        # Again test when design is same:
        doc = metastore.build_design_doc('user',
            [('video', metastore.user_audio, None)]
        )
        self.assertEqual(f(db, doc), 'same')
        self.assertTrue(db['_design/user']['_rev'].startswith('2-'))


class test_MetaStore(CouchCase):
    klass = metastore.MetaStore

    def new(self):
        return self.klass(self.env)

    def test_init(self):
        inst = self.new()
        self.assertEqual(inst.env, self.env)
        self.assertTrue(isinstance(inst.server, couchdb.Server))
        self.assertTrue(isinstance(inst.db, couchdb.Database))

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
