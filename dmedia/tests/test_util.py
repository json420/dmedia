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
Unit tests for `dmedia.util`.
"""

from os import path
from subprocess import check_call, CalledProcessError
from unittest import TestCase
import json

import filestore
import microfiber
from microfiber import random_id

from .base import TempDir
from .couch import CouchCase

import dmedia
from dmedia import schema
from dmedia import util


tree = path.dirname(path.dirname(path.abspath(dmedia.__file__)))
script = path.join(tree, 'init-filestore')


class TestFunctions(TestCase):
    def test_isfilestore(self):
        tmp = TempDir()
        self.assertFalse(util.isfilestore(tmp.dir))
        tmp.makedirs('.dmedia')
        self.assertTrue(util.isfilestore(tmp.dir))

    def test_getfilestore(self):
        tmp = TempDir()
        doc = schema.create_filestore(1)

        # Test when .dmedia/ doesn't exist
        with self.assertRaises(IOError) as cm:
            util.get_filestore(tmp.dir, doc['_id'])

        # Test when .dmedia/ exists, but store.json doesn't:
        tmp.makedirs('.dmedia')
        with self.assertRaises(IOError) as cm:
            util.get_filestore(tmp.dir, doc['_id'])

        # Test when .dmedia/store.json exists
        store = tmp.join('.dmedia', 'store.json')
        json.dump(doc, open(store, 'w'))

        (fs, doc2) = util.get_filestore(tmp.dir, doc['_id'])
        self.assertIsInstance(fs, filestore.FileStore)
        self.assertEqual(fs.parentdir, tmp.dir)
        self.assertEqual(fs.id, doc['_id'])
        self.assertEqual(fs.copies, 1)
        self.assertEqual(doc2, doc)

        # Test when you override copies
        (fs, doc2) = util.get_filestore(tmp.dir, doc['_id'], copies=2)
        self.assertIsInstance(fs, filestore.FileStore)
        self.assertEqual(fs.parentdir, tmp.dir)
        self.assertEqual(fs.id, doc['_id'])
        self.assertEqual(fs.copies, 2)
        self.assertEqual(doc2['copies'], 2)

        # Test when store_id doesn't match
        store_id = random_id()
        with self.assertRaises(Exception) as cm:
            util.get_filestore(tmp.dir, store_id)
        self.assertEqual(
            str(cm.exception),
            'expected store_id {!r}; got {!r}'.format(store_id, doc['_id'])
        )

    def test_init_filestore_script(self):
        if not path.isfile(script):
            self.skipTest('no file {!r}'.format(script))
        tmp = TempDir()

        # Try without arguments
        with self.assertRaises(CalledProcessError) as cm:
            check_call([script])
        self.assertFalse(util.isfilestore(tmp.dir))

        # Try it with correct arguments
        check_call([script, tmp.dir])
        self.assertTrue(util.isfilestore(tmp.dir))


 
doc_type = """
function(doc) {
    emit(doc.type, null);
}
"""


class TestDBFunctions(CouchCase):
    def test_update_design_doc(self):
        f = util.update_design_doc
        db = util.get_db(self.env)
        db.put(None)

        # Test when design doesn't exist:
        doc = {
            '_id': '_design/doc',
            'views': {
                'type': {'map': doc_type},
            },
        }
        self.assertEqual(f(db, doc), 'new')
        self.assertEqual(
            db.get('_design/doc')['_rev'],
            '1-98e283762b239249fc0cfc4159d84797'
        )
        self.assertNotIn('_rev', doc)

        # Test when design is same:
        self.assertEqual(f(db, doc), 'same')
        self.assertEqual(
            db.get('_design/doc')['_rev'],
            '1-98e283762b239249fc0cfc4159d84797'
        )
        self.assertNotIn('_rev', doc)

        # Test when design has changed:
        doc['views']['type'] = {'map': doc_type, 'reduce': '_count'}
        self.assertEqual(f(db, doc), 'changed')
        self.assertEqual(
            db.get('_design/doc')['_rev'],
            '2-a55f77029023bffaf68c19bb618d7b7a'
        )
        self.assertNotIn('_rev', doc)

        # Again test when design is same:
        self.assertEqual(f(db, doc), 'same')
        self.assertEqual(
            db.get('_design/doc')['_rev'],
            '2-a55f77029023bffaf68c19bb618d7b7a'
        )
        self.assertNotIn('_rev', doc)

    def test_init_views(self):
        db = util.get_db(self.env)
        db.put(None)

        doc1 = {
            '_id': '_design/doc',
            'views': {
                'type': {'map': doc_type},
            },
        }
        doc2 = {
            '_id': '_design/stuff',
            'views': {
                'junk': {'map': doc_type, 'reduce': '_count'},
            },
        }
        designs = (doc1, doc2)
 
        self.assertEqual(util.init_views(db, designs),
            [
                ('new', '_design/doc'),
                ('new', '_design/stuff'),   
            ]
        )
        self.assertEqual(
            db.get('_design/doc')['_rev'],
            '1-98e283762b239249fc0cfc4159d84797'
        )
        self.assertEqual(
            db.get('_design/stuff')['_rev'],
            '1-f2fc40529084795118edaa583a0cc89b'
        )

        self.assertEqual(util.init_views(db, designs),
            [
                ('same', '_design/doc'),
                ('same', '_design/stuff'),   
            ]
        )
        self.assertEqual(
            db.get('_design/doc')['_rev'],
            '1-98e283762b239249fc0cfc4159d84797'
        )
        self.assertEqual(
            db.get('_design/stuff')['_rev'],
            '1-f2fc40529084795118edaa583a0cc89b'
        )

    def test_get_db(self):
        db = util.get_db(self.env)
        self.assertIsInstance(db, microfiber.Database)
        self.assertEqual(db.name, schema.DB_NAME)
        self.assertTrue(db.ensure())
        self.assertEqual(db.get()['db_name'], schema.DB_NAME)

    def test_get_db2(self):
        # Test with init=True
        db = util.get_db(self.env, True)
        self.assertIsInstance(db, microfiber.Database)
        self.assertEqual(db.name, schema.DB_NAME)
        self.assertEqual(db.get()['db_name'], schema.DB_NAME)
        self.assertFalse(db.ensure())

    def test_get_project_db(self):
        _id = random_id()
        db_name = schema.project_db_name(_id)
        db = util.get_project_db(_id, self.env)
        self.assertIsInstance(db, microfiber.Database)
        self.assertEqual(db.name, db_name)
        self.assertTrue(db.ensure())
        self.assertEqual(db.get()['db_name'], db_name)

        # Test with init=True
        _id = random_id()
        db_name = schema.project_db_name(_id)
        db = util.get_project_db(_id, self.env, True)
        self.assertIsInstance(db, microfiber.Database)
        self.assertEqual(db.name, db_name)
        self.assertEqual(db.get()['db_name'], db_name)
        self.assertFalse(db.ensure())

